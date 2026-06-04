/**
 * APEX <-> cTrader Open API Bridge (Deno)
 * Built strictly per https://help.ctrader.com/open-api/
 *
 * Auth flow (per docs):
 *   1. ProtoOAApplicationAuthReq  (2100) → ProtoOAApplicationAuthRes  (2101)
 *   2. ProtoOAAccountAuthReq      (2102) → ProtoOAAccountAuthRes      (2103)
 *   3. ProtoOADealListReq         (2133) → ProtoOADealListRes         (2134)
 *      Fields: ctidTraderAccountId, fromTimestamp, toTimestamp, refreshToken (required despite not in docs)
 *
 * Symbol resolution (FIX):
 *   - ProtoOASymbolsListReq  (2114) → ProtoOASymbolsListRes  (2115)
 *     Returns ProtoOALightSymbol[] which reliably carry `symbolName` for ALL symbols.
 *     (ProtoOASymbolByIdReq / 2116 returned ProtoOASymbol objects WITHOUT a name on
 *      this broker, which is why symbols showed up as "Symbol#41".)
 *
 * SL / TP capture (FIX):
 *   - cTrader Deal objects (history) do NOT carry stopLoss/takeProfit.
 *     Those values live on the open Position.
 *   - ProtoOAReconcileReq (2124) → ProtoOAReconcileRes (2125) returns currently-open
 *     positions, each with `stopLoss` and `takeProfit` price levels.
 *   - We return these open positions to the client on every sync. The client caches
 *     SL/TP keyed by positionId while the trade is open, then attaches them when the
 *     matching close deal arrives later. This is the only way to obtain SL/TP, since
 *     closed positions no longer expose them through the API.
 *
 * Token refresh (per docs):
 *   - Access token expires after 2,628,000 seconds (~30 days)
 *   - Refresh via HTTP POST to /apps/token with grant_type=refresh_token
 *   - OR via ProtoOARefreshTokenReq (2173) → ProtoOARefreshTokenRes (2174)
 */

const OAUTH_BASE      = "https://openapi.ctrader.com";
const WS_DEMO         = "wss://demo.ctraderapi.com:5036";
const WS_LIVE         = "wss://live.ctraderapi.com:5036";
const FALLBACK_ORIGIN = "https://sdbyjason.github.io/Apex-Trading-Journal";

// Payload types per ProtoOAPayloadType enum
const PT_APP_AUTH_REQ      = 2100;
const PT_APP_AUTH_RES      = 2101;
const PT_ACCOUNT_AUTH_REQ  = 2102;
const PT_ACCOUNT_AUTH_RES  = 2103;
const PT_REFRESH_TOKEN_REQ = 2173;
const PT_REFRESH_TOKEN_RES = 2174;
const PT_DEAL_LIST_REQ     = 2133;
const PT_DEAL_LIST_RES     = 2134;
const PT_SYMBOLS_LIST_REQ  = 2114; // ProtoOASymbolsListReq  → light symbols w/ names
const PT_SYMBOLS_LIST_RES  = 2115; // ProtoOASymbolsListRes
const PT_RECONCILE_REQ     = 2124; // ProtoOAReconcileReq    → open positions w/ SL/TP
const PT_RECONCILE_RES     = 2125; // ProtoOAReconcileRes
const PT_ERROR_RES         = 50;   // ProtoErrorRes
const PT_OA_ERROR_RES      = 2142; // ProtoOAErrorRes

function getEnv(k: string): string { return Deno.env.get(k) || ""; }

function corsHeaders(): Record<string, string> {
  const origin = getEnv("ALLOWED_ORIGIN") || "*";
  return {
    "Access-Control-Allow-Origin":  origin,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Max-Age":       "86400",
  };
}
function jsonResp(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status, headers: { "Content-Type": "application/json", ...corsHeaders() },
  });
}

function b64urlEncode(obj: unknown): string {
  const bytes = new TextEncoder().encode(JSON.stringify(obj));
  let bin = ""; for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/=+$/, "").replace(/\+/g, "-").replace(/\//g, "_");
}
function b64urlDecode(s: string): Record<string, unknown> {
  const bin = atob(s.replace(/-/g, "+").replace(/_/g, "/"));
  const bytes = new Uint8Array([...bin].map(c => c.charCodeAt(0)));
  return JSON.parse(new TextDecoder().decode(bytes));
}

// ─── WEBSOCKET WRAPPER ─────────────────────────────────────────

type SendFn    = (type: number, payload: Record<string, unknown>, id: string) => void;
type NextMsgFn = () => Promise<Record<string, unknown>>;
type TokenRef  = { accessToken: string; refreshToken: string };

async function withCtraderWS(
  fn: (send: SendFn, nextMsg: NextMsgFn) => Promise<void>,
  accountEnv: string,
  tokenRef: TokenRef
): Promise<void> {
  const wsUrl = accountEnv === "live" ? WS_LIVE : WS_DEMO;
  console.log(`[ws] connecting to ${wsUrl}`);
  return new Promise((resolve, reject) => {
    const ws      = new WebSocket(wsUrl);
    const queue:  Record<string, unknown>[] = [];
    const waiters: ((msg: Record<string, unknown>) => void)[] = [];
    let closed   = false;
    let closeErr: Error | null = null;

    const heartbeat = setInterval(() => {
      if (!closed) {
        try { ws.send(JSON.stringify({ clientMsgId: "hb", payloadType: 51, payload: {} })); }
        catch { /* ignore */ }
      }
    }, 10_000);

    ws.onopen = async () => {
      console.log("[ws] connected");
      const send: SendFn = (payloadType, payload, clientMsgId) => {
        const msg = JSON.stringify({ clientMsgId, payloadType, payload });
        console.log("[ws] →", msg.substring(0, 300));
        ws.send(msg);
      };
      const nextMsg: NextMsgFn = () => {
        if (queue.length > 0) return Promise.resolve(queue.shift()!);
        if (closed) return Promise.reject(closeErr || new Error("WebSocket closed"));
        return new Promise((res, rej) => {
          const timer = setTimeout(() => {
            const i = waiters.indexOf(res);
            if (i !== -1) waiters.splice(i, 1);
            rej(new Error("Timeout waiting for cTrader response (25s)"));
          }, 25_000);
          waiters.push((msg) => { clearTimeout(timer); res(msg); });
        });
      };
      try {
        await fn(send, nextMsg);
        clearInterval(heartbeat); ws.close(); resolve();
      } catch (e) {
        clearInterval(heartbeat); ws.close(); reject(e);
      }
    };

    ws.onmessage = ({ data }) => {
      try {
        const msg = JSON.parse(data as string) as Record<string, unknown>;
        console.log("[ws] ←", JSON.stringify(msg).substring(0, 300));

        // 2147 = ProtoOAAccountsTokenInvalidatedEvent — server push, not a response
        if (msg.payloadType === 2147) {
          console.log("[ws] token-invalidated event received (2147), ignoring");
          return;
        }

        // 2164 = ProtoOATraderUpdatedEvent — server push, ignore
        if (msg.payloadType === 2164) {
          console.log("[ws] trader-updated event received (2164), ignoring");
          return;
        }

        if (waiters.length > 0) waiters.shift()!(msg);
        else queue.push(msg);
      } catch { /* ignore parse errors */ }
    };

    ws.onerror = () => {
      clearInterval(heartbeat);
      closed = true; closeErr = new Error("WebSocket error");
      waiters.forEach(w => w({})); waiters.length = 0;
      reject(closeErr);
    };

    ws.onclose = (e) => {
      console.log(`[ws] closed: code=${e.code} reason=${e.reason}`);
      clearInterval(heartbeat);
      closed = true; closeErr = new Error(`WebSocket closed (code ${e.code})`);
      waiters.forEach(w => w({})); waiters.length = 0;
    };
  });
}

// Waits for a specific payloadType, skips unrelated messages, throws on errors
async function waitFor(
  nextMsg: NextMsgFn,
  expectedType: number
): Promise<Record<string, unknown>> {
  while (true) {
    const msg = await nextMsg();
    const pt  = msg.payloadType as number;
    if (pt === PT_ERROR_RES || pt === PT_OA_ERROR_RES) {
      const p = (msg.payload || {}) as Record<string, unknown>;
      throw new Error(`cTrader error [${p.errorCode}]: ${p.description || JSON.stringify(p)}`);
    }
    if (pt === expectedType) return msg;
    console.log(`[ws] skipping payloadType ${pt} while waiting for ${expectedType}`);
  }
}

// ─── TOKEN REFRESH ─────────────────────────────────────────────

// Refresh via WS using ProtoOARefreshTokenReq (per docs — no HTTP needed)
async function refreshTokenViaWS(
  send: SendFn,
  nextMsg: NextMsgFn,
  refreshToken: string
): Promise<{ accessToken: string; refreshToken: string }> {
  send(PT_REFRESH_TOKEN_REQ, { refreshToken }, "refresh");
  const res     = await waitFor(nextMsg, PT_REFRESH_TOKEN_RES);
  const payload = (res.payload || {}) as Record<string, unknown>;
  return {
    accessToken:  payload.accessToken  as string,
    refreshToken: payload.refreshToken as string,
  };
}

// ─── DEAL FETCHING ─────────────────────────────────────────────

async function fetchDeals(
  send: SendFn, nextMsg: NextMsgFn,
  accountId: number, from: number, to: number,
  tokenRef: TokenRef
): Promise<Array<Record<string, unknown>>> {
  // cTrader requires refreshToken in every DealListReq despite not being in the public docs.
  // The server rotates tokens on each response — always use the latest one.
  const MS_7 = 7 * 24 * 60 * 60 * 1000;
  const all: Array<Record<string, unknown>> = [];
  let chunkFrom = from;
  let chunkNum  = 0;

  while (chunkFrom < to) {
    chunkNum++;
    const chunkTo = Math.min(chunkFrom + MS_7, to);

    send(PT_DEAL_LIST_REQ, {
      ctidTraderAccountId: accountId,
      fromTimestamp:       chunkFrom,
      toTimestamp:         chunkTo,
      refreshToken:        tokenRef.refreshToken,
    }, `dl_${chunkNum}`);

    const res     = await waitFor(nextMsg, PT_DEAL_LIST_RES);
    const payload = (res.payload || {}) as Record<string, unknown>;

    // cTrader rotates tokens on every DealListRes — capture the new ones
    if (payload.accessToken)  tokenRef.accessToken  = payload.accessToken  as string;
    if (payload.refreshToken) tokenRef.refreshToken = payload.refreshToken as string;

    const deals = (payload.deal as Array<Record<string, unknown>>) || [];
    all.push(...deals);
    console.log(`[ws] chunk ${chunkNum} [${new Date(chunkFrom).toISOString().slice(0,10)} → ${new Date(chunkTo).toISOString().slice(0,10)}]: ${deals.length} deals`);

    chunkFrom = chunkTo + 1;

    // Rate limit: cTrader allows ~1 req/sec for historical data requests
    if (chunkFrom < to) await new Promise(r => setTimeout(r, 1000));
  }

  return all;
}

// ─── SYMBOL MAP (FIX) ──────────────────────────────────────────
// Use ProtoOASymbolsListReq (2114) which returns ProtoOALightSymbol[] — these reliably
// carry `symbolName` for EVERY symbol on the account (XAUUSD, EURUSD, …).
// We build a complete id→name map once and reuse it for both deals and open positions.

async function fetchSymbolMap(
  send: SendFn, nextMsg: NextMsgFn,
  accountId: number
): Promise<Map<number, string>> {
  const map = new Map<number, string>();
  send(PT_SYMBOLS_LIST_REQ, {
    ctidTraderAccountId:    accountId,
    includeArchivedSymbols: false,
  }, "symbols_list");

  const res     = await waitFor(nextMsg, PT_SYMBOLS_LIST_RES);
  const payload = (res.payload || {}) as Record<string, unknown>;
  // ProtoOASymbolsListRes.symbol = ProtoOALightSymbol[]
  const symbols = (payload.symbol || payload.symbols || []) as Array<Record<string, unknown>>;

  for (const s of symbols) {
    const id   = Number(s.symbolId);
    const name = (s.symbolName || s.name || s.symbolFullName) as string | undefined;
    if (id && name) map.set(id, String(name).trim());
  }

  console.log(`[sym] loaded ${map.size} symbol names via SymbolsListReq`);
  if (map.size === 0) {
    console.warn(`[sym] SymbolsListRes returned 0 names — payload keys: ${Object.keys(payload).join(",")}`);
  }
  return map;
}

// ─── OPEN POSITIONS (FIX for SL/TP) ────────────────────────────
// ProtoOAReconcileReq (2124) → ProtoOAReconcileRes (2125) returns every currently-open
// position. ProtoOAPosition carries `stopLoss` and `takeProfit` price levels plus the
// tradeData (symbolId, tradeSide). We surface these so the client can cache SL/TP per
// positionId while the trade is open and merge them in once it closes.

async function fetchOpenPositions(
  send: SendFn, nextMsg: NextMsgFn,
  accountId: number,
  symbolMap: Map<number, string>
): Promise<Array<Record<string, unknown>>> {
  send(PT_RECONCILE_REQ, {
    ctidTraderAccountId:   accountId,
    returnProtectionOrders: true,
  }, "reconcile");

  const res     = await waitFor(nextMsg, PT_RECONCILE_RES);
  const payload = (res.payload || {}) as Record<string, unknown>;
  const positions = (payload.position || payload.positions || []) as Array<Record<string, unknown>>;

  const out = positions.map(p => {
    const td       = (p.tradeData || {}) as Record<string, unknown>;
    const symbolId = Number(td.symbolId ?? p.symbolId);
    const sl       = p.stopLoss   ?? p.stopLossPrice   ?? null;
    const tp       = p.takeProfit ?? p.takeProfitPrice ?? null;
    return {
      positionId: String(p.positionId ?? ""),
      symbolId,
      symbolName: symbolMap.get(symbolId) || (symbolId ? `Symbol#${symbolId}` : ""),
      tradeSide:  td.tradeSide ?? p.tradeSide ?? null,
      stopLoss:   sl != null ? Number(sl) : null,
      takeProfit: tp != null ? Number(tp) : null,
      entryPrice: p.price != null ? Number(p.price) : null,
    };
  }).filter(p => p.positionId);

  console.log(`[reconcile] ${out.length} open positions (with SL/TP where set)`);
  return out;
}

// ─── DEAL NORMALIZER ───────────────────────────────────────────

function normalizeDeals(
  deals: Array<Record<string, unknown>>,
  symbolMap: Map<number, string>
): Array<Record<string, unknown>> {
  return deals.map(d => {
    const symbolId   = Number(d.symbolId);
    const symbolName = symbolMap.get(symbolId) || `Symbol#${symbolId}`;
    const cpdRaw     = (d.closePositionDetail || d.closePositionDetails) as Record<string, unknown> | undefined;
    if (cpdRaw) {
      const scale = Math.pow(10, Number(cpdRaw.moneyDigits || d.moneyDigits || 2));
      const cpd   = {
        ...cpdRaw,
        grossProfit:  Number(cpdRaw.grossProfit  || 0) / scale,
        netProfit:    Number(cpdRaw.netProfit    || 0) / scale,
        closedVolume: cpdRaw.closedVolume,
      };
      return { ...d, closePositionDetail: cpd, closePositionDetails: undefined, symbolName };
    }
    return { ...d, symbolName };
  });
}

// ─── HTTP TOKEN REFRESH ────────────────────────────────────────

async function httpRefreshToken(refreshToken: string): Promise<{ accessToken: string; refreshToken: string } | null> {
  try {
    const res  = await fetch(`${OAUTH_BASE}/apps/token`, {
      method:  "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body:    new URLSearchParams({
        grant_type:    "refresh_token",
        refresh_token: refreshToken,
        client_id:     getEnv("CTRADER_CLIENT_ID"),
        client_secret: getEnv("CTRADER_CLIENT_SECRET"),
      }),
    });
    const data = await res.json();
    if (!res.ok || data.errorCode) {
      console.warn("[token] HTTP refresh failed:", data);
      return null;
    }
    console.log("[token] HTTP refresh succeeded");
    return {
      accessToken:  data.accessToken  || data.access_token,
      refreshToken: data.refreshToken || data.refresh_token,
    };
  } catch (e) {
    console.warn("[token] HTTP refresh threw:", e);
    return null;
  }
}

// ─── ROUTE HANDLERS ────────────────────────────────────────────

function handleOAuthStart(url: URL): Response {
  const uid    = url.searchParams.get("uid");
  const env    = (url.searchParams.get("env") || "demo").toLowerCase();
  const origin = url.searchParams.get("origin") || "";
  if (!uid)    return new Response("Missing uid",    { status: 400 });
  if (!origin) return new Response("Missing origin", { status: 400 });
  const state  = b64urlEncode({ uid, env, origin, t: Date.now() });
  const params = new URLSearchParams({
    client_id:     getEnv("CTRADER_CLIENT_ID"),
    redirect_uri:  getEnv("CTRADER_REDIRECT_URI"),
    scope:         "trading",
    response_type: "code",
    state,
  });
  return Response.redirect(`${OAUTH_BASE}/apps/auth?${params.toString()}`, 302);
}

async function handleOAuthCallback(url: URL): Promise<Response> {
  const code     = url.searchParams.get("code");
  const state    = url.searchParams.get("state");
  const errParam = url.searchParams.get("error");
  let parsed: Record<string, unknown> | null = null;
  if (state) { try { parsed = b64urlDecode(state); } catch { /* ignore */ } }
  const origin = (parsed && typeof parsed.origin === "string") ? parsed.origin : FALLBACK_ORIGIN;
  if (errParam || !code) return redirectWithHash(origin, { ok: false, error: errParam || "Missing code" });
  const tokenRes  = await fetch(`${OAUTH_BASE}/apps/token`, {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type:    "authorization_code", code,
      redirect_uri:  getEnv("CTRADER_REDIRECT_URI"),
      client_id:     getEnv("CTRADER_CLIENT_ID"),
      client_secret: getEnv("CTRADER_CLIENT_SECRET"),
    }),
  });
  const tokenData = await tokenRes.json();
  if (!tokenRes.ok || tokenData.errorCode)
    return redirectWithHash(origin, { ok: false, error: tokenData.description || "Token exchange failed" });
  return redirectWithHash(origin, {
    ok: true, uid: parsed?.uid, env: parsed?.env || "demo",
    accessToken:  tokenData.accessToken  || tokenData.access_token,
    refreshToken: tokenData.refreshToken || tokenData.refresh_token,
    expiresIn:    tokenData.expiresIn    || tokenData.expires_in,
  });
}

function redirectWithHash(origin: string, payload: Record<string, unknown>): Response {
  const hash = b64urlEncode({ type: "ctrader-auth", ...payload });
  return new Response(null, {
    status: 302,
    headers: { Location: (origin as string).replace(/\/+$/, "") + "/#ctrader_auth=" + hash },
  });
}

async function handleRefresh(req: Request): Promise<Response> {
  const { refresh_token } = await req.json();
  if (!refresh_token) return jsonResp({ error: "Missing refresh_token" }, 400);
  const res = await fetch(`${OAUTH_BASE}/apps/token`, {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "refresh_token", refresh_token,
      client_id:     getEnv("CTRADER_CLIENT_ID"),
      client_secret: getEnv("CTRADER_CLIENT_SECRET"),
    }),
  });
  return jsonResp(await res.json(), res.ok ? 200 : 400);
}

async function handleAccounts(req: Request): Promise<Response> {
  const { access_token } = await req.json();
  if (!access_token) return jsonResp({ error: "Missing access_token" }, 400);
  const [live, demo] = await Promise.all([
    fetch(`https://api.spotware.com/connect/tradingaccounts?access_token=${encodeURIComponent(access_token)}`).then(r => r.ok ? r.json() : null).catch(() => null),
    fetch(`https://demo.spotware.com/connect/tradingaccounts?access_token=${encodeURIComponent(access_token)}`).then(r => r.ok ? r.json() : null).catch(() => null),
  ]);
  const accounts = []; const seen = new Set();
  for (const result of [live, demo]) {
    if (!result?.data) continue;
    for (const a of result.data) {
      if (seen.has(a.accountId)) continue;
      seen.add(a.accountId);
      accounts.push({ ...a, _env: a.live === false ? "demo" : "live" });
    }
  }
  return jsonResp({ accounts });
}

async function handleDeals(req: Request): Promise<Response> {
  let body: Record<string, unknown>;
  try { body = await req.json(); } catch { return jsonResp({ error: "Invalid JSON" }, 400); }

  const { access_token, refresh_token, ctidTraderAccountId, fromTimestamp, toTimestamp, accountEnv } = body;
  if (!access_token || !ctidTraderAccountId) return jsonResp({ error: "Missing params" }, 400);
  if (!refresh_token) return jsonResp({ error: "Missing refresh_token" }, 400);

  const accountId = Number(ctidTraderAccountId);
  const from      = Number(fromTimestamp || (Date.now() - 7 * 24 * 3600 * 1000));
  const to        = Number(toTimestamp   || Date.now());
  const env       = (accountEnv as string) || "demo";

  if (isNaN(accountId) || accountId <= 0) return jsonResp({ error: "Invalid ctidTraderAccountId" }, 400);

  console.log(`[deals] accountId:${accountId} env:${env} from:${new Date(from).toISOString()} to:${new Date(to).toISOString()}`);

  // Sanity check: reject clearly wrong timestamps (future or before year 2000)
  const now = Date.now();
  if (from > now || to > now + 60_000) {
    console.error(`[deals] timestamp sanity fail: from=${new Date(from).toISOString()} to=${new Date(to).toISOString()}`);
    return jsonResp({ error: `Invalid timestamps: from=${new Date(from).toISOString()} to=${new Date(to).toISOString()} — values are in the future` }, 400);
  }

  const tokenRef: TokenRef = {
    accessToken:  String(access_token),
    refreshToken: String(refresh_token),
  };

  try {
    let result: Array<Record<string, unknown>> = [];
    let openPositions: Array<Record<string, unknown>> = [];

    await withCtraderWS(async (send, nextMsg) => {
      // Step 1: Authenticate application (ProtoOAApplicationAuthReq)
      send(PT_APP_AUTH_REQ, {
        clientId:     getEnv("CTRADER_CLIENT_ID"),
        clientSecret: getEnv("CTRADER_CLIENT_SECRET"),
      }, "app_auth");
      await waitFor(nextMsg, PT_APP_AUTH_RES);
      console.log("[ws] app authenticated");

      // Step 2: Authenticate account (ProtoOAAccountAuthReq)
      send(PT_ACCOUNT_AUTH_REQ, {
        ctidTraderAccountId: accountId,
        accessToken:         tokenRef.accessToken,
      }, "acc_auth");

      let accAuthRes: Record<string, unknown>;
      try {
        accAuthRes = await waitFor(nextMsg, PT_ACCOUNT_AUTH_RES);
        console.log("[ws] account authenticated:", accAuthRes);
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        // If account auth fails with invalid token, try refreshing first
        if (msg.includes("ACCESS_TOKEN") || msg.includes("INVALID")) {
          console.warn("[ws] account auth failed, refreshing token via WS...");
          const refreshed = await refreshTokenViaWS(send, nextMsg, tokenRef.refreshToken);
          tokenRef.accessToken  = refreshed.accessToken;
          tokenRef.refreshToken = refreshed.refreshToken;
          // Retry account auth with fresh token
          send(PT_ACCOUNT_AUTH_REQ, {
            ctidTraderAccountId: accountId,
            accessToken:         tokenRef.accessToken,
          }, "acc_auth_retry");
          await waitFor(nextMsg, PT_ACCOUNT_AUTH_RES);
          console.log("[ws] account authenticated after token refresh");
        } else {
          throw e;
        }
      }

      // Step 3: Load the full symbol name map FIRST (id → name) so deals AND open
      // positions can both be labelled correctly (fixes "Symbol#41" → "XAUUSD").
      const symbolMap = await fetchSymbolMap(send, nextMsg, accountId);

      // Step 4: Fetch deals in 7-day chunks (ProtoOADealListReq)
      const rawDeals = await fetchDeals(send, nextMsg, accountId, from, to, tokenRef);
      console.log(`[ws] total raw deals: ${rawDeals.length}`);

      // Step 5: Fetch currently-open positions (with SL/TP) so the client can cache
      // them and attach to the trade when it later closes.
      try {
        openPositions = await fetchOpenPositions(send, nextMsg, accountId, symbolMap);
      } catch (e) {
        console.warn("[reconcile] failed (non-fatal):", e instanceof Error ? e.message : e);
        openPositions = [];
      }

      // Step 6: Normalize monetary values + attach symbol names
      result = normalizeDeals(rawDeals, symbolMap);
      console.log(`[deals] done: ${result.length} normalized deals, ${openPositions.length} open positions`);
    }, env, tokenRef);

    return jsonResp({
      data:            result,
      openPositions,                       // ← NEW: open positions carrying SL/TP
      newAccessToken:  tokenRef.accessToken  || null,
      newRefreshToken: tokenRef.refreshToken || null,
    });

  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("[deals] error:", msg);
    return jsonResp({ error: msg }, 502);
  }
}

// ─── MAIN HANDLER ──────────────────────────────────────────────

async function handler(req: Request): Promise<Response> {
  const url = new URL(req.url);
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders() });
  try {
    if (url.pathname === "/" || url.pathname === "/health")        return jsonResp({ ok: true });
    if (url.pathname === "/oauth/start")                           return handleOAuthStart(url);
    if (url.pathname === "/oauth/callback")                        return await handleOAuthCallback(url);
    if (url.pathname === "/api/refresh"  && req.method === "POST") return await handleRefresh(req);
    if (url.pathname === "/api/accounts" && req.method === "POST") return await handleAccounts(req);
    if (url.pathname === "/api/deals"    && req.method === "POST") return await handleDeals(req);
    return jsonResp({ error: "Not found" }, 404);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("[handler] error:", msg);
    return new Response(JSON.stringify({ error: msg }), {
      status: 500, headers: { "Content-Type": "application/json", ...corsHeaders() },
    });
  }
}

const PORT = parseInt(getEnv("PORT") || "8000");
Deno.serve({ port: PORT }, async (req) => {
  if (req.method === "OPTIONS")
    return new Response(null, { status: 204, headers: corsHeaders() });
  try {
    const res     = await handler(req);
    const headers = new Headers(res.headers);
    for (const [k, v] of Object.entries(corsHeaders())) headers.set(k, v);
    return new Response(res.body, { status: res.status, headers });
  } catch (err) {
    console.error("[top-level]", err);
    return new Response(JSON.stringify({ error: String(err) }), {
      status: 500, headers: { "Content-Type": "application/json", ...corsHeaders() },
    });
  }
});
