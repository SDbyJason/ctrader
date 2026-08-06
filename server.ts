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
const FALLBACK_ORIGIN = "https://evidencetrading.com";

/* Origins allowed to talk to this bridge. ALLOWED_ORIGIN (env) may hold a
   comma-separated list and is merged with these defaults, so adding a domain
   never means losing the ones already here — the single-value env var is what
   broke evidencetrading.com when the app moved off the github.io host.
   This list guards two different things:
     1. the CORS Access-Control-Allow-Origin header, and
     2. the OAuth return origin, which carries the cTrader access AND refresh
        token in the URL hash. That one matters: /oauth/start takes the origin
        from a query parameter, so without this check anyone could send a user
        through a crafted link and have the tokens redirected to their own
        host. Unknown origins fall back to FALLBACK_ORIGIN. */
const DEFAULT_ORIGINS = [
  "https://evidencetrading.com",
  "https://www.evidencetrading.com",
  "https://sdbyjason.github.io",
];
function allowedOrigins(): string[] {
  const fromEnv = getEnv("ALLOWED_ORIGIN")
    .split(",").map(s => s.trim().replace(/\/+$/, "")).filter(Boolean);
  return [...new Set([...fromEnv, ...DEFAULT_ORIGINS])];
}
/** Origin part of a URL ("https://host.tld"), or "" if it is not parseable. */
function originOf(u: string): string {
  try { return new URL(u).origin; } catch { return ""; }
}
/** True if `u` (a full URL or bare origin) sits on an allowed origin. */
function isAllowedOrigin(u: string): boolean {
  const norm = originOf(u);
  return !!norm && allowedOrigins().some(o => originOf(o) === norm);
}
/** The request's Origin if we allow it, otherwise "". */
function matchOrigin(req?: Request): string {
  const sent = req?.headers.get("Origin") || "";
  if (!sent) return "";
  const norm = originOf(sent);
  return allowedOrigins().some(o => originOf(o) === norm) ? sent : "";
}

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

function corsHeaders(req?: Request): Record<string, string> {
  // Echo the caller's Origin when it is on the allowlist. Echoing (rather than
  // sending a fixed value) is what lets several front-end hosts share one
  // bridge; "Vary: Origin" keeps caches from serving one host's header to
  // another. Callers without an Origin header get the primary domain.
  const origin = matchOrigin(req) || FALLBACK_ORIGIN;
  return {
    "Access-Control-Allow-Origin":  origin,
    "Vary":                         "Origin",
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
  // Reject unknown return origins here, at the start of the flow, so the user
  // sees an error instead of handing cTrader tokens to someone else's host.
  if (!isAllowedOrigin(origin))
    return new Response("Origin not allowed", { status: 400 });
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
  // Re-checked on the way back too: the state blob is only base64url, not
  // signed, so it must not be trusted to name the redirect target on its own.
  const claimed = (parsed && typeof parsed.origin === "string") ? parsed.origin : "";
  const origin  = isAllowedOrigin(claimed) ? claimed : FALLBACK_ORIGIN;
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

// ════════════════════════════════════════════════════════════════
//  BACKGROUND POLLER — sync while the app is closed
//  ──────────────────────────────────────────────────────────────
//  Everything above this line is request/response: the browser asks,
//  we proxy. That only works while a tab is open, so SL/TP and closed
//  trades were lost whenever the user was away — the "automatic
//  journal" was only automatic while you watched it.
//
//  This section keeps the refresh token server-side (encrypted) and
//  polls each linked account on a cron tick. Found trades are written
//  as EVENTS into apexUsers/<uid>/inbox — the exact same channel the
//  MT5/TradingView ingest worker uses. The web app already drains that
//  inbox and owns the merge, so nothing here needs to understand pip
//  math, and there is no read-modify-write race on the journal doc.
//
//  Endpoints added:
//    POST /api/link    { uid, refresh_token, accountId, accountEnv }
//    POST /api/unlink  { uid, accountId }
//    GET  /api/link/status?uid=&accountId=
//    POST /cron/poll   header: X-Cron-Secret  — driven by a Cloudflare
//                      cron trigger (Render free has no scheduler and
//                      sleeps; the tick both wakes and drives it)
//
//  Extra env vars:
//    FB_PROJECT_ID, FB_CLIENT_EMAIL, FB_PRIVATE_KEY   (service account)
//    LINK_ENC_KEY   base64 32 bytes — AES-GCM key for refresh tokens
//    CRON_SECRET    shared secret for /cron/poll
// ════════════════════════════════════════════════════════════════

const POLL_BUDGET_MS   = 45_000;   // stay under Render's request timeout
/* Floor between two polls of the SAME account — a safety net against a
   misconfigured cron, not the schedule itself. Keep it comfortably BELOW the
   cron interval, or the guard silently eats every other tick and the cadence
   you configured is not the cadence you get. (It was 4 min against a 2 min
   cron, which halved the effective rate and cost SL/TP on short trades.) */
const POLL_MIN_GAP_MS  = 100_000;
const MAX_ERRORS       = 8;        // then pause the link, stop burning budget
const LOOKBACK_MS      = 3 * 24 * 3600 * 1000;

// ─── Google service-account access token (cached) ───────────────

let _gTok: { value: string; exp: number } = { value: "", exp: 0 };

function b64url(bytes: Uint8Array): string {
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function importServiceKey(pem: string): Promise<CryptoKey> {
  // Robust against every paste variant: literal "\n", real newlines, quotes.
  const body = pem
    .replace(/\\n/g, "\n")
    .replace(/-----[^-]+-----/g, "")
    .replace(/[^A-Za-z0-9+/=]/g, "");
  const der = Uint8Array.from(atob(body), c => c.charCodeAt(0));
  return crypto.subtle.importKey("pkcs8", der.buffer,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["sign"]);
}

async function googleToken(): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  if (_gTok.value && now < _gTok.exp - 60) return _gTok.value;

  const enc = (o: unknown) => b64url(new TextEncoder().encode(JSON.stringify(o)));
  const unsigned = enc({ alg: "RS256", typ: "JWT" }) + "." + enc({
    iss:   getEnv("FB_CLIENT_EMAIL"),
    scope: "https://www.googleapis.com/auth/datastore",
    aud:   "https://oauth2.googleapis.com/token",
    iat:   now,
    exp:   now + 3600,
  });
  const key = await importServiceKey(getEnv("FB_PRIVATE_KEY"));
  const sig = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key,
    new TextEncoder().encode(unsigned));
  const jwt = `${unsigned}.${b64url(new Uint8Array(sig))}`;

  const r = await fetch("https://oauth2.googleapis.com/token", {
    method:  "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body:    `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${jwt}`,
  });
  const d = await r.json();
  if (!d.access_token) throw new Error("google oauth: " + JSON.stringify(d));
  _gTok = { value: d.access_token, exp: now + (d.expires_in || 3600) };
  return _gTok.value;
}

// ─── Firestore REST ─────────────────────────────────────────────

function fsBase(): string {
  return `https://firestore.googleapis.com/v1/projects/${getEnv("FB_PROJECT_ID")}/databases/(default)/documents`;
}
// deno-lint-ignore no-explicit-any
function fsVal(v: any): Record<string, unknown> {
  if (v === null || v === undefined)  return { nullValue: null };
  if (typeof v === "boolean")         return { booleanValue: v };
  if (typeof v === "number")
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  return { stringValue: String(v) };
}
function fsFields(obj: Record<string, unknown>) {
  const fields: Record<string, unknown> = {};
  for (const k in obj) fields[k] = fsVal(obj[k]);
  return { fields };
}
// deno-lint-ignore no-explicit-any
function fsRead(doc: any): Record<string, any> {
  const out: Record<string, unknown> = {};
  const f = (doc && doc.fields) || {};
  for (const k in f) {
    const v = f[k];
    out[k] = v.stringValue !== undefined  ? v.stringValue
           : v.integerValue !== undefined ? Number(v.integerValue)
           : v.doubleValue !== undefined  ? v.doubleValue
           : v.booleanValue !== undefined ? v.booleanValue
           : null;
  }
  return out;
}

async function fsGet(path: string): Promise<Record<string, unknown> | null> {
  const r = await fetch(`${fsBase()}/${path}`, {
    headers: { Authorization: `Bearer ${await googleToken()}` },
  });
  if (r.status === 404) return null;
  if (!r.ok) throw new Error(`fsGet ${r.status} ${await r.text()}`);
  return fsRead(await r.json());
}
async function fsSet(path: string, data: Record<string, unknown>): Promise<void> {
  // updateMask so a partial write never wipes fields it does not mention
  const mask = Object.keys(data).map(k => `updateMask.fieldPaths=${encodeURIComponent(k)}`).join("&");
  const r = await fetch(`${fsBase()}/${path}?${mask}`, {
    method:  "PATCH",
    headers: { Authorization: `Bearer ${await googleToken()}`, "Content-Type": "application/json" },
    body:    JSON.stringify(fsFields(data)),
  });
  if (!r.ok) throw new Error(`fsSet ${r.status} ${await r.text()}`);
}
async function fsDelete(path: string): Promise<void> {
  await fetch(`${fsBase()}/${path}`, {
    method: "DELETE", headers: { Authorization: `Bearer ${await googleToken()}` },
  });
}
async function fsCreate(collection: string, data: Record<string, unknown>): Promise<void> {
  const r = await fetch(`${fsBase()}/${collection}`, {
    method:  "POST",
    headers: { Authorization: `Bearer ${await googleToken()}`, "Content-Type": "application/json" },
    body:    JSON.stringify(fsFields(data)),
  });
  if (!r.ok) throw new Error(`fsCreate ${r.status} ${await r.text()}`);
}
// Links due for a poll, oldest first — a plain query so one slow account
// cannot starve the others.
async function fsDueLinks(limit: number): Promise<Array<Record<string, unknown>>> {
  const body = {
    structuredQuery: {
      from:    [{ collectionId: "brokerLinks" }],
      where:   { fieldFilter: { field: { fieldPath: "status" }, op: "EQUAL",
                                value: { stringValue: "active" } } },
      orderBy: [{ field: { fieldPath: "lastPollAt" }, direction: "ASCENDING" }],
      limit,
    },
  };
  const r = await fetch(`${fsBase()}:runQuery`, {
    method:  "POST",
    headers: { Authorization: `Bearer ${await googleToken()}`, "Content-Type": "application/json" },
    body:    JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`fsQuery ${r.status} ${await r.text()}`);
  const rows = await r.json();
  return (rows as Array<Record<string, unknown>>)
    // deno-lint-ignore no-explicit-any
    .filter((x: any) => x.document)
    // deno-lint-ignore no-explicit-any
    .map((x: any) => ({ ...fsRead(x.document), _name: x.document.name.split("/documents/")[1] }));
}

// ─── Refresh-token encryption (AES-GCM) ─────────────────────────
// The poller has to keep a long-lived broker credential. Storing it in
// plaintext would mean a Firestore read is enough to trade-read someone's
// account, so the key lives only in the worker's environment.

async function encKey(): Promise<CryptoKey> {
  const raw = Uint8Array.from(atob(getEnv("LINK_ENC_KEY")), c => c.charCodeAt(0));
  if (raw.length !== 32) throw new Error("LINK_ENC_KEY must be 32 bytes, base64-encoded");
  return crypto.subtle.importKey("raw", raw, "AES-GCM", false, ["encrypt", "decrypt"]);
}
async function encryptToken(plain: string): Promise<string> {
  const iv  = crypto.getRandomValues(new Uint8Array(12));
  const buf = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, await encKey(),
    new TextEncoder().encode(plain));
  const out = new Uint8Array(iv.length + buf.byteLength);
  out.set(iv, 0); out.set(new Uint8Array(buf), iv.length);
  return btoa(String.fromCharCode(...out));
}
async function decryptToken(blob: string): Promise<string> {
  const all = Uint8Array.from(atob(blob), c => c.charCodeAt(0));
  const buf = await crypto.subtle.decrypt(
    { name: "AES-GCM", iv: all.slice(0, 12) }, await encKey(), all.slice(12));
  return new TextDecoder().decode(buf);
}

// ─── Link management ────────────────────────────────────────────

function linkPath(uid: string, accountId: number): string {
  return `brokerLinks/${uid}__${accountId}`;
}

async function handleLink(req: Request): Promise<Response> {
  let body: Record<string, unknown>;
  try { body = await req.json(); } catch { return jsonResp({ error: "Invalid JSON" }, 400); }

  const uid       = String(body.uid || "");
  const refresh   = String(body.refresh_token || "");
  const accountId = Number(body.accountId || 0);
  const env       = String(body.accountEnv || "demo");
  if (!uid || !refresh || !accountId) return jsonResp({ error: "Missing params" }, 400);

  const existing = await fsGet(linkPath(uid, accountId));
  await fsSet(linkPath(uid, accountId), {
    uid, provider: "ctrader", accountId, accountEnv: env,
    refreshTokenEnc: await encryptToken(refresh),
    status: "active", errorCount: 0, lastError: "",
    // Keep the cursor on re-link so reconnecting does not replay old trades
    lastDealTs: Number(existing?.lastDealTs || (Date.now() - LOOKBACK_MS)),
    knownOpen:  String(existing?.knownOpen || "{}"),
    lastPollAt: 0,
    updatedAt:  Date.now(),
  });
  console.log(`[link] ${uid} account ${accountId} (${env}) linked for background sync`);
  return jsonResp({ ok: true });
}

async function handleUnlink(req: Request): Promise<Response> {
  let body: Record<string, unknown>;
  try { body = await req.json(); } catch { return jsonResp({ error: "Invalid JSON" }, 400); }
  const uid = String(body.uid || ""), accountId = Number(body.accountId || 0);
  if (!uid || !accountId) return jsonResp({ error: "Missing params" }, 400);
  await fsDelete(linkPath(uid, accountId));
  console.log(`[link] ${uid} account ${accountId} unlinked`);
  return jsonResp({ ok: true });
}

async function handleLinkStatus(url: URL): Promise<Response> {
  const uid = url.searchParams.get("uid") || "";
  const accountId = Number(url.searchParams.get("accountId") || 0);
  if (!uid || !accountId) return jsonResp({ error: "Missing params" }, 400);
  const doc = await fsGet(linkPath(uid, accountId));
  if (!doc) return jsonResp({ linked: false });
  return jsonResp({
    linked:     true,
    status:     doc.status,
    lastPollAt: doc.lastPollAt || 0,
    lastError:  doc.status === "active" ? "" : (doc.lastError || ""),
  });
}

// ─── Event emission ─────────────────────────────────────────────
// Same shape the ingest worker writes, so the app's existing inbox
// drainer merges cTrader background trades with no changes.

function emitEvent(uid: string, ev: Record<string, unknown>): Promise<void> {
  return fsCreate(`apexUsers/${uid}/inbox`, {
    source: "ctrader", receivedAt: Date.now(), ...ev,
  });
}

function toMs(ts: unknown): number | null {
  if (!ts) return null;
  const n = Number(ts);
  if (!Number.isFinite(n)) return null;
  return (n > 946684800 && n < 32503680000) ? n * 1000 : n;
}
function dirOfDeal(d: Record<string, unknown>): string {
  const raw = String(d.tradeSide ?? d.dealType ?? "").toUpperCase();
  const num = Number(d.tradeSide || 0);
  if (raw.includes("BUY")  || raw === "LONG"  || num === 1) return "long";
  if (raw.includes("SELL") || raw === "SHORT" || num === 2) return "short";
  return "";
}

// ─── Event diffing (pure — see ctraderapi.test.js) ──────────────
//
// The only stateful thing a poll has to decide is: which of these deals
// has the journal not heard about yet? `knownOpen` answers that. It maps
// positionId → "<sl>|<tp>" while a position is open, and → "closed" once
// its exit has been sent, so a redelivered deal (the cursor overlaps on
// purpose) produces nothing the second time.
//
// Positions that opened AND closed between two ticks are the normal case
// when the app was shut. For those the open event is synthesised from the
// opening deal, so the journal keeps the entry price instead of a bare
// exit row. Their SL/TP is genuinely unrecoverable — cTrader only exposes
// protection levels on positions that are currently open — which is why
// the cron interval, not this function, decides how much detail survives.

interface PollDiff {
  events: Array<Record<string, unknown>>;
  known:  Record<string, string>;
  maxTs:  number;
}

function buildEvents(
  deals: Array<Record<string, unknown>>,
  openPositions: Array<Record<string, unknown>>,
  knownRaw: unknown,
  from: number,
): PollDiff {
  const groups = new Map<string, Array<Record<string, unknown>>>();
  for (const d of deals) {
    const pid = String(d.positionId ?? d.dealId ?? "");
    if (!pid) continue;
    if (!groups.has(pid)) groups.set(pid, []);
    groups.get(pid)!.push(d);
  }

  let known: Record<string, string> = {};
  try { known = JSON.parse(String(knownRaw || "{}")); } catch { known = {}; }

  const openNow = new Map<string, Record<string, unknown>>();
  for (const p of openPositions) openNow.set(String(p.positionId), p);

  const events: Array<Record<string, unknown>> = [];
  let maxTs = from;

  for (const [pid, group] of groups) {
    group.sort((a, b) =>
      (toMs(a.executionTimestamp || a.createTimestamp) || 0) -
      (toMs(b.executionTimestamp || b.createTimestamp) || 0));
    const openDeal  = group[0];
    const closeDeal = group[group.length - 1];
    const cpd = (closeDeal.closePositionDetail || openDeal.closePositionDetail) as
      Record<string, unknown> | undefined;

    const openTs  = toMs(openDeal.executionTimestamp  || openDeal.createTimestamp);
    const closeTs = toMs(closeDeal.executionTimestamp || closeDeal.createTimestamp);
    if (closeTs && closeTs > maxTs) maxTs = closeTs;
    if (openTs  && openTs  > maxTs) maxTs = openTs;

    const live   = openNow.get(pid);
    const sl     = live ? (live.stopLoss   ?? null) : null;
    const tp     = live ? (live.takeProfit ?? null) : null;
    const symbol = String(closeDeal.symbolName || openDeal.symbolName || "");
    // cTrader volume is in units of 0.01 lots (10000 = 1.00 lot)
    const rawVol = Number(openDeal.volume || openDeal.filledVolume || 0);
    const volume = rawVol > 0 ? rawVol / 10000 : null;
    const entryPrice = Number(openDeal.executionPrice ?? openDeal.price ?? 0) || null;

    if (!cpd) {
      const sig = `${sl ?? ""}|${tp ?? ""}`;
      if (known[pid] === undefined) {
        events.push({ event: "open", symbol, dir: dirOfDeal(openDeal), price: entryPrice,
                      sl, tp, volume, time: openTs || Date.now(), posId: pid });
      } else if (known[pid] !== sig && known[pid] !== "closed") {
        events.push({ event: "modify", symbol, sl, tp, posId: pid, time: Date.now() });
      }
      if (known[pid] !== "closed") known[pid] = sig;
      continue;
    }

    if (known[pid] === "closed") continue;          // already reported

    if (known[pid] === undefined) {
      events.push({ event: "open", symbol, dir: dirOfDeal(openDeal), price: entryPrice,
                    sl, tp, volume, time: openTs || closeTs || Date.now(), posId: pid });
    }
    const gross = cpd.grossProfit !== undefined ? Number(cpd.grossProfit) : Number(cpd.netProfit || 0);
    events.push({
      event: "close", symbol, dir: dirOfDeal(openDeal),
      price: Number(closeDeal.executionPrice ?? closeDeal.price ?? 0) || null,
      volume, pnl: gross,
      commission: cpd.commission !== undefined ? Number(cpd.commission) : null,
      swap:       cpd.swap       !== undefined ? Number(cpd.swap)       : null,
      time: closeTs || Date.now(), posId: pid,
    });
    known[pid] = "closed";
  }

  // Closed positions only need to be remembered long enough to outlive the
  // cursor overlap that redelivers them; otherwise the map grows forever.
  const trimmed: Record<string, string> = {};
  for (const pid of Object.keys(known)) {
    if (known[pid] !== "closed" || openNow.has(pid) || groups.has(pid)) trimmed[pid] = known[pid];
  }

  return { events, known: trimmed, maxTs };
}

// ─── One account ────────────────────────────────────────────────

async function pollLink(link: Record<string, unknown>): Promise<number> {
  const uid       = String(link.uid);
  const accountId = Number(link.accountId);
  const env       = String(link.accountEnv || "demo");
  const path      = linkPath(uid, accountId);

  const tokenRef: TokenRef = {
    accessToken:  "",
    refreshToken: await decryptToken(String(link.refreshTokenEnc)),
  };

  // A fresh access token every cycle: it is cheap and avoids storing one.
  const refreshed = await httpRefreshToken(tokenRef.refreshToken);
  if (!refreshed) {
    // The user revoked access, or the refresh token expired.
    await fsSet(path, {
      status: "reauth_required", lastPollAt: Date.now(),
      lastError: "cTrader rejected the stored credentials — please reconnect.",
      updatedAt: Date.now(),
    });
    console.warn(`[poll] ${uid}/${accountId}: refresh rejected → reauth_required`);
    return 0;
  }
  tokenRef.accessToken  = refreshed.accessToken;
  tokenRef.refreshToken = refreshed.refreshToken;

  const from = Number(link.lastDealTs || (Date.now() - LOOKBACK_MS));
  const to   = Date.now();

  let deals: Array<Record<string, unknown>> = [];
  let openPositions: Array<Record<string, unknown>> = [];

  await withCtraderWS(async (send, nextMsg) => {
    send(PT_APP_AUTH_REQ, {
      clientId: getEnv("CTRADER_CLIENT_ID"), clientSecret: getEnv("CTRADER_CLIENT_SECRET"),
    }, "app_auth");
    await waitFor(nextMsg, PT_APP_AUTH_RES);

    send(PT_ACCOUNT_AUTH_REQ, {
      ctidTraderAccountId: accountId, accessToken: tokenRef.accessToken,
    }, "acc_auth");
    await waitFor(nextMsg, PT_ACCOUNT_AUTH_RES);

    const symbolMap = await fetchSymbolMap(send, nextMsg, accountId);
    // A little overlap on the cursor: a deal can be written just after the
    // previous poll read its window. Duplicates are free (the drainer keys
    // on positionId), a missed close is not.
    deals = normalizeDeals(
      await fetchDeals(send, nextMsg, accountId, Math.max(0, from - 60_000), to, tokenRef),
      symbolMap);
    try {
      openPositions = await fetchOpenPositions(send, nextMsg, accountId, symbolMap);
    } catch (e) {
      console.warn("[poll] reconcile failed (non-fatal):", e instanceof Error ? e.message : e);
    }
  }, env, tokenRef);

  const { events, known, maxTs } = buildEvents(deals, openPositions, link.knownOpen, from);
  for (const ev of events) await emitEvent(uid, ev);

  await fsSet(path, {
    refreshTokenEnc: await encryptToken(tokenRef.refreshToken),   // cTrader rotates it
    lastDealTs: maxTs,
    knownOpen:  JSON.stringify(known).slice(0, 900_000),
    lastPollAt: Date.now(),
    status: "active", errorCount: 0, lastError: "",
    updatedAt: Date.now(),
  });

  console.log(`[poll] ${uid}/${accountId}: ${deals.length} deals → ${events.length} events`);
  return events.length;
}

// On-demand poll for a single account — this is what "Sync Now" calls once
// background sync owns the credential. Rate-limited so it cannot be used to
// hammer cTrader; it returns counts only, never account data, so the worst a
// guessed uid achieves is an extra poll of that user's own account.
async function handlePollNow(req: Request): Promise<Response> {
  let body: Record<string, unknown>;
  try { body = await req.json(); } catch { return jsonResp({ error: "Invalid JSON" }, 400); }
  const uid = String(body.uid || ""), accountId = Number(body.accountId || 0);
  if (!uid || !accountId) return jsonResp({ error: "Missing params" }, 400);

  const path = linkPath(uid, accountId);
  const link = await fsGet(path);
  if (!link) return jsonResp({ error: "not_linked" }, 404);
  if (link.status === "reauth_required") return jsonResp({ error: "reauth_required" }, 409);

  if (Date.now() - Number(link.lastPollAt || 0) < 30_000)
    return jsonResp({ ok: true, throttled: true, events: 0 });

  try {
    const events = await pollLink({ ...link, _name: path });
    return jsonResp({ ok: true, events });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error(`[poll-now] ${uid}/${accountId}: ${msg}`);
    return jsonResp({ error: msg }, 502);
  }
}

// ─── Cron entry point ───────────────────────────────────────────

/* Last tick, remembered in memory. This lives here rather than in the
   Cloudflare worker on purpose: Workers run the cron handler in a different
   isolate than incoming requests, so a counter there is invisible to anyone
   checking the health URL. Render is one long-running process, so this
   survives — and /health becomes the honest answer to "is it running?".
   Resets on redeploy or when the free instance sleeps, which is fine: both
   mean the next tick re-establishes it within minutes. */
let _lastCron: Record<string, unknown> | null = null;

async function handleCronPoll(req: Request): Promise<Response> {
  const secret = req.headers.get("X-Cron-Secret") || "";
  if (!getEnv("CRON_SECRET") || secret !== getEnv("CRON_SECRET"))
    return jsonResp({ error: "forbidden" }, 403);

  const started = Date.now();
  let links: Array<Record<string, unknown>>;
  try { links = await fsDueLinks(40); }
  catch (e) { return jsonResp({ error: String(e) }, 500); }

  let polled = 0, events = 0, failed = 0, skipped = 0;

  for (const link of links) {
    if (Date.now() - started > POLL_BUDGET_MS) break;   // next tick continues
    if (Date.now() - Number(link.lastPollAt || 0) < POLL_MIN_GAP_MS) { skipped++; continue; }
    try {
      events += await pollLink(link);
      polled++;
    } catch (e) {
      failed++;
      const msg = e instanceof Error ? e.message : String(e);
      const n   = Number(link.errorCount || 0) + 1;
      console.error(`[poll] ${link.uid}/${link.accountId} failed (${n}): ${msg}`);
      try {
        await fsSet(String(link._name), {
          errorCount: n, lastError: msg.slice(0, 300), lastPollAt: Date.now(),
          status: n >= MAX_ERRORS ? "paused" : "active",
          updatedAt: Date.now(),
        });
      } catch { /* the next tick will retry */ }
    }
  }

  const result = { ok: true, polled, events, failed, skipped,
                   ms: Date.now() - started, due: links.length };
  _lastCron = { at: new Date(started).toISOString(), ...result };
  return jsonResp(result);
}

// ─── MAIN HANDLER ──────────────────────────────────────────────

async function handler(req: Request): Promise<Response> {
  const url = new URL(req.url);
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders(req) });
  try {
    if (url.pathname === "/" || url.pathname === "/health")
      return jsonResp({
        ok: true,
        // Is the background sync actually ticking? This is the place to look.
        cron: _lastCron
          ? { ...(_lastCron as Record<string, unknown>),
              secondsAgo: Math.round((Date.now() - Date.parse(String(_lastCron.at))) / 1000) }
          : "no tick received since this instance started",
        configured: {
          firestore:  !!(getEnv("FB_PROJECT_ID") && getEnv("FB_CLIENT_EMAIL") && getEnv("FB_PRIVATE_KEY")),
          encryption: getEnv("LINK_ENC_KEY").length > 0,
          cronSecret: getEnv("CRON_SECRET").length > 0,
        },
      });
    if (url.pathname === "/oauth/start")                           return handleOAuthStart(url);
    if (url.pathname === "/oauth/callback")                        return await handleOAuthCallback(url);
    if (url.pathname === "/api/refresh"  && req.method === "POST") return await handleRefresh(req);
    if (url.pathname === "/api/accounts" && req.method === "POST") return await handleAccounts(req);
    if (url.pathname === "/api/deals"    && req.method === "POST") return await handleDeals(req);
    // Background sync (see the POLLER section above)
    if (url.pathname === "/api/link"        && req.method === "POST") return await handleLink(req);
    if (url.pathname === "/api/unlink"      && req.method === "POST") return await handleUnlink(req);
    if (url.pathname === "/api/link/status" && req.method === "GET")  return await handleLinkStatus(url);
    if (url.pathname === "/api/poll-now"    && req.method === "POST") return await handlePollNow(req);
    if (url.pathname === "/cron/poll"       && req.method === "POST") return await handleCronPoll(req);
    return jsonResp({ error: "Not found" }, 404);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("[handler] error:", msg);
    return new Response(JSON.stringify({ error: msg }), {
      status: 500, headers: { "Content-Type": "application/json", ...corsHeaders(req) },
    });
  }
}

const PORT = parseInt(getEnv("PORT") || "8000");
Deno.serve({ port: PORT }, async (req) => {
  if (req.method === "OPTIONS")
    return new Response(null, { status: 204, headers: corsHeaders(req) });
  try {
    const res     = await handler(req);
    // The OAuth routes answer with a 302 whose Location is the whole point of
    // the request — leave redirects untouched, CORS headers are meaningless
    // there and rewriting the response would drop the Location on some paths.
    if (res.status >= 300 && res.status < 400) return res;
    const headers = new Headers(res.headers);
    for (const [k, v] of Object.entries(corsHeaders(req))) headers.set(k, v);
    return new Response(res.body, { status: res.status, headers });
  } catch (err) {
    console.error("[top-level]", err);
    return new Response(JSON.stringify({ error: String(err) }), {
      status: 500, headers: { "Content-Type": "application/json", ...corsHeaders(req) },
    });
  }
});
