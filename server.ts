/**
 * APEX <-> cTrader Open API Bridge (Deno)
 * Correct endpoints: demo.ctraderapi.com / live.ctraderapi.com
 *
 * ProtoOADealListReq (2173) REQUIRES refreshToken as a mandatory field.
 * When the server sends a token-rotation event (2147) mid-session,
 * the new refreshToken is captured via tokenRef and used for subsequent chunks.
 * payloadType 2164 (account disconnect) is treated as a fatal error.
 */

const OAUTH_BASE      = "https://openapi.ctrader.com";
const WS_DEMO         = "wss://demo.ctraderapi.com:5036";
const WS_LIVE         = "wss://live.ctraderapi.com:5036";
const FALLBACK_ORIGIN = "https://sdbyjason.github.io/Apex-Trading-Journal";

const PT_APP_AUTH_REQ     = 2100;
const PT_APP_AUTH_RES     = 2101;
const PT_ACCOUNT_AUTH_REQ = 2102;
const PT_ACCOUNT_AUTH_RES = 2103;
const PT_DEAL_LIST_REQ    = 2173;
const PT_DEAL_LIST_RES    = 2174;
const PT_SYMBOL_BY_ID_REQ = 2121;
const PT_SYMBOL_BY_ID_RES = 2122;
const PT_ERROR_RES        = 50;
const PT_ACCOUNT_DISC     = 2164; // account disconnected — fatal

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

type SendFn    = (type: number, payload: Record<string, unknown>, id: string) => void;
type NextMsgFn = () => Promise<Record<string, unknown>>;

async function withCtraderWS(
  fn: (send: SendFn, nextMsg: NextMsgFn) => Promise<void>,
  accountEnv = "demo",
  tokenRef: { accessToken?: string; refreshToken?: string } = {}
): Promise<void> {
  const wsUrl = accountEnv === "live" ? WS_LIVE : WS_DEMO;
  console.log(`[ws] connecting to ${wsUrl}`);
  return new Promise((resolve, reject) => {
    const ws      = new WebSocket(wsUrl);
    const queue:  Record<string, unknown>[] = [];
    const waiters: ((msg: Record<string, unknown>) => void)[] = [];
    let closed    = false;
    let closeErr: Error | null = null;

    const heartbeat = setInterval(() => {
      if (!closed) {
        try { ws.send(JSON.stringify({ clientMsgId: "hb", payloadType: 51, payload: {} })); }
        catch { /* ignore */ }
      }
    }, 10_000);

    ws.onopen = async () => {
      console.log("[ws] onopen fired, readyState:", ws.readyState);
      const send: SendFn = (payloadType, payload, clientMsgId) => {
        const msg = JSON.stringify({ clientMsgId, payloadType, payload });
        console.log("[ws] sending:", msg.substring(0, 300));
        ws.send(msg);
      };
      const nextMsg: NextMsgFn = () => {
        if (queue.length > 0) return Promise.resolve(queue.shift()!);
        if (closed) return Promise.reject(closeErr || new Error("WebSocket closed"));
        return new Promise((res, rej) => {
          const timer = setTimeout(() => {
            const i = waiters.indexOf(res);
            if (i !== -1) waiters.splice(i, 1);
            rej(new Error("Timeout waiting for cTrader response (20s)"));
          }, 20_000);
          waiters.push((msg) => { clearTimeout(timer); res(msg); });
        });
      };
      console.log("[ws] starting fn...");
      try   { await fn(send, nextMsg); clearInterval(heartbeat); ws.close(); resolve(); }
      catch (e) { clearInterval(heartbeat); ws.close(); reject(e); }
    };

    ws.onmessage = ({ data }) => {
      try {
        const msg = JSON.parse(data as string) as Record<string, unknown>;
        console.log("[ws] received:", JSON.stringify(msg).substring(0, 300));

        // 2147 = server-push token rotation — capture new tokens, skip dispatch
        if (msg.payloadType === 2147) {
          const p = (msg.payload || {}) as Record<string, unknown>;
          if (p.accessToken)  tokenRef.accessToken  = p.accessToken as string;
          if (p.refreshToken) tokenRef.refreshToken = p.refreshToken as string;
          console.log("[ws] token rotation received (2147), new tokens captured");
          return; // do NOT forward to waiters
        }

        // 2174 = deal list response — may also carry rotated tokens
        if (msg.payloadType === PT_DEAL_LIST_RES) {
          const p = (msg.payload || {}) as Record<string, unknown>;
          if (p.accessToken)  tokenRef.accessToken  = p.accessToken as string;
          if (p.refreshToken) tokenRef.refreshToken = p.refreshToken as string;
        }

        if (waiters.length > 0) waiters.shift()!(msg);
        else queue.push(msg);
      } catch { /* ignore */ }
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

/**
 * Wait for a specific payloadType, skipping unrelated push messages.
 * Throws on protocol errors (50, 2142) and account-disconnect (2164).
 */
async function waitFor(nextMsg: NextMsgFn, expectedType: number): Promise<Record<string, unknown>> {
  while (true) {
    const msg = await nextMsg();
    const pt  = msg.payloadType as number;

    if (pt === PT_ERROR_RES || pt === 2142) {
      const p = (msg.payload || {}) as Record<string, unknown>;
      throw new Error(`cTrader error [${p.errorCode}]: ${p.description || JSON.stringify(p)}`);
    }

    // Account disconnected — signals expired/invalid access token
    if (pt === PT_ACCOUNT_DISC) {
      throw new Error("cTrader: account disconnected (2164) — access token expired, re-authenticate");
    }

    if (pt === expectedType) return msg;

    // Unrelated push (e.g. server heartbeat, spot prices) — skip
    console.log(`[ws] skipping unexpected payloadType ${pt} while waiting for ${expectedType}`);
  }
}

/**
 * Fetch deals in 7-day chunks.
 *
 * ProtoOADealListReq (2173) requires refreshToken as a mandatory field.
 * tokenRef is a live reference — if a 2147 token-rotation arrives mid-session,
 * tokenRef.refreshToken is updated and subsequent chunks use the fresh token.
 */
async function fetchDeals(
  send: SendFn,
  nextMsg: NextMsgFn,
  accountId: number,
  from: number,
  to: number,
  tokenRef: { accessToken?: string; refreshToken?: string },
  initialRefreshToken: string
): Promise<Array<Record<string, unknown>>> {
  const MS_7 = 7 * 24 * 60 * 60 * 1000;
  const all: Array<Record<string, unknown>> = [];
  let chunkFrom = from, i = 0;

  while (chunkFrom < to) {
    const chunkTo = Math.min(chunkFrom + MS_7, to);

    // Always use the most current refreshToken (may have been rotated by a 2147 event)
    const currentRefreshToken = tokenRef.refreshToken || initialRefreshToken;

    send(PT_DEAL_LIST_REQ, {
      ctidTraderAccountId: accountId,
      fromTimestamp:       chunkFrom,
      toTimestamp:         chunkTo,
      refreshToken:        currentRefreshToken, // required by cTrader API
    }, `dl_${++i}`);

    const res   = await waitFor(nextMsg, PT_DEAL_LIST_RES);
    const deals = ((res.payload || {}) as Record<string, unknown>).deal as Array<Record<string, unknown>> || [];
    all.push(...deals);
    console.log(`[ws] chunk ${i}: ${deals.length} deals, ${new Date(chunkFrom).toISOString().slice(0,10)} – ${new Date(chunkTo).toISOString().slice(0,10)}`);
    chunkFrom = chunkTo + 1;
  }
  return all;
}

async function fetchSymbolMap(
  send: SendFn, nextMsg: NextMsgFn,
  accountId: number, symbolIds: number[]
): Promise<Map<number, string>> {
  const map = new Map<number, string>();
  if (symbolIds.length === 0) return map;
  send(PT_SYMBOL_BY_ID_REQ, { ctidTraderAccountId: accountId, symbolId: symbolIds }, "sym_by_id");
  const res     = await waitFor(nextMsg, PT_SYMBOL_BY_ID_RES);
  const symbols = (((res.payload || {}) as Record<string, unknown>).symbol || []) as Array<Record<string, unknown>>;
  for (const s of symbols) {
    const name = (s.symbolName || (s.tradeData as Record<string,unknown>)?.symbolName) as string | undefined;
    if (s.symbolId != null && name) map.set(Number(s.symbolId), name);
  }
  return map;
}

function normalizeDeals(
  deals: Array<Record<string, unknown>>,
  symbolMap: Map<number, string>
): Array<Record<string, unknown>> {
  return deals.map(d => {
    const symbolId   = Number(d.symbolId);
    const symbolName = symbolMap.get(symbolId) || `Symbol#${symbolId}`;
    const cpd        = d.closePositionDetail as Record<string, unknown> | undefined;
    if (cpd) {
      const scale = Math.pow(10, Number(cpd.moneyDigits || d.moneyDigits || 2));
      d.closePositionDetail = { ...cpd,
        grossProfit: Number(cpd.grossProfit || 0) / scale,
        netProfit:   Number(cpd.netProfit   || 0) / scale,
      };
    }
    return { ...d, symbolName };
  });
}

// ─── HANDLERS ──────────────────────────────────────────────────
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
  const tokenRes = await fetch(`${OAUTH_BASE}/apps/token`, {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type:    "authorization_code",
      code,
      redirect_uri:  getEnv("CTRADER_REDIRECT_URI"),
      client_id:     getEnv("CTRADER_CLIENT_ID"),
      client_secret: getEnv("CTRADER_CLIENT_SECRET"),
    }),
  });
  const tokenData = await tokenRes.json();
  if (!tokenRes.ok || tokenData.errorCode)
    return redirectWithHash(origin, { ok: false, error: tokenData.description || "Token exchange failed" });
  return redirectWithHash(origin, {
    ok:           true,
    uid:          parsed?.uid,
    env:          parsed?.env || "demo",
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
      grant_type:    "refresh_token",
      refresh_token,
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
    fetch(`https://api.spotware.com/connect/tradingaccounts?access_token=${encodeURIComponent(access_token)}`)
      .then(r => r.ok ? r.json() : null).catch(() => null),
    fetch(`https://demo.spotware.com/connect/tradingaccounts?access_token=${encodeURIComponent(access_token)}`)
      .then(r => r.ok ? r.json() : null).catch(() => null),
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

  const accountId          = Number(ctidTraderAccountId);
  const from               = Number(fromTimestamp || (Date.now() - 7 * 24 * 3600 * 1000));
  const to                 = Number(toTimestamp   || Date.now());
  const env                = (accountEnv as string) || "demo";
  const initialRefreshToken = String(refresh_token);

  if (isNaN(accountId) || accountId <= 0) return jsonResp({ error: "Invalid ctidTraderAccountId" }, 400);

  try {
    let result: Array<Record<string, unknown>> = [];
    // tokenRef is passed into both withCtraderWS (for 2147/2174 capture)
    // and fetchDeals (so each chunk uses the latest token after rotation)
    const tokenRef: { accessToken?: string; refreshToken?: string } = {};

    await withCtraderWS(async (send, nextMsg) => {
      // 1. App auth
      send(PT_APP_AUTH_REQ, {
        clientId:     getEnv("CTRADER_CLIENT_ID"),
        clientSecret: getEnv("CTRADER_CLIENT_SECRET"),
      }, "app");
      await waitFor(nextMsg, PT_APP_AUTH_RES);

      // 2. Account auth
      send(PT_ACCOUNT_AUTH_REQ, {
        ctidTraderAccountId: accountId,
        accessToken:         String(access_token),
      }, "acc");
      await waitFor(nextMsg, PT_ACCOUNT_AUTH_RES);

      // 3. Fetch deals chunk by chunk (refreshToken required by API, rotated tokens auto-used)
      const rawDeals  = await fetchDeals(send, nextMsg, accountId, from, to, tokenRef, initialRefreshToken);
      const uniqueIds = [...new Set(rawDeals.map(d => Number(d.symbolId)).filter(Boolean))];
      const symbolMap = await fetchSymbolMap(send, nextMsg, accountId, uniqueIds);
      result = normalizeDeals(rawDeals, symbolMap);
      console.log(`[deals] done: ${result.length} deals`);
    }, env, tokenRef);

    return jsonResp({
      data:            result,
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
    if (url.pathname === "/" || url.pathname === "/health")            return jsonResp({ ok: true });
    if (url.pathname === "/oauth/start")                               return handleOAuthStart(url);
    if (url.pathname === "/oauth/callback")                            return await handleOAuthCallback(url);
    if (url.pathname === "/api/refresh"  && req.method === "POST")     return await handleRefresh(req);
    if (url.pathname === "/api/accounts" && req.method === "POST")     return await handleAccounts(req);
    if (url.pathname === "/api/deals"    && req.method === "POST")     return await handleDeals(req);
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
    const res = await handler(req);
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
