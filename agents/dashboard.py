#!/usr/bin/env python3
# agents/dashboard.py
"""Control dashboard for the ingestion and execution agents.

Serves a small web UI on http://127.0.0.1:8080 (localhost only, since it can
place orders) that can:
  * pick a ticker, start/stop its ingestion market-data stream and watch live ticks
  * preview (/whatif) and submit limit orders through the execution agent

The execution safety rails still apply: while DRY_RUN is on (the default) the
Submit button is disabled and the API refuses to place orders — only previews
work. Start with DRY_RUN=0 to enable submission; non-paper accounts are still
refused unless ALLOW_LIVE=1.

Run:  ./venv/bin/python agents/dashboard.py   (or DASHBOARD_PORT=<port> ...)
"""
import asyncio
import os
from collections import deque

import aiohttp
from aiohttp import web

try:
    from agents import execution, ingestion
    from agents.ib_gateway import GATEWAY_BASE_URL, TICKER, ssl_context
except ImportError:  # when run directly as ./agents/dashboard.py
    import execution
    import ingestion
    from ib_gateway import GATEWAY_BASE_URL, TICKER, ssl_context

DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "8080"))


# ---------------------------------------------------------------- ingestion

async def _ingest(app):
    """Consumes the ingestion agent's tick stream into the shared buffer and
    records it to data/ticks/ for the strategy agent."""
    stream = ingestion.mock_market_stream(app["ticker"])
    store = ingestion.TickStore()
    try:
        async for tick in stream:
            store.add(tick)
            app["tick_count"] += 1
            app["ticks"].append({
                "timestamp": tick["timestamp"].isoformat(),
                "ticker": tick["ticker"],
                "price": tick["price"],
                "volume": tick["volume"],
            })
    finally:
        store.flush()
        await stream.aclose()


async def api_ingestion_start(request):
    app = request.app
    ticker = None
    try:
        body = await request.json()
        ticker = str(body.get("ticker") or "").strip().upper() or None
    except (ValueError, AttributeError):
        pass  # no or malformed body: keep the current ticker
    if ticker is not None and (
            len(ticker) > 12 or not all(c.isalnum() or c in ". -" for c in ticker)):
        return web.json_response({"error": f"invalid ticker {ticker!r}"}, status=400)

    task = app["ingestion_task"]
    running = task is not None and not task.done()
    if ticker is not None and ticker != app["ticker"]:
        app["ticker"] = ticker
        if running:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            running = False
        app["ticks"].clear()  # don't mix prices of different stocks in the chart
    if not running:
        app["ingestion_task"] = asyncio.create_task(_ingest(app))
    return web.json_response({"ok": True, "ticker": app["ticker"]})


async def api_ingestion_stop(request):
    app = request.app
    task = app["ingestion_task"]
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    app["ingestion_task"] = None
    return web.json_response({"ok": True})


async def api_status(request):
    app = request.app
    task = app["ingestion_task"]
    error = None
    if task is not None and task.done() and not task.cancelled():
        exc = task.exception()
        if exc is not None:
            error = f"{type(exc).__name__}: {exc}"
    return web.json_response({
        "ingestion_running": task is not None and not task.done(),
        "ingestion_error": error,
        "ticker": app["ticker"],
        "tick_count": app["tick_count"],
        "ticks": list(app["ticks"])[-90:],
        "gateway_url": GATEWAY_BASE_URL,
        "dry_run": execution.DRY_RUN,
        "allow_live": execution.ALLOW_LIVE,
        "default_ticker": TICKER,
    })


# ---------------------------------------------------------------- execution

async def api_order(request):
    try:
        body = await request.json()
        side = str(body.get("side", "BUY"))
        ticker = str(body.get("ticker") or TICKER).upper()
        quantity = int(body.get("quantity", 1))
        limit_price = float(body["limit_price"])
        submit = bool(body.get("submit", False))
    except (ValueError, KeyError, TypeError):
        return web.json_response(
            {"error": "quantity and limit price must be numbers"}, status=400)

    if submit and execution.DRY_RUN:
        return web.json_response(
            {"error": "DRY_RUN is on — restart the dashboard with DRY_RUN=0 "
                      "to submit orders"}, status=403)

    try:
        # validate side/quantity/price locally before any gateway round-trip
        execution.build_order(0, side, quantity, price=limit_price)
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=400)

    session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=ssl_context()))
    try:
        account_id = await execution.get_account_id(session)
        conid = await execution.search_conid(session, ticker)
        order = execution.build_order(conid, side, quantity, price=limit_price)
        preview = await execution.preview_order(session, account_id, order)
        result = {
            "account_id": account_id,
            "side": order["side"],
            "ticker": ticker,
            "quantity": quantity,
            "limit_price": limit_price,
            "preview": preview,
            "submitted": False,
        }
        if submit:
            placed = await execution.place_order(session, account_id, order)
            final = await execution.wait_for_status(
                session, placed["order_id"], timeout=20)
            result["submitted"] = True
            result["order_id"] = placed["order_id"]
            result["final_status"] = (final or {}).get(
                "status", placed.get("order_status", "unknown"))
        return web.json_response(result)
    except (ValueError, RuntimeError) as e:
        return web.json_response({"error": str(e)}, status=400)
    except (aiohttp.ClientError, OSError) as e:
        return web.json_response(
            {"error": f"gateway unreachable at {GATEWAY_BASE_URL}: {e}"},
            status=502)
    finally:
        await session.close()


# ------------------------------------------------------------------- web ui

async def index(request):
    return web.Response(text=PAGE, content_type="text/html")


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>QI Trader — Control</title>
<style>
  :root {
    --page: #f9f9f7; --surface-1: #fcfcfb;
    --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #898781;
    --grid: #e1e0d9; --baseline: #c3c2b7; --border: rgba(11,11,11,0.10);
    --series-1: #2a78d6; --good: #0ca30c; --warning: #fab219; --critical: #d03b3b;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --page: #0d0d0d; --surface-1: #1a1a19;
      --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #898781;
      --grid: #2c2c2a; --baseline: #383835; --border: rgba(255,255,255,0.10);
      --series-1: #3987e5;
    }
  }
  * { box-sizing: border-box; margin: 0; }
  body { background: var(--page); color: var(--text-primary); padding: 24px;
         font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
  .wrap { max-width: 920px; margin: 0 auto; display: grid; gap: 16px; }
  header { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
  h1 { font-size: 18px; margin-right: 8px; }
  h2 { font-size: 15px; }
  .sub { color: var(--text-secondary); font-size: 13px; margin-top: 2px; }
  .chip { font-size: 12px; color: var(--text-secondary); background: var(--surface-1);
          border: 1px solid var(--border); border-radius: 999px; padding: 3px 10px;
          display: inline-flex; align-items: center; gap: 6px; }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--text-muted); }
  .dot.on { background: var(--good); }
  .card { background: var(--surface-1); border: 1px solid var(--border);
          border-radius: 10px; padding: 20px; }
  .card-head { display: flex; justify-content: space-between; align-items: start;
               gap: 12px; margin-bottom: 16px; }
  .controls { display: flex; gap: 10px; align-items: end; }
  .controls input { width: 90px; text-transform: uppercase; }
  button { font: inherit; border-radius: 8px; padding: 7px 16px; cursor: pointer;
           border: 1px solid var(--border); background: var(--surface-1);
           color: var(--text-primary); }
  button.primary { background: var(--series-1); border-color: transparent; color: #fff; }
  button:disabled { opacity: 0.45; cursor: not-allowed; }
  .tiles { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px;
           margin-bottom: 16px; }
  .tile { border: 1px solid var(--border); border-radius: 8px; padding: 10px 14px; }
  .tile .label { font-size: 12px; color: var(--text-secondary); }
  .tile .value { font-size: 22px; font-weight: 600; margin-top: 2px; }
  .chart-title { font-size: 12px; color: var(--text-secondary); margin-bottom: 4px; }
  #chart-wrap { position: relative; }
  #chart { display: block; width: 100%; }
  .empty { position: absolute; inset: 0; display: flex; align-items: center;
           justify-content: center; color: var(--text-muted); font-size: 13px; }
  .tooltip { position: absolute; pointer-events: none; background: var(--surface-1);
             border: 1px solid var(--border); border-radius: 6px; padding: 6px 10px;
             font-size: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.12); }
  .tooltip .val { font-weight: 600; font-size: 14px; color: var(--text-primary);
                  display: flex; align-items: center; gap: 6px; }
  .tooltip .key { width: 10px; height: 0; border-top: 2px solid var(--series-1); }
  .tooltip .meta { color: var(--text-secondary); margin-top: 1px; }
  details { margin-top: 12px; }
  summary { font-size: 12px; color: var(--text-secondary); cursor: pointer; }
  table { border-collapse: collapse; margin-top: 8px; font-size: 12px; width: 100%; }
  th { text-align: left; color: var(--text-secondary); font-weight: 500; }
  th, td { padding: 3px 12px 3px 0; border-bottom: 1px solid var(--grid); }
  td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
  form { display: flex; gap: 10px; flex-wrap: wrap; align-items: end; }
  .field { display: grid; gap: 3px; }
  .field span { font-size: 12px; color: var(--text-secondary); }
  input, select { font: inherit; padding: 6px 8px; border: 1px solid var(--border);
                  border-radius: 6px; background: var(--page); color: var(--text-primary);
                  width: 110px; }
  #order-result { margin-top: 14px; font-size: 13px; display: none; }
  #order-result.error { color: var(--critical); }
  #order-result dl { display: grid; grid-template-columns: max-content 1fr;
                     gap: 2px 14px; }
  #order-result dt { color: var(--text-secondary); }
  #order-result dd { font-variant-numeric: tabular-nums; }
  .note { font-size: 12px; color: var(--text-muted); margin-top: 10px; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>QI Trader — Control</h1>
    <span class="chip"><span class="dot" id="ingest-dot"></span><span id="ingest-chip">Stopped</span></span>
    <span class="chip" id="dryrun-chip">…</span>
    <span class="chip" id="gateway-chip">gateway: …</span>
  </header>

  <section class="card">
    <div class="card-head">
      <div>
        <h2>Ingestion</h2>
        <p class="sub">Live ticks from the IBKR gateway (simulated stream if it is unreachable).</p>
      </div>
      <div class="controls">
        <label class="field"><span>Ticker</span><input id="i-ticker" maxlength="12"></label>
        <button id="ingest-toggle" class="primary">Start</button>
      </div>
    </div>
    <div class="tiles">
      <div class="tile"><div class="label">Last price</div><div class="value" id="t-price">—</div></div>
      <div class="tile"><div class="label">Ticks received</div><div class="value" id="t-count">0</div></div>
      <div class="tile"><div class="label">Last volume</div><div class="value" id="t-volume">—</div></div>
    </div>
    <div class="chart-title" id="chart-title">Price — last 90 ticks</div>
    <div id="chart-wrap">
      <svg id="chart" height="200" role="img" aria-label="Price line chart of recent ticks"></svg>
      <div class="empty" id="chart-empty">Stopped — press Start to stream ticks.</div>
      <div class="tooltip" id="tooltip" hidden></div>
    </div>
    <details>
      <summary>Recent ticks (table)</summary>
      <table>
        <thead><tr><th>Time</th><th>Ticker</th><th class="num">Price</th><th class="num">Volume</th></tr></thead>
        <tbody id="tick-rows"></tbody>
      </table>
    </details>
  </section>

  <section class="card">
    <div class="card-head">
      <div>
        <h2>Execution</h2>
        <p class="sub">Preview asks the gateway what the order would cost (/whatif); Submit places it.</p>
      </div>
    </div>
    <form id="order-form">
      <label class="field"><span>Ticker</span><input id="f-ticker" value="TSLA"></label>
      <label class="field"><span>Side</span>
        <select id="f-side"><option>BUY</option><option>SELL</option></select></label>
      <label class="field"><span>Quantity</span><input id="f-qty" type="number" value="1" min="1"></label>
      <label class="field"><span>Limit price</span><input id="f-price" type="number" step="0.01" min="0.01"></label>
      <button type="button" id="preview-btn" class="primary">Preview</button>
      <button type="button" id="submit-btn">Submit order</button>
    </form>
    <div id="order-result"></div>
    <p class="note" id="order-note"></p>
  </section>
</div>

<script>
"use strict";
const $ = (id) => document.getElementById(id);
let status = null;
let layout = null;   // geometry of the last chart render, for the hover layer
let hoverX = null;
let orderPending = false;
let tickerInit = false;  // fill the ticker input from the server only once

const fmtPrice = (p) => p.toLocaleString(undefined,
  { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtTime = (iso) => iso.slice(11, 19);

async function poll() {
  try {
    status = await (await fetch("/api/status")).json();
    render();
  } catch (e) { /* server briefly unreachable; keep polling */ }
  setTimeout(poll, 1000);
}

function render() {
  const s = status;
  const running = s.ingestion_running;
  $("ingest-dot").className = "dot" + (running ? " on" : "");
  $("ingest-chip").textContent = running ? "Streaming" : "Stopped";
  $("gateway-chip").textContent = "gateway: " + s.gateway_url;
  $("dryrun-chip").textContent = s.dry_run
    ? "DRY_RUN on — preview only" : "\\u26a0 DRY_RUN off — orders are live";
  $("ingest-toggle").textContent = running ? "Stop" : "Start";
  $("ingest-toggle").className = running ? "" : "primary";
  if (!tickerInit && s.ticker) { $("i-ticker").value = s.ticker; tickerInit = true; }

  const ticks = s.ticks;
  const last = ticks[ticks.length - 1];
  $("t-price").textContent = last ? fmtPrice(last.price) : "—";
  $("t-count").textContent = s.tick_count.toLocaleString();
  $("t-volume").textContent = last ? last.volume.toLocaleString() : "—";
  $("chart-title").textContent =
    (last ? last.ticker : s.ticker) + " price — last 90 ticks";
  $("chart-empty").hidden = ticks.length > 0;
  $("chart-empty").textContent = s.ingestion_error
    ? "Ingestion stopped: " + s.ingestion_error
    : running ? "Waiting for " + s.ticker + " ticks…"
    : "Stopped — press Start to stream ticks.";

  $("submit-btn").disabled = s.dry_run || orderPending;
  $("order-note").textContent = s.dry_run
    ? "DRY_RUN is on: Submit is disabled; restart with DRY_RUN=0 to trade."
    : "Orders will be submitted to the gateway.";

  drawChart(ticks);
  drawTable(ticks);
}

function svgEl(name, attrs, styles) {
  const el = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const k in attrs) el.setAttribute(k, attrs[k]);
  for (const k in styles || {}) el.style[k] = styles[k];
  return el;
}

function niceTicks(lo, hi, n) {
  const raw = (hi - lo) / n, mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1, 2, 5, 10].map(m => m * mag).find(s => s >= raw);
  const out = [];
  for (let v = Math.ceil(lo / step) * step; v <= hi + 1e-9; v += step) out.push(v);
  const dec = Math.max(0, -Math.floor(Math.log10(step)));
  return { values: out, decimals: dec };
}

function drawChart(ticks) {
  const svg = $("chart"), wrap = $("chart-wrap");
  const W = wrap.clientWidth, H = 200;
  svg.setAttribute("width", W);
  svg.setAttribute("height", H);
  svg.replaceChildren();
  layout = null;
  if (!ticks.length) { $("tooltip").hidden = true; return; }

  const padL = 48, padR = 62, padT = 12, padB = 22;
  const prices = ticks.map(t => t.price);
  let lo = Math.min(...prices), hi = Math.max(...prices);
  if (hi - lo < 1e-9) { lo -= 0.5; hi += 0.5; }
  const pad = (hi - lo) * 0.08; lo -= pad; hi += pad;
  const x = (i) => padL + (W - padL - padR) *
    (ticks.length === 1 ? 0.5 : i / (ticks.length - 1));
  const y = (p) => padT + (H - padT - padB) * (1 - (p - lo) / (hi - lo));

  // recessive hairline gridlines + axis tick labels (text tokens, never series color)
  const t = niceTicks(lo, hi, 3);
  for (const v of t.values) {
    svg.append(svgEl("line", { x1: padL, x2: W - padR, y1: y(v), y2: y(v) },
      { stroke: "var(--grid)", strokeWidth: 1 }));
    const lbl = svgEl("text", { x: padL - 6, y: y(v) + 4, "text-anchor": "end",
      "font-size": 11 }, { fill: "var(--text-muted)",
      fontVariantNumeric: "tabular-nums" });
    lbl.textContent = v.toFixed(t.decimals);
    svg.append(lbl);
  }
  svg.append(svgEl("line", { x1: padL, x2: W - padR, y1: H - padB, y2: H - padB },
    { stroke: "var(--baseline)", strokeWidth: 1 }));

  // area wash (~10%) + 2px line, round joins
  const pts = ticks.map((tk, i) => x(i) + "," + y(tk.price));
  svg.append(svgEl("path", { d: "M" + pts.join("L") + "L" + x(ticks.length - 1) +
    "," + (H - padB) + "L" + x(0) + "," + (H - padB) + "Z" },
    { fill: "var(--series-1)", opacity: 0.1 }));
  svg.append(svgEl("path", { d: "M" + pts.join("L") },
    { fill: "none", stroke: "var(--series-1)", strokeWidth: 2,
      strokeLinejoin: "round", strokeLinecap: "round" }));

  // end marker (r=4, 2px surface ring) + endpoint direct label
  const li = ticks.length - 1;
  svg.append(svgEl("circle", { cx: x(li), cy: y(ticks[li].price), r: 4 },
    { fill: "var(--series-1)", stroke: "var(--surface-1)", strokeWidth: 2 }));
  svg.append(svgEl("text", { x: x(li) + 8, y: y(ticks[li].price) + 4,
    "font-size": 12 }, { fill: "var(--text-secondary)" }));
  svg.lastChild.textContent = fmtPrice(ticks[li].price);

  // first/last time labels
  for (const [i, anchor] of [[0, "start"], [li, "end"]]) {
    if (i === li && li === 0) break;
    svg.append(svgEl("text", { x: x(i), y: H - 6, "text-anchor": anchor,
      "font-size": 11 }, { fill: "var(--text-muted)",
      fontVariantNumeric: "tabular-nums" }));
    svg.lastChild.textContent = fmtTime(ticks[i].timestamp);
  }

  layout = { ticks, x, y, W, H, padT, padB };
  renderHover();
}

function renderHover() {
  const tip = $("tooltip");
  const old = document.getElementById("hover-layer");
  if (old) old.remove();
  if (!layout || hoverX === null) { tip.hidden = true; return; }
  const { ticks, x, y, H, padT, padB } = layout;
  let best = 0;
  for (let i = 1; i < ticks.length; i++)
    if (Math.abs(x(i) - hoverX) < Math.abs(x(best) - hoverX)) best = i;
  const tk = ticks[best], px = x(best), py = y(tk.price);

  const g = svgEl("g", { id: "hover-layer" });
  g.append(svgEl("line", { x1: px, x2: px, y1: padT, y2: H - padB },
    { stroke: "var(--baseline)", strokeWidth: 1 }));
  g.append(svgEl("circle", { cx: px, cy: py, r: 4 },
    { fill: "var(--series-1)", stroke: "var(--surface-1)", strokeWidth: 2 }));
  $("chart").append(g);

  tip.replaceChildren();
  const val = document.createElement("div"); val.className = "val";
  const key = document.createElement("span"); key.className = "key";
  val.append(key, document.createTextNode(fmtPrice(tk.price)));
  const meta = document.createElement("div"); meta.className = "meta";
  meta.textContent = fmtTime(tk.timestamp) + " · vol " + tk.volume.toLocaleString();
  tip.append(val, meta);
  tip.hidden = false;
  const wrap = $("chart-wrap");
  tip.style.left = Math.min(px + 12, wrap.clientWidth - tip.offsetWidth - 4) + "px";
  tip.style.top = Math.max(py - tip.offsetHeight - 10, 0) + "px";
}

function drawTable(ticks) {
  const rows = ticks.slice(-10).reverse().map(tk => {
    const tr = document.createElement("tr");
    for (const [txt, num] of [[fmtTime(tk.timestamp)], [tk.ticker],
        [fmtPrice(tk.price), 1], [tk.volume.toLocaleString(), 1]]) {
      const td = document.createElement("td");
      if (num) td.className = "num";
      td.textContent = txt;
      tr.append(td);
    }
    return tr;
  });
  $("tick-rows").replaceChildren(...rows);
}

$("chart").addEventListener("pointermove", (e) => {
  hoverX = e.offsetX; renderHover();
});
$("chart").addEventListener("pointerleave", () => {
  hoverX = null; renderHover();
});

async function startIngestion() {
  const resp = await fetch("/api/ingestion/start", { method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ticker: $("i-ticker").value.trim().toUpperCase() }) });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({}));
    alert(data.error || ("HTTP " + resp.status));
  }
  status = await (await fetch("/api/status")).json();
  render();
}

$("ingest-toggle").addEventListener("click", async () => {
  if (status && status.ingestion_running) {
    await fetch("/api/ingestion/stop", { method: "POST" });
    status = await (await fetch("/api/status")).json();
    render();
  } else {
    await startIngestion();
  }
});

// Committing a new ticker (Enter/blur) while streaming switches the stream to it
$("i-ticker").addEventListener("change", async () => {
  if (status && status.ingestion_running &&
      $("i-ticker").value.trim().toUpperCase() !== status.ticker) {
    await startIngestion();
  }
});

async function sendOrder(submit) {
  const body = {
    ticker: $("f-ticker").value.trim(),
    side: $("f-side").value,
    quantity: Number($("f-qty").value),
    limit_price: Number($("f-price").value),
    submit,
  };
  if (!body.limit_price) { showOrderError("Enter a limit price first."); return; }
  if (submit && !confirm("Place " + body.side + " " + body.quantity + " " +
      body.ticker + " LMT " + body.limit_price + "?")) return;
  orderPending = true;
  $("preview-btn").disabled = true;
  $("submit-btn").disabled = true;
  try {
    const resp = await fetch("/api/order", { method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body) });
    const data = await resp.json();
    if (!resp.ok) { showOrderError(data.error || ("HTTP " + resp.status)); return; }
    showOrderResult(data);
  } catch (e) {
    showOrderError("dashboard unreachable: " + e.message);
  } finally {
    orderPending = false;
    $("preview-btn").disabled = false;
    $("submit-btn").disabled = !status || status.dry_run;
  }
}

function showOrderError(msg) {
  const box = $("order-result");
  box.className = "error";
  box.style.display = "block";
  box.textContent = "\\u26a0 Error: " + msg;
}

function showOrderResult(d) {
  const box = $("order-result");
  box.className = "";
  box.style.display = "block";
  box.replaceChildren();
  const dl = document.createElement("dl");
  const row = (k, v) => {
    const dt = document.createElement("dt"); dt.textContent = k;
    const dd = document.createElement("dd"); dd.textContent = v;
    dl.append(dt, dd);
  };
  row("Order", d.side + " " + d.quantity + " " + d.ticker + " LMT " +
    fmtPrice(d.limit_price) + " on " + d.account_id);
  const amount = d.preview && d.preview.amount;
  if (amount) {
    row("Estimated total", amount.total || "—");
    row("Commission", amount.commission || "—");
  } else {
    row("Preview", JSON.stringify(d.preview));
  }
  row("Status", d.submitted
    ? "submitted — order " + d.order_id + ": " + d.final_status
    : "preview only — not submitted");
  box.append(dl);
}

$("preview-btn").addEventListener("click", () => sendOrder(false));
$("submit-btn").addEventListener("click", () => sendOrder(true));
poll();
</script>
</body>
</html>
"""


def create_app():
    app = web.Application()
    app["ticks"] = deque(maxlen=600)
    app["tick_count"] = 0
    app["ingestion_task"] = None
    app["ticker"] = TICKER
    app.router.add_get("/", index)
    app.router.add_get("/api/status", api_status)
    app.router.add_post("/api/ingestion/start", api_ingestion_start)
    app.router.add_post("/api/ingestion/stop", api_ingestion_stop)
    app.router.add_post("/api/order", api_order)

    async def _cleanup(app):
        task = app["ingestion_task"]
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    app.on_cleanup.append(_cleanup)
    return app


if __name__ == "__main__":
    print(f"QI Trader dashboard on http://127.0.0.1:{DASHBOARD_PORT} "
          f"(DRY_RUN={'on' if execution.DRY_RUN else 'off'})")
    # localhost only: this UI can place orders, so never bind a public interface
    web.run_app(create_app(), host="127.0.0.1", port=DASHBOARD_PORT,
                print=None)
