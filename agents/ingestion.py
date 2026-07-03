#!/usr/bin/env python3
# agents/ingestion.py
import polars as pl
import asyncio
import io
import json
import time
import aiohttp
import random
from datetime import datetime, timedelta
from pathlib import Path

try:
    from agents.ib_gateway import GATEWAY_BASE_URL, GATEWAY_WS_URL, TICKER, ssl_context
except ImportError:  # when run directly as ./agents/ingestion.py
    from ib_gateway import GATEWAY_BASE_URL, GATEWAY_WS_URL, TICKER, ssl_context

# Client Portal streaming tick fields (see gateway/doc/RealtimeSubscription.md):
FIELD_LAST_PRICE = "31"
FIELD_LAST_SIZE = "7059"

# Strict tick schema shared by every agent that writes or reads tick data
SCHEMA = {
    "timestamp": pl.Datetime,
    "ticker": pl.String,
    "price": pl.Float64,
    "volume": pl.Int64
}

# Recorded ticks live in data/ticks/<ticker>-<date>.ndjson at the repo root
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "ticks"
# strftime always emits the fractional part so every stored line parses the same
_TIME_WRITE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
_TIME_READ_FORMAT = "%Y-%m-%dT%H:%M:%S%.f"  # chrono spelling of the same format


def _tick_path(ticker, day):
    safe = "".join(c if c.isalnum() or c in ".-" else "_" for c in ticker)
    return DATA_DIR / f"{safe}-{day:%Y-%m-%d}.ndjson"


class TickStore:
    """Buffers ticks and appends them to per-ticker, per-day NDJSON files so
    other agents (e.g. strategy) can read them back via load_recent_ticks().
    Each flush appends whole lines in a single write, so a concurrent reader
    never sees a torn record.
    """

    def __init__(self, flush_every=20, max_age_seconds=5.0):
        self.flush_every = flush_every
        self.max_age_seconds = max_age_seconds
        self._buffer = []
        self._last_flush = time.monotonic()

    def add(self, tick):
        """Buffers one tick. Returns the batch that was flushed to disk, or
        [] if the tick was only buffered."""
        self._buffer.append(tick)
        if (len(self._buffer) >= self.flush_every or
                time.monotonic() - self._last_flush >= self.max_age_seconds):
            return self.flush()
        return []

    def flush(self):
        batch, self._buffer = self._buffer, []
        self._last_flush = time.monotonic()
        if not batch:
            return batch
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        # group per file: a batch can straddle midnight or (later) mix tickers
        chunks = {}
        for tick in batch:
            path = _tick_path(tick["ticker"], tick["timestamp"])
            chunks.setdefault(path, []).append(json.dumps({
                "timestamp": tick["timestamp"].strftime(_TIME_WRITE_FORMAT),
                "ticker": tick["ticker"],
                "price": tick["price"],
                "volume": tick["volume"],
            }))
        for path, lines in chunks.items():
            with open(path, "a") as f:
                f.write("\n".join(lines) + "\n")
        return batch


def load_recent_ticks(ticker, minutes=15.0):
    """Returns the last `minutes` of recorded ticks for `ticker` as a
    DataFrame with SCHEMA, sorted by timestamp (empty if nothing is stored)."""
    now = datetime.now()
    cutoff = now - timedelta(minutes=minutes)
    frames = []
    for day in sorted({cutoff.date(), now.date()}):
        path = _tick_path(ticker, day)
        if not path.exists():
            continue
        raw = path.read_bytes()
        raw = raw[:raw.rfind(b"\n") + 1]  # drop a partially appended last line
        if raw:
            frames.append(pl.read_ndjson(
                io.BytesIO(raw), schema={**SCHEMA, "timestamp": pl.String}))
    if not frames:
        return pl.DataFrame(schema=SCHEMA)
    df = pl.concat(frames).with_columns(
        pl.col("timestamp").str.to_datetime(_TIME_READ_FORMAT))
    return df.filter(pl.col("timestamp") >= cutoff).sort("timestamp")


async def _prepare_gateway(session, ticker=TICKER):
    """Validates the gateway session and resolves the contract id for ticker.
    Returns (conid, websocket session token).
    """
    # /iserver/accounts must be queried once before other /iserver endpoints
    async with session.get(f"{GATEWAY_BASE_URL}/iserver/accounts") as resp:
        if resp.status != 200:
            raise RuntimeError(f"gateway returned HTTP {resp.status} — "
                               "log in via browser at the gateway URL first")

    async with session.post(f"{GATEWAY_BASE_URL}/iserver/secdef/search",
                            json={"symbol": ticker, "secType": "STK"}) as resp:
        results = await resp.json()
    if not results:
        raise RuntimeError(f"no contract found for {ticker}")
    conid = int(results[0]["conid"])

    # /tickle keeps the session alive and returns the token the websocket
    # needs to authorize itself
    async with session.post(f"{GATEWAY_BASE_URL}/tickle") as resp:
        tickle = await resp.json()
    if not tickle.get("iserver", {}).get("authStatus", {}).get("authenticated"):
        raise RuntimeError("gateway session is not authenticated")
    return conid, tickle["session"]


async def _heartbeat(ws):
    # The gateway drops websocket connections without a heartbeat
    # at least once per minute
    while True:
        await asyncio.sleep(30)
        await ws.send_str("ech+hb")


def _parse_tick(data, ticker=TICKER):
    """Converts an smd update into our tick dict. Updates are deltas, so the
    last-price field is only present when a trade happened; skip the rest.
    """
    price_raw = data.get(FIELD_LAST_PRICE)
    if price_raw is None:
        return None
    price_str = str(price_raw)
    # A 'C' prefix means prior-day close, 'H' means trading halted — not live trades
    if price_str[:1] in ("C", "H"):
        return None
    try:
        volume = int(float(str(data.get(FIELD_LAST_SIZE, 0)).replace(",", "") or 0))
    except ValueError:
        volume = 0
    updated_ms = data.get("_updated")
    return {
        "timestamp": datetime.fromtimestamp(updated_ms / 1000) if updated_ms else datetime.now(),
        "ticker": ticker,
        "price": float(price_str),
        "volume": volume
    }


async def mock_market_stream(ticker=TICKER):
    """Streams live market ticks from the IBKR Client Portal Gateway if it is
    running and authenticated, otherwise falls back to a simulated stream.
    """
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context()))
    try:
        conid, ws_token = await _prepare_gateway(session, ticker)
    except Exception as e:
        await session.close()
        print(f"IBKR gateway not available at {GATEWAY_BASE_URL} ({e}). "
              "Falling back to simulated market stream...", flush=True)
        while True:
            # Simulating market data for testing
            yield {
                "timestamp": datetime.now(),
                "ticker": ticker,
                "price": round(random.uniform(180.0, 185.0), 2),
                "volume": random.randint(10, 500)
            }
            # Simulating a 100ms tick latency
            await asyncio.sleep(0.1)

    print(f"Connected to IBKR gateway ({ticker} conid={conid}). Opening websocket...",
          flush=True)
    try:
        async with session.ws_connect(GATEWAY_WS_URL) as ws:
            # Authorize the websocket with the session token from /tickle
            await ws.send_str(json.dumps({"session": ws_token}))
            heartbeat = asyncio.create_task(_heartbeat(ws))
            subscribed = False
            try:
                async for msg in ws:
                    if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                        raw = msg.data if isinstance(msg.data, str) else msg.data.decode()
                        try:
                            data = json.loads(raw)
                        except ValueError:
                            continue  # e.g. the 'ech+hb' echo is not JSON
                        topic = data.get("topic", "")
                        if not subscribed and topic == "system":
                            # Connection confirmed — subscribe to market data
                            fields = json.dumps({"fields": [FIELD_LAST_PRICE, FIELD_LAST_SIZE]})
                            await ws.send_str(f"smd+{conid}+{fields}")
                            subscribed = True
                            print(f"Subscribed to {ticker} trades. Streaming live ticks...",
                                  flush=True)
                        elif topic == f"smd+{conid}":
                            tick = _parse_tick(data, ticker)
                            if tick:
                                yield tick
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        print("WebSocket connection closed.")
                        break
            finally:
                heartbeat.cancel()
    finally:
        await session.close()


async def main():
    print("Starting Data Agent initialization...")
    print(f"Recording ticks to {DATA_DIR}")

    # Batches ticks and appends them to data/ticks/ for the Strategy Agent
    store = TickStore()

    async for tick in mock_market_stream():
        batch = store.add(tick)
        if batch:
            df = pl.DataFrame(batch, schema=SCHEMA)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Stored batch of {len(df)} ticks.")
            print(df.head(5))
            print("-" * 40)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Data Agent shut down safely.")
