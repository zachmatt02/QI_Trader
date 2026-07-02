#!/usr/bin/env python3
# agents/ingestion.py
import polars as pl
import asyncio
import json
import aiohttp
import random
from datetime import datetime

try:
    from agents.ib_gateway import GATEWAY_BASE_URL, GATEWAY_WS_URL, TICKER, ssl_context
except ImportError:  # when run directly as ./agents/ingestion.py
    from ib_gateway import GATEWAY_BASE_URL, GATEWAY_WS_URL, TICKER, ssl_context

# Client Portal streaming tick fields (see gateway/doc/RealtimeSubscription.md):
FIELD_LAST_PRICE = "31"
FIELD_LAST_SIZE = "7059"


async def _prepare_gateway(session):
    """Validates the gateway session and resolves the contract id for TICKER.
    Returns (conid, websocket session token).
    """
    # /iserver/accounts must be queried once before other /iserver endpoints
    async with session.get(f"{GATEWAY_BASE_URL}/iserver/accounts") as resp:
        if resp.status != 200:
            raise RuntimeError(f"gateway returned HTTP {resp.status} — "
                               "log in via browser at the gateway URL first")

    async with session.post(f"{GATEWAY_BASE_URL}/iserver/secdef/search",
                            json={"symbol": TICKER, "secType": "STK"}) as resp:
        results = await resp.json()
    if not results:
        raise RuntimeError(f"no contract found for {TICKER}")
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


def _parse_tick(data):
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
        "ticker": TICKER,
        "price": float(price_str),
        "volume": volume
    }


async def mock_market_stream():
    """Streams live market ticks from the IBKR Client Portal Gateway if it is
    running and authenticated, otherwise falls back to a simulated stream.
    """
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context()))
    try:
        conid, ws_token = await _prepare_gateway(session)
    except Exception as e:
        await session.close()
        print(f"IBKR gateway not available at {GATEWAY_BASE_URL} ({e}). "
              "Falling back to simulated market stream...")
        while True:
            # Simulating TSLA data for testing
            yield {
                "timestamp": datetime.now(),
                "ticker": TICKER,
                "price": round(random.uniform(180.0, 185.0), 2),
                "volume": random.randint(10, 500)
            }
            # Simulating a 100ms tick latency
            await asyncio.sleep(0.1)

    print(f"Connected to IBKR gateway ({TICKER} conid={conid}). Opening websocket...")
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
                            print(f"Subscribed to {TICKER} trades. Streaming live ticks...")
                        elif topic == f"smd+{conid}":
                            tick = _parse_tick(data)
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

    # Define the strict schema for Polars
    schema = {
        "timestamp": pl.Datetime,
        "ticker": pl.String,
        "price": pl.Float64,
        "volume": pl.Int64
    }

    # Buffer to hold ticks before converting to a DataFrame
    buffer = []

    async for tick in mock_market_stream():
        buffer.append(tick)

        # Process and flush the buffer in batches of 20 ticks
        if len(buffer) >= 20:
            # Convert the list of dicts into a Polars DataFrame
            df = pl.DataFrame(buffer, schema=schema)

            # Example Polars operation: Calculate VWAP or simple rolling metrics
            # In a real system, you would append this DataFrame to a TimescaleDB instance
            # or save it as a Parquet file for the Strategy Agent to read.
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Processed Batch of {len(df)} ticks.")
            print(df.head(5))
            print("-" * 40)

            buffer.clear()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Data Agent shut down safely.")
