#!/usr/bin/env python3
# agents/execution.py
"""Execution Agent: places stock orders through the IBKR Client Portal Gateway.

Safety rails:
  * DRY_RUN=1 (the default) only previews the order via /whatif — nothing is
    submitted. Set DRY_RUN=0 to actually place orders.
  * Order size is capped at MAX_ORDER_QTY shares (default 10).
  * Refuses to trade on a non-paper account (id not starting with "DU")
    unless ALLOW_LIVE=1 is set explicitly.

Run directly for a one-off order:
  DRY_RUN=0 SIDE=BUY QTY=1 LIMIT_PRICE=420 TICKER=TSLA ./agents/execution.py
"""
import asyncio
import json
import os
import time
import uuid
from datetime import datetime

import aiohttp

try:
    from agents.ib_gateway import GATEWAY_BASE_URL, TICKER, ssl_context
except ImportError:  # when run directly as ./agents/execution.py
    from ib_gateway import GATEWAY_BASE_URL, TICKER, ssl_context

MAX_ORDER_QTY = int(os.environ.get("MAX_ORDER_QTY", "10"))
DRY_RUN = os.environ.get("DRY_RUN", "1") != "0"
ALLOW_LIVE = os.environ.get("ALLOW_LIVE") == "1"

# The order endpoint usually answers with confirmation questions (price cap
# warnings etc.) instead of an order id; each must be confirmed via
# /iserver/reply before the order is actually submitted.
MAX_CONFIRMATIONS = 5


async def get_account_id(session):
    async with session.get(f"{GATEWAY_BASE_URL}/iserver/accounts") as resp:
        if resp.status != 200:
            raise RuntimeError(f"gateway returned HTTP {resp.status} — "
                               "log in via browser at the gateway URL first")
        data = await resp.json()
    account_id = data["accounts"][0]
    if not account_id.startswith("DU") and not ALLOW_LIVE:
        raise RuntimeError(f"{account_id} is not a paper account; refusing to "
                           "trade (set ALLOW_LIVE=1 to override)")
    return account_id


async def search_conid(session, ticker):
    async with session.post(f"{GATEWAY_BASE_URL}/iserver/secdef/search",
                            json={"symbol": ticker, "secType": "STK"}) as resp:
        results = await resp.json()
    if not results:
        raise RuntimeError(f"no contract found for {ticker}")
    return int(results[0]["conid"])


def build_order(conid, side, quantity, price=None, order_type="LMT", tif="DAY"):
    side = side.upper()
    if side not in ("BUY", "SELL"):
        raise ValueError(f"side must be BUY or SELL, got {side!r}")
    if not 0 < quantity <= MAX_ORDER_QTY:
        raise ValueError(f"quantity must be between 1 and {MAX_ORDER_QTY} "
                         "(raise MAX_ORDER_QTY to allow more)")
    order = {
        "conid": conid,
        "orderType": order_type,
        "side": side,
        "quantity": quantity,
        "tif": tif,
        # A unique client order id makes the gateway reject accidental duplicates
        "cOID": f"qi-trader-{uuid.uuid4().hex[:12]}",
    }
    if order_type == "LMT":
        if price is None:
            raise ValueError("a limit order needs a price")
        order["price"] = price
    return order


async def preview_order(session, account_id, order):
    """Asks the gateway what the order would cost, without submitting it."""
    url = f"{GATEWAY_BASE_URL}/iserver/account/{account_id}/orders/whatif"
    async with session.post(url, json={"orders": [order]}) as resp:
        return await resp.json()


async def place_order(session, account_id, order):
    """Submits the order and answers the gateway's confirmation prompts.
    Returns the final response entry (contains 'order_id')."""
    url = f"{GATEWAY_BASE_URL}/iserver/account/{account_id}/orders"
    async with session.post(url, json={"orders": [order]}) as resp:
        result = await resp.json()

    for _ in range(MAX_CONFIRMATIONS):
        if isinstance(result, dict):
            # a dict here is always a failure, e.g. {"error": "..."}
            raise RuntimeError(f"order rejected: {result.get('error', result)}")
        reply = result[0]
        if "order_id" in reply:
            return reply
        if "id" not in reply:
            raise RuntimeError(f"unexpected order response: {reply}")
        print(f"  Confirming gateway prompt: {' '.join(reply.get('message', []))!r}")
        async with session.post(f"{GATEWAY_BASE_URL}/iserver/reply/{reply['id']}",
                                json={"confirmed": True}) as resp:
            result = await resp.json()
    raise RuntimeError(f"gave up after {MAX_CONFIRMATIONS} confirmation prompts")


async def wait_for_status(session, order_id, timeout=60.0, poll_interval=2.0):
    """Polls the order list until the order reaches a terminal status or the
    timeout expires. Returns the last seen order dict (None if never seen)."""
    deadline = time.monotonic() + timeout
    last = None
    last_status = None
    while time.monotonic() < deadline:
        async with session.get(f"{GATEWAY_BASE_URL}/iserver/account/orders") as resp:
            data = await resp.json()
        for order in data.get("orders") or []:
            if str(order.get("orderId")) == str(order_id):
                last = order
                if order.get("status") != last_status:
                    last_status = order.get("status")
                    print(f"  Order {order_id}: {last_status}")
                if last_status in ("Filled", "Cancelled", "Inactive"):
                    return last
        await asyncio.sleep(poll_interval)
    return last


async def cancel_order(session, account_id, order_id):
    url = f"{GATEWAY_BASE_URL}/iserver/account/{account_id}/order/{order_id}"
    async with session.delete(url) as resp:
        return await resp.json()


async def execute_signal(side, ticker, limit_price, quantity=1):
    """End-to-end entry point for the Strategy Agent: previews — and, unless
    DRY_RUN is on, places and tracks — a limit order for `ticker`."""
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context()))
    try:
        account_id = await get_account_id(session)
        conid = await search_conid(session, ticker)
        order = build_order(conid, side, quantity, price=limit_price)

        preview = await preview_order(session, account_id, order)
        print(f"[{datetime.now():%H:%M:%S}] Preview: {order['side']} {quantity} "
              f"{ticker} LMT {limit_price} on {account_id}")
        if isinstance(preview, dict) and "amount" in preview:
            amount = preview["amount"]
            print(f"  Estimated total {amount.get('total')} "
                  f"(commission {amount.get('commission')})")
        else:
            print(f"  {json.dumps(preview)}")

        if DRY_RUN:
            print("  DRY_RUN is on — order not submitted. Set DRY_RUN=0 to trade.")
            return None

        placed = await place_order(session, account_id, order)
        order_id = placed["order_id"]
        print(f"  Order {order_id} submitted "
              f"({placed.get('order_status', 'status unknown')}).")
        return await wait_for_status(session, order_id)
    finally:
        await session.close()


async def main():
    print("Starting Execution Agent...")
    price = os.environ.get("LIMIT_PRICE")
    if price is None:
        raise SystemExit("Set LIMIT_PRICE=<price> (and optionally SIDE=BUY|SELL, "
                         "QTY=<n>, TICKER=<symbol>, DRY_RUN=0)")
    result = await execute_signal(os.environ.get("SIDE", "BUY"), TICKER,
                                  float(price), int(os.environ.get("QTY", "1")))
    if result:
        print(f"Final order state: {result.get('status')}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Execution Agent shut down safely.")
