#!/usr/bin/env python3
# gateway/execution.py
"""Execution Agent: places stock orders through the IBKR Client Portal Gateway.

Safety rails:
  * DRY_RUN=1 (the default) only previews the order via /whatif — nothing is
    submitted. Set DRY_RUN=0 to actually place orders.
  * Refuses to trade on a non-paper account (id not starting with "DU")
    unless ALLOW_LIVE=1 is set explicitly.

Run directly for a one-off order:
  DRY_RUN=0 SIDE=BUY QTY=1 LIMIT_PRICE=420 TICKER=TSLA ./gateway/execution.py

Backfill fills the ledger missed (also runs at the start of every main.py
cycle):
  ./gateway/execution.py reconcile
"""
import asyncio
import json
import os
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone

import aiohttp

try:
    from gateway.ib_gateway import GATEWAY_BASE_URL, TICKER, ssl_context
    from gateway import transactions
except ImportError:  # when run directly as ./gateway/execution.py
    from ib_gateway import GATEWAY_BASE_URL, TICKER, ssl_context
    import transactions

DRY_RUN = os.environ.get("DRY_RUN", "1") != "0"
ALLOW_LIVE = os.environ.get("ALLOW_LIVE") == "1"

# The order endpoint usually answers with confirmation questions (price cap
# warnings etc.) instead of an order id; each must be confirmed via
# /iserver/reply before the order is actually submitted.
MAX_CONFIRMATIONS = 5

# Once an order reaches one of these it will never fill any further.
TERMINAL_STATUSES = ("Filled", "Cancelled", "Inactive")


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
    if quantity <= 0:
        raise ValueError(f"quantity must be positive, got {quantity}")
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
                if last_status in TERMINAL_STATUSES:
                    return last
        await asyncio.sleep(poll_interval)
    return last


async def fetch_orders(session):
    """Returns the account's orders for the day. The endpoint answers []
    on its first call while the gateway warms up, so an empty result is
    retried once."""
    for attempt in (1, 2):
        async with session.get(f"{GATEWAY_BASE_URL}/iserver/account/orders") as resp:
            data = await resp.json()
        orders = data.get("orders") or []
        if orders or attempt == 2:
            return orders
        await asyncio.sleep(1)


async def reconcile_fills():
    """Backfills the transaction ledger with fills it missed: an order that
    fills after wait_for_status stops polling (or while this process is
    down) never goes through record_fill. Scans the day's orders on the
    gateway and records every terminal order with filled shares whose order
    id the ledger does not know yet. Best-effort: returns the number of
    rows added, 0 if the gateway is unreachable."""
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context()))
    try:
        account_id = await get_account_id(session)
        orders = await fetch_orders(session)
    except Exception as e:
        print(f"  Fill reconciliation skipped (gateway not available: {e})")
        return 0
    finally:
        await session.close()

    known = transactions.recorded_order_ids()
    added = 0
    for order in orders:
        order_id = str(order.get("orderId") or "")
        filled = int(float(order.get("filledQuantity") or 0))
        if (order.get("status") not in TERMINAL_STATUSES or filled <= 0
                or not order_id or order_id in known):
            continue
        price = order.get("avgPrice") or order.get("price")
        filled_at = order.get("lastExecutionTime_r")  # epoch milliseconds
        try:
            row_id = transactions.record_transaction(
                ticker=order.get("ticker"),
                share_price=float(price),
                shares=filled,
                buy=order.get("side") or "BUY",
                currency=order.get("cashCcy") or "USD",
                when=(datetime.fromtimestamp(filled_at / 1000, timezone.utc)
                      if filled_at else None),
                order_id=order_id,
                account_id=account_id,
                conid=order.get("conid"))
            print(f"  Reconciled missed fill: {order.get('side', 'BUY')} "
                  f"{filled} {order.get('ticker')} @ {price} "
                  f"(order {order_id}, row {row_id}).")
            added += 1
        except (sqlite3.Error, ValueError, TypeError, AttributeError) as e:
            print(f"  WARNING: could not reconcile order {order_id}: {e}")
    return added


def record_fill(order_state, account_id, ticker, quantity, limit_price):
    """Writes a filled order into the transaction ledger (transactions.py).
    Best-effort: a ledger problem must never look like a trading failure."""
    if not order_state or order_state.get("status") != "Filled":
        return
    try:
        row_id = transactions.record_transaction(
            ticker=order_state.get("ticker") or ticker,
            share_price=float(order_state.get("avgPrice") or limit_price),
            shares=int(float(order_state.get("filledQuantity") or quantity)),
            buy=order_state.get("side") or "BUY",
            currency=order_state.get("cashCcy") or "USD",
            order_id=str(order_state.get("orderId", "")) or None,
            account_id=account_id,
            conid=order_state.get("conid"))
        print(f"  Recorded fill in transaction ledger (row {row_id}).")
    except (sqlite3.Error, ValueError, TypeError) as e:
        print(f"  WARNING: fill executed but not recorded in ledger: {e}")


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
        final = await wait_for_status(session, order_id)
        record_fill(final, account_id, ticker, quantity, limit_price)
        return final
    finally:
        await session.close()


async def main():
    if "reconcile" in sys.argv[1:]:
        print("Reconciling the transaction ledger with today's orders...")
        added = await reconcile_fills()
        print(f"Reconciliation done: {added} fill(s) added.")
        return
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
