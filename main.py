#!/usr/bin/env python3
# main.py
"""QI Trader main loop: connects the whole pipeline and runs it hourly.

Each cycle:
  1. agents/strategy.py  -- pull the day's news from Massive, score it with
     Gemini, refresh the impressions DB (industries + companies).
  2. agents/decision.py  -- have Gemini judge those impressions against
     current holdings and record BUY/SELL/HOLD decisions.
  3. gateway/execution.py -- act on the decisions that clear the guardrails
     below, then sleep until the next cycle (RUN_INTERVAL, default 1h).

Guardrails: HOLDs are never traded; BUY/SELL needs confidence >=
MIN_CONFIDENCE (default 7/10) and a limit price; quantity is capped at
MAX_QTY (default 10); at most MAX_ORDERS_PER_CYCLE (default 3) orders per
cycle; SELLs are clipped to the shares actually held. Orders inherit
execution.py's own rails: DRY_RUN=1 by default (preview only) and a refusal
to touch non-paper accounts without ALLOW_LIVE=1.

Run:  ./main.py            (DRY_RUN=0 to actually place orders)
      ./main.py once       (single cycle, no loop -- for testing)
"""
import asyncio
import os
import sys
import time
from datetime import datetime

from agents import decision, strategy
from gateway import execution, transactions

RUN_INTERVAL = int(os.environ.get("RUN_INTERVAL", "3600"))
MIN_CONFIDENCE = int(os.environ.get("MIN_CONFIDENCE", "7"))
MAX_ORDERS_PER_CYCLE = int(os.environ.get("MAX_ORDERS_PER_CYCLE", "3"))
MAX_QTY = int(os.environ.get("MAX_QTY", "10"))


def _tradeable(dec):
    """Returns (quantity, reason-if-skipped) for one decision dict."""
    if dec["action"] == "HOLD":
        return 0, "hold"
    if dec["confidence"] < MIN_CONFIDENCE:
        return 0, f"confidence {dec['confidence']} < {MIN_CONFIDENCE}"
    if not dec.get("limit_price"):
        return 0, "no limit price"
    qty = min(int(dec.get("quantity") or 1), MAX_QTY)
    if dec["action"] == "SELL":
        held = transactions.position(dec["ticker"])
        qty = min(qty, held)
        if qty <= 0:
            return 0, "nothing held to sell"
    return qty, None


async def act_on(decisions, db_path=None):
    """Executes the decisions that clear the guardrails; returns how many
    orders were sent to the gateway."""
    placed = 0
    for dec in decisions:
        label = f"{dec['action']} {dec['ticker']}"
        if placed >= MAX_ORDERS_PER_CYCLE:
            print(f"  skip {label}: order cap reached "
                  f"({MAX_ORDERS_PER_CYCLE}/cycle)")
            continue
        qty, skip = _tradeable(dec)
        if skip:
            print(f"  skip {label}: {skip}")
            continue
        print(f"  {label} x{qty} LMT {dec['limit_price']} "
              f"(confidence {dec['confidence']}/10)")
        try:
            await execution.execute_signal(dec["action"], dec["ticker"],
                                           float(dec["limit_price"]), qty)
            decision.mark_executed(dec["id"], db_path)
            placed += 1
        except Exception as e:  # one bad order must not kill the cycle
            print(f"  ERROR executing {label}: {e}")
    return placed


async def run_cycle():
    print(f"\n=== Cycle started {datetime.now():%Y-%m-%d %H:%M:%S} "
          f"(DRY_RUN={'on' if execution.DRY_RUN else 'OFF -- live orders!'}) ===")
    articles, industries, companies = await strategy.run_daily()
    print(f"Impressions updated: {industries} industries, "
          f"{companies} companies from {articles} articles.")

    decisions = await decision.decide()
    if not decisions:
        print("No decisions this cycle.")
        return
    for d in decisions:
        print(f"  {d['action']:<4} {d['ticker']:<6} "
              f"confidence {d['confidence']}/10 -- {d.get('reasoning', '')}")

    placed = await act_on(decisions)
    print(f"Cycle done: {placed} order(s) sent to the gateway.")


async def main_loop():
    print(f"QI Trader main loop: one cycle every {RUN_INTERVAL}s. Ctrl-C stops.")
    while True:
        started = time.monotonic()
        try:
            await run_cycle()
        except Exception as e:  # a failed cycle should not stop the loop
            print(f"Cycle failed: {e}")
        wait = max(0.0, RUN_INTERVAL - (time.monotonic() - started))
        print(f"Next cycle at {datetime.fromtimestamp(time.time() + wait):%H:%M:%S}.")
        await asyncio.sleep(wait)


if __name__ == "__main__":
    try:
        if "once" in sys.argv[1:]:
            asyncio.run(run_cycle())
        else:
            asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nQI Trader shut down safely.")
