import asyncio
import json
import os
import random
from datetime import datetime
import polars as pl

try:
    from agents import features
    from agents.risk import RiskManager
    from gateway.execution import execute_signal
    from gateway.ingestion import DATA_DIR, load_recent_ticks
    from gateway.ib_gateway import TICKER
except ImportError:  # when run directly as ./agents/strategy.py
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from agents import features
    from agents.risk import RiskManager
    from gateway.execution import execute_signal
    from gateway.ingestion import DATA_DIR, load_recent_ticks
    from gateway.ib_gateway import TICKER

POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "5"))
LOOKBACK_MINUTES = float(os.environ.get("LOOKBACK_MINUTES", "15"))
STRATEGY = os.environ.get("STRATEGY", "baseline").lower()  # baseline | ai
ORDER_QTY = int(os.environ.get("ORDER_QTY", "1"))


class BaselineStrategy:
    """Deterministic SMA-crossover rule: BUY when the short SMA moves above
    the long SMA, SELL when it moves below, HOLD otherwise. This is the
    benchmark any AI decision maker (STRATEGY=ai) has to beat, and it returns
    the same response shape as call_ai_api so the two are swappable.
    """

    def __init__(self):
        self._regime = None  # which side of the long SMA the short SMA is on

    async def decide(self, data: pl.DataFrame) -> dict:
        base = {"timestamp": datetime.now().isoformat()}
        if len(data) < features.LONG_WINDOW:
            return {**base, "decision": "HOLD", "confidence": 1.0,
                    "reasoning": f"warming up: {len(data)}/{features.LONG_WINDOW} "
                                 "ticks in the window"}
        last = features.compute_indicators(data).row(-1, named=True)
        short, long_ = last["sma_short"], last["sma_long"]
        regime = "above" if short > long_ else "below"
        previous, self._regime = self._regime, regime
        gap_pct = (short - long_) / long_ * 100
        if previous is None or regime == previous:
            decision = "HOLD"
            reasoning = (f"no crossover: short SMA {short:.2f} stays {regime} "
                         f"long SMA {long_:.2f}")
        else:
            decision = "BUY" if regime == "above" else "SELL"
            reasoning = (f"short SMA {short:.2f} crossed {regime} long SMA "
                         f"{long_:.2f} ({gap_pct:+.3f}%)")
        return {**base, "decision": decision,
                "confidence": round(min(0.95, 0.5 + abs(gap_pct) * 2), 2),
                "reasoning": reasoning}

# To use real HTTP calls, you would install aiohttp: `pip install aiohttp`
# import aiohttp 

async def call_ai_api(data: pl.DataFrame) -> dict:
    """
    Sends the aggregated market data to an AI API (e.g., OpenAI, Gemini, Anthropic) 
    and receives a trading signal.
    """
    # Convert Polars DataFrame to JSON string to send to the API
    # In a real system, you might only send summary statistics or technical indicators to save tokens.
    data_json = data.write_json()
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Sending batch of {len(data)} records to AI API...")
    
    # =====================================================================
    # REAL API CALL EXAMPLE (Using aiohttp)
    # =====================================================================
    # async with aiohttp.ClientSession() as session:
    #     api_key = os.environ.get("AI_API_KEY", "YOUR_API_KEY_HERE")
    #     headers = {
    #         "Authorization": f"Bearer {api_key}",
    #         "Content-Type": "application/json"
    #     }
    #     payload = {
    #         "model": "gpt-4o",  # Or your chosen model
    #         "messages": [
    #             {
    #                 "role": "system", 
    #                 "content": "You are a quantitative trading AI. Analyze the provided market data JSON. Output only a JSON object with 'decision' (BUY, SELL, HOLD), 'confidence' (0.0 to 1.0), and 'reasoning'."
    #             },
    #             {
    #                 "role": "user", 
    #                 "content": data_json
    #             }
    #         ],
    #         "response_format": {"type": "json_object"}
    #     }
    #     try:
    #         async with session.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers) as resp:
    #             if resp.status == 200:
    #                 result = await resp.json()
    #                 content = result['choices'][0]['message']['content']
    #                 return json.loads(content)
    #             else:
    #                 error_text = await resp.text()
    #                 print(f"API Error ({resp.status}): {error_text}")
    #     except Exception as e:
    #         print(f"Network error: {e}")
    # =====================================================================
    
    # --- MOCK API RESPONSE (Simulating the AI thinking) ---
    await asyncio.sleep(1.0)  # Simulate network and processing latency
    
    decision = random.choices(["BUY", "SELL", "HOLD"], weights=[0.15, 0.15, 0.70])[0]
    confidence = round(random.uniform(0.6, 0.95), 2)
    
    return {
        "timestamp": datetime.now().isoformat(),
        "decision": decision,
        "confidence": confidence,
        "reasoning": f"Simulated AI analysis based on {len(data)} recent ticks. Volume and price action indicated a {decision} signal."
    }

async def process_data_stream():
    """
    Polls the ticks the Ingestion Agent records under data/ticks/ and forwards
    each window that contains new data to the decision maker (the SMA baseline
    by default, the AI with STRATEGY=ai). Every actionable signal must pass
    the RiskManager before it reaches the Execution Agent.
    """
    decide = call_ai_api if STRATEGY == "ai" else BaselineStrategy().decide
    risk_mgr = RiskManager(TICKER)
    print(f"Strategy '{STRATEGY}', order size {ORDER_QTY} — resuming with "
          f"position {risk_mgr.position:+d} shares, realized P&L today "
          f"{risk_mgr.realized_pnl_today:+.2f}")
    print(f"Watching {TICKER} ticks in {DATA_DIR} "
          f"(last {LOOKBACK_MINUTES:g} min, polling every {POLL_SECONDS:g}s)...")
    last_seen = None  # newest tick timestamp of the previous round

    while True:
        await asyncio.sleep(POLL_SECONDS)
        now = datetime.now().strftime('%H:%M:%S')

        df = load_recent_ticks(TICKER, minutes=LOOKBACK_MINUTES)
        if df.is_empty():
            print(f"[{now}] No {TICKER} ticks in the last {LOOKBACK_MINUTES:g} min — "
                  "start the ingestion stream (dashboard or gateway/ingestion.py).")
            continue
        newest = df["timestamp"][-1]
        if last_seen is not None and newest <= last_seen:
            print(f"[{now}] No new {TICKER} ticks since the last decision. Waiting...")
            continue
        last_seen = newest

        # --- Sending data to the decision maker ---
        response = await decide(df)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Decision received:")
        print(json.dumps(response, indent=2))

        decision = response.get("decision")
        if decision in ["BUY", "SELL"]:
            last_price = float(df["price"][-1])
            veto = risk_mgr.check(decision, ORDER_QTY, last_price)
            if veto:
                print(f"--> RISK VETO on {decision} {ORDER_QTY} {TICKER}: {veto}")
            else:
                print(f"--> Actionable signal! Forwarding {decision} order to Execution Agent...")
                risk_mgr.record_order()  # cooldown starts on the attempt
                result = None
                try:
                    # Uses the latest tick as the limit price. execution.py defaults
                    # to DRY_RUN (preview only); set DRY_RUN=0 to actually trade.
                    result = await execute_signal(decision, TICKER, last_price,
                                                  quantity=ORDER_QTY)
                except Exception as e:
                    print(f"--> Execution Agent failed: {e}")
                if result and result.get("status") == "Filled":
                    # the gateway's order fields can be missing or strings;
                    # fall back to what we asked for
                    qty = int(float(result.get("filledQuantity") or ORDER_QTY))
                    fill_price = float(result.get("avgPrice") or last_price)
                    pnl = risk_mgr.record_fill(decision, qty, fill_price)
                    print(f"--> Filled {decision} {qty} @ {fill_price:.2f}: "
                          f"position {risk_mgr.position:+d}, "
                          f"realized P&L today {pnl:+.2f}")
        else:
            print("--> Holding position. No action taken.")

        print("-" * 60)

if __name__ == "__main__":
    print("Starting AI Strategy Agent initialization...")
    try:
        asyncio.run(process_data_stream())
    except KeyboardInterrupt:
        print("Strategy Agent shut down safely.")
