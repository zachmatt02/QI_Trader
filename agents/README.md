# QI Trader Agents

The decision-making side of the pipeline: everything AI/strategy related lives here. These modules read the market data recorded by the gateway side (`gateway/`), decide trades, and enforce the safety limits every signal must pass before it reaches the broker.

## Files

### Strategy Agent (`strategy.py`)
* **Role**: Watches the recorded market data and decides trades.
* **Functionality**:
  * Polls the ticks recorded by the ingestion agent (`data/ticks/`, last `LOOKBACK_MINUTES` window every `POLL_SECONDS`) and skips rounds with no new data.
  * Two swappable decision makers returning `{decision, confidence, reasoning}`:
    * `STRATEGY=baseline` (default): deterministic SMA-crossover rule (`features.py` windows) — the benchmark the AI has to beat.
    * `STRATEGY=ai`: sends the window to an AI model (currently mocked; includes API examples) for a `BUY`/`SELL`/`HOLD` decision.
  * Every actionable signal must pass `risk.RiskManager` before it is forwarded to the Execution Agent (`gateway/execution.py`, `ORDER_QTY` shares, latest tick as limit price); fills update position and realized P&L.

### Technical Indicators (`features.py`)
Not an agent — rolling indicators over tick DataFrames: `compute_indicators()` (short/long SMA, VWAP; windows via `SHORT_WINDOW`/`LONG_WINDOW`, in ticks) and `summarize()` (a compact dict of the window for LLM prompts and logging).

### Risk Rails (`risk.py`)
Not an agent — `RiskManager` vetoes orders that break the hard limits, all enforced in code (never by the model): `MAX_POSITION` (±shares), `MAX_ORDER_NOTIONAL`, `ORDER_COOLDOWN_SECONDS` between order attempts, `MAX_DAILY_LOSS` (realized, per day), and a kill switch (`touch data/KILL_SWITCH` blocks all orders until removed). Position/P&L state survives restarts in `data/risk/<ticker>.json`.
