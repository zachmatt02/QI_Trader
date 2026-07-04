# QI Trader Agents

The active workforce. This folder contains the independent, running programs that actually do the work: pulling live market data, predicting price movements, enforcing safety rules, and sending the final buy/sell orders to the broker.

## Agent Files

### 1. Data Ingestion Agent (`ingestion.py`)
* **Role**: Handles real-time market data ingestion.
* **Functionality**:
  * Streams live ticks from the IBKR gateway websocket (falls back to a simulated stream when the gateway is unreachable).
  * Buffers and batches incoming market ticks (`TickStore`: flushes every 20 ticks or 5 seconds).
  * Records each batch to `data/ticks/<ticker>-<date>.ndjson` so the Strategy Agent can read it back with `load_recent_ticks()` (strict Polars schema, safe to read while being appended).

### 2. Strategy Agent (`strategy.py`)
* **Role**: Watches the recorded market data and decides trades.
* **Functionality**:
  * Polls the ticks recorded by the ingestion agent (`data/ticks/`, last `LOOKBACK_MINUTES` window every `POLL_SECONDS`) and skips rounds with no new data.
  * Two swappable decision makers returning `{decision, confidence, reasoning}`:
    * `STRATEGY=baseline` (default): deterministic SMA-crossover rule (`features.py` windows) — the benchmark the AI has to beat.
    * `STRATEGY=ai`: sends the window to an AI model (currently mocked; includes API examples) for a `BUY`/`SELL`/`HOLD` decision.
  * Every actionable signal must pass `risk.RiskManager` before it is forwarded to the Execution Agent (`ORDER_QTY` shares, latest tick as limit price); fills update position and realized P&L.

### 3. Execution Agent (`execution.py`)
* **Role**: Places stock orders through the IBKR Client Portal Gateway.
* **Functionality**:
  * Previews orders via the gateway's `/whatif` endpoint (estimated cost, commission, margin impact) before submitting.
  * Places limit orders and automatically answers the gateway's confirmation prompts (price-cap warnings etc.), then polls the order until it reaches a terminal status.
  * Safety rails: `DRY_RUN=1` by default (preview only — set `DRY_RUN=0` to trade), and refuses non-paper accounts unless `ALLOW_LIVE=1`.
  * One-off order from the shell: `DRY_RUN=0 SIDE=BUY QTY=1 LIMIT_PRICE=420 TICKER=TSLA ./agents/execution.py`

### Control Dashboard (`dashboard.py`)
* **Role**: Local web UI to control the ingestion and execution agents by hand.
* **Functionality**:
  * Start/stop the ingestion tick stream and watch it live: last price, tick count, a price chart with hover readout, and a recent-ticks table. Streamed ticks are also recorded to `data/ticks/` for the strategy agent.
  * Preview (`/whatif`) and submit limit orders through the execution agent's functions — same safety rails: Submit is disabled while `DRY_RUN=1` (the default), non-paper accounts refused without `ALLOW_LIVE=1`.
  * A Transactions page (`/transactions`) showing everything in the transaction ledger (`data/transactions.db`): every recorded trade with all its columns (filterable by ticker, auto-refreshing) and net positions per ticker.
  * Binds to localhost only (it can place orders). Run with `./venv/bin/python agents/dashboard.py`, then open http://127.0.0.1:8080 (port via `DASHBOARD_PORT`).

### Shared: Technical Indicators (`features.py`)
Not an agent — rolling indicators over tick DataFrames: `compute_indicators()` (short/long SMA, VWAP; windows via `SHORT_WINDOW`/`LONG_WINDOW`, in ticks) and `summarize()` (a compact dict of the window for LLM prompts and logging).

### Shared: Risk Rails (`risk.py`)
Not an agent — `RiskManager` vetoes orders that break the hard limits, all enforced in code (never by the model): `MAX_POSITION` (±shares), `MAX_ORDER_NOTIONAL`, `ORDER_COOLDOWN_SECONDS` between order attempts, `MAX_DAILY_LOSS` (realized, per day), and a kill switch (`touch data/KILL_SWITCH` blocks all orders until removed). Position/P&L state survives restarts in `data/risk/<ticker>.json`.

### Shared: Transaction Ledger (`transactions.py`)
Not an agent — a SQLite ledger (`data/transactions.db`, path via `TRANSACTIONS_DB`) with a single `transactions` table recording every executed trade: ticker, ISIN, share price, currency, datetime (UTC), shares, buy flag (1 = buy, 0 = sell), plus broker context (order id, account, conid, commission, status). The Execution Agent and dashboard insert a row automatically whenever an order fills; `record_transaction()`, `list_transactions()` and `position()` are available for other agents. Run `./agents/transactions.py` to initialise the file and print recorded trades and net positions.

### Shared: IBKR Gateway Settings (`ib_gateway.py`)
Not an agent — the connection settings (`GATEWAY_BASE_URL`, `GATEWAY_WS_URL`, `TICKER`) and the self-signed-certificate `ssl_context()` helper shared by the ingestion and execution agents.
