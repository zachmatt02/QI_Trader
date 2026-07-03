# QI Trader Agents

The active workforce. This folder contains the independent, running programs that actually do the work: pulling live market data, predicting price movements, enforcing safety rules, and sending the final buy/sell orders to the broker.

## Agent Files

### 1. Data Ingestion Agent (`ingestion.py`)
* **Role**: Handles real-time market data ingestion.
* **Functionality**:
  * Simulates an incoming high-frequency WebSocket tick stream (mocking TSLA tick data for testing).
  * Buffers and batches incoming market ticks (e.g., in chunks of 20).
  * Converts raw tick records into structured Polars DataFrames using a strict schema.
  * In production, this agent is responsible for saving data (e.g., to TimescaleDB or Parquet files) so that strategy agents can read it.

### 2. AI Strategy Agent (`strategy.py`)
* **Role**: Analyzes market data and queries AI models to retrieve trading signals.
* **Functionality**:
  * Simulates retrieving batches of aggregated market data (polling-based).
  * Serializes Polars DataFrames to JSON and prepares a prompt structure for the LLM.
  * Calls an AI model API (includes examples for OpenAI, Gemini, etc.) to analyze the data and return a JSON trading decision (`BUY`, `SELL`, `HOLD`) with a confidence level and reasoning.
  * Forwards actionable signals (`BUY` or `SELL`) to the Execution Agent.

### 3. Execution Agent (`execution.py`)
* **Role**: Places stock orders through the IBKR Client Portal Gateway.
* **Functionality**:
  * Previews orders via the gateway's `/whatif` endpoint (estimated cost, commission, margin impact) before submitting.
  * Places limit orders and automatically answers the gateway's confirmation prompts (price-cap warnings etc.), then polls the order until it reaches a terminal status.
  * Safety rails: `DRY_RUN=1` by default (preview only — set `DRY_RUN=0` to trade), order size capped at `MAX_ORDER_QTY` (default 10), and refuses non-paper accounts unless `ALLOW_LIVE=1`.
  * One-off order from the shell: `DRY_RUN=0 SIDE=BUY QTY=1 LIMIT_PRICE=420 TICKER=TSLA ./agents/execution.py`

### Control Dashboard (`dashboard.py`)
* **Role**: Local web UI to control the ingestion and execution agents by hand.
* **Functionality**:
  * Start/stop the ingestion tick stream and watch it live: last price, tick count, a price chart with hover readout, and a recent-ticks table.
  * Preview (`/whatif`) and submit limit orders through the execution agent's functions — same safety rails: Submit is disabled while `DRY_RUN=1` (the default), quantity capped at `MAX_ORDER_QTY`, non-paper accounts refused without `ALLOW_LIVE=1`.
  * Binds to localhost only (it can place orders). Run with `./venv/bin/python agents/dashboard.py`, then open http://127.0.0.1:8080 (port via `DASHBOARD_PORT`).

### Shared: IBKR Gateway Settings (`ib_gateway.py`)
Not an agent — the connection settings (`GATEWAY_BASE_URL`, `GATEWAY_WS_URL`, `TICKER`) and the self-signed-certificate `ssl_context()` helper shared by the ingestion and execution agents.
