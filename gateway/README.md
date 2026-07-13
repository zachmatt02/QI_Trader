# QI Trader Gateway

Everything that talks to the IBKR Client Portal Gateway: market-data ingestion, order execution, the shared connection settings, and the transaction ledger that records fills. The vendored gateway application itself (Java) lives in `clientportal/` at the repo root and is started via `./gateway.sh`.

## Files

### Data Ingestion Agent (`ingestion.py`)
* **Role**: Handles real-time market data ingestion.
* **Functionality**:
  * Streams live ticks from the IBKR gateway websocket (raises if the gateway is unreachable or its session is not authenticated).
  * Buffers and batches incoming market ticks (`TickStore`: flushes every 20 ticks or 5 seconds).
  * Records each batch to `data/ticks/<ticker>-<date>.ndjson` so the Strategy Agent (`agents/strategy.py`) can read it back with `load_recent_ticks()` (strict Polars schema, safe to read while being appended).

### Execution Agent (`execution.py`)
* **Role**: Places stock orders through the IBKR Client Portal Gateway.
* **Functionality**:
  * Previews orders via the gateway's `/whatif` endpoint (estimated cost, commission, margin impact) before submitting.
  * Places limit orders and automatically answers the gateway's confirmation prompts (price-cap warnings etc.), then polls the order until it reaches a terminal status.
  * Safety rails: `DRY_RUN=1` by default (preview only — set `DRY_RUN=0` to trade), and refuses non-paper accounts unless `ALLOW_LIVE=1`.
  * One-off order from the shell: `DRY_RUN=0 SIDE=BUY QTY=1 LIMIT_PRICE=420 TICKER=TSLA ./gateway/execution.py`

### Transaction Ledger (`transactions.py`)
Not an agent — a SQLite ledger (`data/transactions.db`, path via `TRANSACTIONS_DB`) with a single `transactions` table recording every executed trade: ticker, ISIN, share price, currency, datetime (UTC), shares, buy flag (1 = buy, 0 = sell), plus broker context (order id, account, conid, commission, status). The Execution Agent and dashboard insert a row automatically whenever an order fills; `record_transaction()`, `list_transactions()` and `position()` are available for other agents. Run `./gateway/transactions.py` to initialise the file and print recorded trades and net positions.

### IBKR Gateway Settings (`ib_gateway.py`)
Not an agent — the connection settings (`GATEWAY_BASE_URL`, `GATEWAY_WS_URL`, `TICKER`) and the self-signed-certificate `ssl_context()` helper shared by the ingestion and execution agents.
