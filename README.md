# QI Trader

Agent-based trading pipeline on the IBKR Client Portal Gateway.

## Layout

* **`agents/`** — the AI/decision side: the strategy agent, technical indicators, and the risk rails every signal must pass. See `agents/README.md`.
* **`gateway/`** — everything that talks to the broker: market-data ingestion, order execution, the IBKR connection settings, and the SQLite transaction ledger. See `gateway/README.md`.
* **`clientportal/`** — the vendored IBKR Client Portal Gateway (Java app, gitignored). Managed via `./gateway.sh`.
* **`dashboard.py`** — the control dashboard (see below).
* **`data/`** — recorded ticks (`data/ticks/`), the transaction ledger (`data/transactions.db`), and risk state (gitignored).
* **`tests/`** — pytest suite (`./venv/bin/python -m pytest`).

## Control Dashboard (`dashboard.py`)
* **Role**: Local web UI to control the ingestion and execution agents by hand.
* **Functionality**:
  * Start/stop the ingestion tick stream and watch it live: last price, tick count, a price chart with hover readout, and a recent-ticks table. Streamed ticks are also recorded to `data/ticks/` for the strategy agent.
  * Preview (`/whatif`) and submit limit orders through the execution agent's functions — same safety rails: Submit is disabled while `DRY_RUN=1` (the default), non-paper accounts refused without `ALLOW_LIVE=1`.
  * A Transactions page (`/transactions`) showing everything in the transaction ledger (`data/transactions.db`): every recorded trade with all its columns (filterable by ticker, auto-refreshing) and net positions per ticker.
  * Binds to localhost only (it can place orders). Run with `./venv/bin/python dashboard.py`, then open http://127.0.0.1:8080 (port via `DASHBOARD_PORT`).

## Running

`./gateway.sh` starts the Client Portal Gateway and the dashboard detached (`-dr` for preview-only DRY_RUN mode, `-e` to stop both).
