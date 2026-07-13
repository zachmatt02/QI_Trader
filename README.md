# QI Trader

An AI-driven trading pipeline for Interactive Brokers. Once an hour it reads
the day's market news, has an LLM build an impression of the market, has the
LLM judge that impression against current holdings, and places the resulting
limit orders through the IBKR Client Portal Gateway — with guardrails at
every step and paper trading as the default.

Everything runs locally: two SQLite files, NDJSON tick files, no external
services beyond the two APIs (Massive for market data, Gemini for the model)
and the broker gateway.

## How it works

```
                        ┌─────────────────────────── every RUN_INTERVAL (1h) ──┐
                        │                                                      │
  Massive API ──news──► │  agents/strategy.py                                  │
  (fundamentals)──────► │    LLM scores news into industry/company impressions │
                        │    └─► data/impressions.db (industry, company)       │
                        │                                                      │
  holdings ───────────► │  agents/decision.py                                  │
  (transactions.db)     │    LLM judges impressions + holdings                 │
                        │    └─► decision table: BUY/SELL/HOLD + confidence    │
                        │                                                      │
                        │  main.py guardrails                                  │
                        │    confidence ≥ MIN_CONFIDENCE, qty ≤ MAX_QTY,       │
                        │    ≤ MAX_ORDERS_PER_CYCLE, SELL clipped to held      │
                        │                                                      │
                        │  gateway/execution.py                                │
                        │    preview (/whatif) ─► place ─► poll until filled   │
                        │    └─► data/transactions.db (ledger of every fill)   │
                        └──────────────────────────────────────────────────────┘

  IBKR gateway ─ticks─► gateway/ingestion.py ─► data/ticks/*.ndjson   (standalone)
  dashboard.py: web UI to stream ticks, preview/submit orders, browse the ledger
```

Each `main.py` cycle in order:

1. **Reconcile** (`gateway/execution.py reconcile_fills`) — backfills the
   transaction ledger with any fills that landed while nothing was watching
   (after the 60s post-order poll, or while the process was down).
2. **Strategy** (`agents/strategy.py`) — fetches the last 24h of news from
   Massive, prompts the LLM to group it into *niche* industries ("Chip
   Fabricator", never "Tech") and score each industry and mentioned company
   on `sentiment` and `recent_activity` (1–10). Enriches each ticker with
   fundamentals from Massive (P/E, market cap, price, Y/Y %) and upserts
   everything into `data/impressions.db` — one current row per
   industry/ticker across runs.
3. **Decision** (`agents/decision.py`) — sends the stored impressions plus
   current net positions to the LLM and asks for BUY/SELL/HOLD calls with a
   confidence (1–10), limit price, quantity and reasoning. Every call is
   appended to a `decision` audit table; nothing is traded here.
4. **Execution** (`gateway/execution.py`) — for each decision that clears
   the guardrails, previews the limit order via the gateway's `/whatif`
   endpoint, submits it (answering the gateway's confirmation prompts),
   polls until it reaches a terminal status, and records the fill in the
   ledger. Executed decisions are flagged in the audit table.

The tick stream (`gateway/ingestion.py`) is a separate, optional agent: it
records live trades to `data/ticks/<ticker>-<date>.ndjson` for later use and
feeds the dashboard's live chart. The hourly loop does not depend on it.

## Safety rails

- **`DRY_RUN=1` is the code default** — orders are previewed, never
  submitted. Note that `./gateway.sh` launches with `DRY_RUN=0` (live
  submission on the paper account) unless you pass `-dr`.
- **Paper account only** — execution refuses any account id not starting
  with `DU` (IBKR paper prefix) unless `ALLOW_LIVE=1` is set explicitly.
- **Per-cycle guardrails** in `main.py` — HOLDs never trade; BUY/SELL needs
  confidence ≥ `MIN_CONFIDENCE` (default 7/10) and a limit price; quantity
  capped at `MAX_QTY` (default 10); at most `MAX_ORDERS_PER_CYCLE`
  (default 3) orders per cycle; SELLs clipped to shares actually held.
- **Fat-finger ceiling** — `build_order` rejects any order over
  `MAX_ORDER_QTY` (100 shares), even when called directly.
- **Audit trail** — every decision (with reasoning) and every fill is stored
  in SQLite; re-running reconciliation never duplicates a fill (order ids
  are checked against the ledger).

## Repository layout

| Path | What it is |
|---|---|
| `main.py` | The hourly pipeline loop (`./main.py once` for a single cycle). |
| `agents/ai.py` | Single entry point for every LLM call. Provider chosen by `AI_PROVIDER` (default `gemini`); OpenAI/Anthropic/Ollama backends exist but are untested. |
| `agents/strategy.py` | News → impressions. Owns `data/impressions.db`. |
| `agents/decision.py` | Impressions + holdings → BUY/SELL/HOLD audit rows. |
| `gateway/ib_gateway.py` | Shared gateway settings (`IB_GATEWAY_URL`, default ticker) and the self-signed-cert `ssl_context()`. |
| `gateway/ingestion.py` | Live tick stream over the gateway websocket → `data/ticks/*.ndjson` (batched writes, torn-line-safe reads). |
| `gateway/execution.py` | Order preview/placement/tracking + fill reconciliation. Runnable directly for one-off orders. |
| `gateway/transactions.py` | The SQLite trade ledger: `record_transaction()`, `positions()`, etc. |
| `dashboard.py` | Localhost web UI: live tick chart, manual order form (same rails), transactions browser. |
| `gateway.sh` | Starts/stops the Java gateway + dashboard + main loop, detached with logs in `logs/`. |
| `clientportal/` | Vendored IBKR Client Portal Gateway (Java, gitignored). |
| `data/` | `impressions.db`, `transactions.db`, `ticks/` (gitignored). |
| `tests/` | Pytest suite — 38 tests, all offline (APIs mocked). |

## Setup

1. **Python** ≥ 3.12:

   ```sh
   python3 -m venv venv
   ./venv/bin/pip install -r requirements.txt
   ```

2. **IBKR Client Portal Gateway** — download from IBKR and unpack into
   `clientportal/` so that `clientportal/bin/run.sh` exists. `root/conf.yaml`
   should listen on port 5001 with SSL (the code's default gateway URL is
   `https://localhost:5001/v1/api`).

3. **API keys** — create `.env` in the repo root:

   ```
   AIKEY=<Gemini API key>
   MassiveKey=<Massive API key>
   ```

4. **Log in once per gateway start** — the gateway requires a browser login
   at https://localhost:5001 (self-signed certificate warning is expected)
   before any agent can use it. Use an IBKR **paper** account.

## Running

```sh
./gateway.sh          # start gateway + dashboard + hourly loop (orders ON, paper account)
./gateway.sh -dr      # same, but DRY_RUN: orders previewed only
./gateway.sh -e       # stop all three
```

Logs land in `logs/` (`gateway.log`, `dashboard.log`, `main.log`).
Dashboard: http://127.0.0.1:8080 (port via `DASHBOARD_PORT`).

Every piece also runs standalone, which is the easiest way to test:

```sh
./main.py once                     # one full cycle, no loop
./agents/strategy.py               # score today's news, print the impressions
./agents/decision.py               # decide once from stored impressions, print
./gateway/ingestion.py             # stream + record live ticks (Ctrl-C stops)
./gateway/transactions.py          # print recorded trades and net positions
./gateway/execution.py reconcile   # backfill missed fills into the ledger
DRY_RUN=0 SIDE=BUY QTY=1 LIMIT_PRICE=420 TICKER=TSLA ./gateway/execution.py   # one-off order
```

## Configuration

All configuration is environment variables (the `.env` file is loaded for
keys; everything else can be set inline). Defaults in parentheses.

**Pipeline (`main.py`)**
- `RUN_INTERVAL` (3600) — seconds between cycles.
- `MIN_CONFIDENCE` (7) — minimum decision confidence to trade.
- `MAX_ORDERS_PER_CYCLE` (3), `MAX_QTY` (10) — per-cycle order caps.

**Execution (`gateway/execution.py`)**
- `DRY_RUN` (1) — preview only; set `0` to submit orders.
- `ALLOW_LIVE` (unset) — set `1` to permit a non-paper account. Don't.
- `SIDE`, `QTY`, `LIMIT_PRICE`, `TICKER` — for one-off direct runs only.

**Gateway (`gateway/ib_gateway.py`)**
- `IB_GATEWAY_URL` (`https://localhost:5001/v1/api`) — REST base; the
  websocket URL is derived from it.
- `TICKER` (`TSLA`) — default symbol for ingestion and direct execution runs.

**Strategy (`agents/strategy.py`)**
- `MassiveKey` — Massive API key (required).
- `MASSIVE_BASE_URL` (`https://api.massive.com`), `NEWS_LIMIT` (80),
  `MAX_COMPANIES` (15).
- `MASSIVE_RPM` (5) — Massive rate limit; the free tier is 5/min, set `0`
  for unlimited paid plans. Fundamentals for 15 tickers take ~12 min at 5.
- `IMPRESSIONS_DB` (`data/impressions.db`).

**AI provider (`agents/ai.py`)**
- `AI_PROVIDER` (`gemini`) — one of `gemini`, `openai`, `anthropic`,
  `ollama`. Only the gemini path is exercised by the running pipeline.
- `AIKEY` — Gemini key (required for the default provider);
  `GEMINI_MODEL` (`gemini-2.5-flash`).
- `OPENAI_API_KEY`, `OPENAI_MODEL` (`gpt-4o-mini`), `OPENAI_BASE_URL`
  (api.openai.com — point it at LM Studio/vLLM/llama.cpp for local models).
- `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL` (`claude-opus-4-8`).
- `OLLAMA_BASE_URL` (`http://localhost:11434`), `LOCAL_MODEL` (`llama3.1`).
- `AI_TIMEOUT` (180) — seconds; raise for slow local models.

**Ledger & dashboard**
- `TRANSACTIONS_DB` (`data/transactions.db`).
- `DASHBOARD_PORT` (8080) — dashboard binds to localhost only.

## Data

- **`data/impressions.db`** — `industry` (name, sentiment, recent_activity,
  summary), `company` (ticker, industry FK, scores, fundamentals, notes),
  `decision` (ticker, action, confidence, limit price, quantity, reasoning,
  executed flag). Impressions upsert in place; decisions append forever.
- **`data/transactions.db`** — one `transactions` table: ticker, price,
  currency, UTC datetime, shares, buy/sell flag, plus broker context
  (order id, account, conid, commission, status). Net positions are computed
  from it (buys minus sells).
- **`data/ticks/<ticker>-<date>.ndjson`** — one line per trade tick
  (timestamp, ticker, price, volume), appended in whole-line batches so a
  concurrent reader never sees a torn record.

## Tests

```sh
./venv/bin/python -m pytest
```

38 tests covering the ledger, order building/placement/reconciliation, the
strategy and decision agents (LLM and Massive calls mocked), the tick store,
the main-loop guardrails, and the dashboard endpoints. Everything runs
offline.

See `agents/README.md` and `gateway/README.md` for per-module detail.
