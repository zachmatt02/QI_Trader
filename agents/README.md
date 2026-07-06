# Agents

AI side of QI Trader. Everything that talks to a model lives here;
broker/market plumbing lives in `gateway/`.

## strategy.py

Daily news impressions. Pulls the last 24h of market news from the Massive
API (`MassiveKey` in `.env`), sends a digest to Gemini (`AIKEY` in `.env`),
and stores the model's read of the market in `data/impressions.db`:

- **industry** — niche industry groups (e.g. "Chip Fabricator", not "Tech")
  with `sentiment` and `recent_activity`, both 1-10 (1 = doing poorly,
  10 = doing amazing).
- **company** — individual tickers linked to an industry (`industry_id`),
  same two scores, plus fundamentals from Massive: P/E ratio, market cap,
  current share price and Y/Y performance.

Re-running upserts in place, so the tables always hold the current
impression per industry/ticker.

```sh
./agents/strategy.py
```

## decision.py

Decision Agent. Reads the impressions plus current holdings (transaction
ledger), sends the snapshot to Gemini and asks for its own judgement:
possible BUYs, SELLs of held tickers, or HOLD. Decisions land in a
`decision` audit table in the same DB (ticker, action, confidence 1-10,
limit price, quantity, reasoning, executed flag). Nothing is traded here.

```sh
./agents/decision.py
```

## Putting it together

`main.py` at the project root is the running entry point: an hourly loop
of strategy → decision → `gateway/execution.py` for the decisions that
clear its guardrails (confidence, order cap, quantity cap, DRY_RUN).

```sh
./main.py
```
