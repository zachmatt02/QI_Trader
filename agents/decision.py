#!/usr/bin/env python3
# agents/decision.py
"""Decision Agent: turns stored impressions into trade decisions.

Reads the industry/company impressions that strategy.py stored in
data/impressions.db plus the current net positions from the transaction
ledger, sends that snapshot to Gemini (`AIKEY` in .env) and asks it to
judge whether anything is worth buying (or selling out of). Every decision
is appended to a `decision` table in the same database as an audit trail:
ticker, BUY/SELL/HOLD, confidence 1-10, suggested limit price and quantity,
and the model's reasoning. main.py acts on them and flips `executed`.

Run directly to decide once and print the result (nothing is traded here):
  ./agents/decision.py
"""
import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agents.strategy import (GEMINI_MODEL, _parse_ai_json, _require_key,
                             connect)
from gateway import transactions

MIN_CONFIDENCE = int(os.environ.get("MIN_CONFIDENCE", "7"))

_DECISION_SCHEMA = """
CREATE TABLE IF NOT EXISTS decision (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      VARCHAR(12) NOT NULL,
    action      TEXT    NOT NULL CHECK (action IN ('BUY', 'SELL', 'HOLD')),
    confidence  INTEGER NOT NULL CHECK (confidence BETWEEN 1 AND 10),
    limit_price REAL,
    quantity    INTEGER,
    reasoning   TEXT,
    executed    INTEGER NOT NULL DEFAULT 0 CHECK (executed IN (0, 1)),
    created_at  TEXT    NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_decision_created ON decision (created_at);
"""


def _connect(db_path=None):
    conn = connect(db_path)
    conn.executescript(_DECISION_SCHEMA)
    return conn


def load_snapshot(db_path=None, ledger_path=None):
    """Returns industries, companies and net positions for the prompt."""
    conn = _connect(db_path)
    try:
        industries = [dict(r) for r in conn.execute(
            "SELECT * FROM industry ORDER BY sentiment DESC")]
        companies = [dict(r) for r in conn.execute(
            """SELECT c.*, i.name AS industry FROM company c
               JOIN industry i ON i.id = c.industry_id
               ORDER BY c.sentiment DESC""")]
    finally:
        conn.close()
    positions = [p for p in transactions.positions(ledger_path)
                 if p["shares"]]
    return {"industries": industries, "companies": companies,
            "positions": positions}


def snapshot_digest(snapshot):
    """Compacts the DB snapshot into the text block Gemini judges from."""
    def fmt(value, spec=""):
        return format(value, spec) if value is not None else "?"

    lines = ["Industries (sentiment/activity are 1-10, 10 = doing amazing):"]
    for ind in snapshot["industries"]:
        lines.append(f"  {ind['name']}: sentiment {ind['sentiment']}, "
                     f"activity {ind['recent_activity']}"
                     + (f" -- {ind['summary']}" if ind.get("summary") else ""))
    lines.append("Companies:")
    for c in snapshot["companies"]:
        lines.append(
            f"  {c['ticker']} ({c.get('name') or '?'}, {c['industry']}): "
            f"sentiment {c['sentiment']}, activity {c['recent_activity']}, "
            f"price {fmt(c['share_price'], '.2f')}, "
            f"P/E {fmt(c['pe_ratio'], '.1f')}, "
            f"mktcap {fmt(c['market_cap'], '.3g')}, "
            f"Y/Y {fmt(c['yoy_performance'], '+.1f')}%"
            + (f" -- {c['notes']}" if c.get("notes") else ""))
    lines.append("Current holdings (net shares):")
    if snapshot["positions"]:
        for p in snapshot["positions"]:
            lines.append(f"  {p['ticker']}: {p['shares']:+d} shares")
    else:
        lines.append("  none")
    return "\n".join(lines)


_RESPONSE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "market_view": {"type": "STRING"},
        "decisions": {"type": "ARRAY", "items": {
            "type": "OBJECT",
            "properties": {
                "ticker": {"type": "STRING"},
                "action": {"type": "STRING", "enum": ["BUY", "SELL", "HOLD"]},
                "confidence": {"type": "INTEGER"},
                "limit_price": {"type": "NUMBER"},
                "quantity": {"type": "INTEGER"},
                "reasoning": {"type": "STRING"},
            },
            "required": ["ticker", "action", "confidence", "reasoning"],
        }},
    },
    "required": ["market_view", "decisions"],
}

_PROMPT = """\
You are the portfolio manager of a small, cautious trading system. Below is
your analyst's current impression of the market (built from today's news)
and the portfolio's current holdings. Use your own judgement to decide
whether there are any possible buys, and whether any current holding should
be sold.

Rules:
- Be selective: only propose BUY or SELL when the evidence is genuinely
  strong; it is fine to return only HOLDs or an empty list.
- confidence is 1-10; anything below {min_confidence} will not be executed.
- For BUY/SELL, set limit_price close to the listed share price (never more
  than a few percent away) and a small quantity (1-10 shares). If a
  company's share price is unknown, do not propose an order for it.
- Only SELL tickers that appear in current holdings, and never more shares
  than are held.
- reasoning must say in one or two sentences why, based on the data above.

{digest}
"""


async def call_gemini(session, prompt):
    """Sends the snapshot prompt to Gemini and returns its JSON verdict."""
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent")
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": _RESPONSE_SCHEMA,
        },
    }
    headers = {"x-goog-api-key": _require_key("AIKEY")}
    async with session.post(url, json=payload, headers=headers,
                            timeout=aiohttp.ClientTimeout(total=180)) as resp:
        resp.raise_for_status()
        return _parse_ai_json(await resp.json())


def save_decisions(verdict, db_path=None):
    """Appends the verdict's decisions to the audit table and returns them
    as dicts that include their new row `id`."""
    saved = []
    conn = _connect(db_path)
    try:
        with conn:
            for d in verdict.get("decisions", []):
                cur = conn.execute(
                    """INSERT INTO decision (ticker, action, confidence,
                                             limit_price, quantity, reasoning)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (d["ticker"].strip().upper(), d["action"].upper(),
                     max(1, min(10, int(d["confidence"]))),
                     d.get("limit_price"), d.get("quantity"),
                     d.get("reasoning")))
                saved.append({**d, "id": cur.lastrowid,
                              "ticker": d["ticker"].strip().upper(),
                              "action": d["action"].upper()})
    finally:
        conn.close()
    return saved


def mark_executed(decision_id, db_path=None):
    conn = _connect(db_path)
    try:
        with conn:
            conn.execute("UPDATE decision SET executed = 1 WHERE id = ?",
                         (decision_id,))
    finally:
        conn.close()


async def decide(session=None, db_path=None, ledger_path=None):
    """Judges the stored impressions and returns the saved decision dicts."""
    snapshot = load_snapshot(db_path, ledger_path)
    if not snapshot["companies"]:
        print("No impressions stored yet; run agents/strategy.py first.")
        return []
    prompt = _PROMPT.format(min_confidence=MIN_CONFIDENCE,
                            digest=snapshot_digest(snapshot))
    own_session = session is None
    session = session or aiohttp.ClientSession()
    try:
        verdict = await call_gemini(session, prompt)
    finally:
        if own_session:
            await session.close()
    if verdict.get("market_view"):
        print(f"Market view: {verdict['market_view']}")
    return save_decisions(verdict, db_path)


def main():
    decisions = asyncio.run(decide())
    if not decisions:
        print("No decisions.")
        return
    for d in decisions:
        price = f" @ {d['limit_price']}" if d.get("limit_price") else ""
        qty = f" x{d['quantity']}" if d.get("quantity") else ""
        print(f"  {d['action']:<4} {d['ticker']:<6}{qty}{price}  "
              f"confidence {d['confidence']}/10 -- {d.get('reasoning', '')}")


if __name__ == "__main__":
    main()
