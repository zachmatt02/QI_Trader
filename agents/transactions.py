#!/usr/bin/env python3
# agents/transactions.py
"""Transaction ledger: one SQLite file (data/transactions.db) that records
every executed trade in a single `transactions` table.

Columns: ticker, isin, share_price, currency, datetime (ISO-8601 UTC),
shares, buy (1 = buy, 0 = sell), plus broker context (order_id, account_id,
conid, commission, status) and a created_at insert timestamp.

The Execution Agent inserts a row for each fill automatically. Run directly
to initialise the database and print the recorded trades and net positions:
  ./agents/transactions.py
"""
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(os.environ.get(
    "TRANSACTIONS_DB",
    Path(__file__).resolve().parent.parent / "data" / "transactions.db"))

BUY, SELL = 1, 0

_SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      VARCHAR(12) NOT NULL,
    isin        VARCHAR(12),
    share_price REAL        NOT NULL CHECK (share_price > 0),
    currency    VARCHAR(3)  NOT NULL DEFAULT 'USD',
    datetime    TEXT        NOT NULL,
    shares      INTEGER     NOT NULL CHECK (shares > 0),
    buy         INTEGER     NOT NULL CHECK (buy IN (0, 1)),
    order_id    TEXT,
    account_id  TEXT,
    conid       INTEGER,
    commission  REAL,
    status      TEXT        NOT NULL DEFAULT 'Filled',
    created_at  TEXT        NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_transactions_ticker   ON transactions (ticker);
CREATE INDEX IF NOT EXISTS idx_transactions_datetime ON transactions (datetime);
"""


def connect(db_path=None):
    """Opens the ledger (creating file and table if needed); caller closes."""
    path = Path(db_path or DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    return conn


def _as_buy_flag(buy):
    if isinstance(buy, str):
        side = buy.upper()
        if side not in ("BUY", "SELL"):
            raise ValueError(f"buy must be BUY/SELL or 1/0, got {buy!r}")
        return BUY if side == "BUY" else SELL
    if buy not in (0, 1, True, False):
        raise ValueError(f"buy must be BUY/SELL or 1/0, got {buy!r}")
    return int(buy)


def _as_utc_iso(when):
    when = when or datetime.now(timezone.utc)
    if isinstance(when, datetime):
        return when.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(when)


def record_transaction(ticker, share_price, shares, buy, *, isin=None,
                       currency="USD", when=None, order_id=None,
                       account_id=None, conid=None, commission=None,
                       status="Filled", db_path=None):
    """Inserts one executed trade and returns its row id.

    `buy` accepts "BUY"/"SELL" or 1/0; `when` accepts a datetime (converted
    to UTC) or an ISO string, defaulting to now."""
    conn = connect(db_path)
    try:
        with conn:  # commits on success
            cur = conn.execute(
                """INSERT INTO transactions
                   (ticker, isin, share_price, currency, datetime, shares,
                    buy, order_id, account_id, conid, commission, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ticker.upper(), isin, float(share_price), currency,
                 _as_utc_iso(when), int(shares), _as_buy_flag(buy),
                 order_id, account_id, conid, commission, status))
        return cur.lastrowid
    finally:
        conn.close()


def list_transactions(ticker=None, limit=100, db_path=None):
    """Returns the most recent trades (newest first) as a list of dicts."""
    conn = connect(db_path)
    try:
        query = "SELECT * FROM transactions"
        params = []
        if ticker:
            query += " WHERE ticker = ?"
            params.append(ticker.upper())
        query += " ORDER BY datetime DESC, id DESC LIMIT ?"
        params.append(limit)
        return [dict(row) for row in conn.execute(query, params)]
    finally:
        conn.close()


def position(ticker, db_path=None):
    """Net shares currently held for `ticker` (buys minus sells)."""
    conn = connect(db_path)
    try:
        row = conn.execute(
            """SELECT COALESCE(SUM(CASE WHEN buy = 1 THEN shares
                                        ELSE -shares END), 0)
               FROM transactions WHERE ticker = ?""",
            (ticker.upper(),)).fetchone()
        return row[0]
    finally:
        conn.close()


def positions(db_path=None):
    """Net shares and trade count per ticker, as a list of dicts."""
    conn = connect(db_path)
    try:
        rows = conn.execute(
            """SELECT ticker,
                      SUM(CASE WHEN buy = 1 THEN shares ELSE -shares END)
                          AS shares,
                      COUNT(*) AS trades
               FROM transactions GROUP BY ticker ORDER BY ticker""").fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def count_transactions(ticker=None, db_path=None):
    """Total number of recorded trades (optionally for one ticker)."""
    conn = connect(db_path)
    try:
        query = "SELECT COUNT(*) FROM transactions"
        params = []
        if ticker:
            query += " WHERE ticker = ?"
            params.append(ticker.upper())
        return conn.execute(query, params).fetchone()[0]
    finally:
        conn.close()


def main():
    print(f"Transaction ledger: {DB_PATH}")
    trades = list_transactions(limit=20)
    if not trades:
        print("No transactions recorded yet.")
        return
    for t in trades:
        side = "BUY " if t["buy"] else "SELL"
        print(f"  {t['datetime']}  {side} {t['shares']:>5} {t['ticker']:<6} "
              f"@ {t['share_price']} {t['currency']}  ({t['status']})")
    print("Net positions: " +
          ", ".join(f"{p['ticker']} {p['shares']:+d}" for p in positions()))


if __name__ == "__main__":
    main()
