from unittest.mock import AsyncMock, patch

import pytest

from agents.decision import (decide, load_snapshot, mark_executed,
                             save_decisions, snapshot_digest, _connect)
from agents.strategy import save_impressions
from gateway.transactions import record_transaction

IMPRESSIONS = {
    "industries": [{"name": "Chip Fabricator", "sentiment": 8,
                    "recent_activity": 9, "summary": "Capacity sold out."}],
    "companies": [{"ticker": "TSM", "industry": "Chip Fabricator",
                   "sentiment": 9, "recent_activity": 9}],
}

VERDICT = {
    "market_view": "Semis look strong.",
    "decisions": [
        {"ticker": "tsm", "action": "buy", "confidence": 8,
         "limit_price": 212.5, "quantity": 2, "reasoning": "Strong niche."},
        {"ticker": "JPM", "action": "HOLD", "confidence": 5,
         "reasoning": "Mixed news."},
    ],
}


@pytest.fixture
def dbs(tmp_path):
    impressions_db = tmp_path / "impressions.db"
    ledger_db = tmp_path / "transactions.db"
    fundamentals = {"TSM": {"name": "TSMC", "share_price": 212.0,
                            "pe_ratio": 28.5, "market_cap": 1.1e12,
                            "yoy_performance": 41.2}}
    save_impressions(IMPRESSIONS, fundamentals, impressions_db)
    record_transaction("TSM", 180.0, 3, "BUY", db_path=ledger_db)
    return impressions_db, ledger_db


def test_snapshot_digest_shows_scores_prices_and_holdings(dbs):
    impressions_db, ledger_db = dbs
    digest = snapshot_digest(load_snapshot(impressions_db, ledger_db))
    assert "Chip Fabricator: sentiment 8, activity 9" in digest
    assert "TSM (TSMC, Chip Fabricator)" in digest
    assert "price 212.00" in digest
    assert "P/E 28.5" in digest
    assert "TSM: +3 shares" in digest


def test_snapshot_digest_handles_missing_fundamentals(tmp_path):
    save_impressions(IMPRESSIONS, db_path=tmp_path / "i.db")
    digest = snapshot_digest(load_snapshot(tmp_path / "i.db",
                                           tmp_path / "t.db"))
    assert "price ?" in digest
    assert "  none" in digest  # no holdings


def test_save_decisions_appends_with_ids_and_normalises(dbs):
    impressions_db, _ = dbs
    saved = save_decisions(VERDICT, impressions_db)
    assert [d["ticker"] for d in saved] == ["TSM", "JPM"]
    assert saved[0]["action"] == "BUY"
    assert all(d["id"] for d in saved)

    save_decisions(VERDICT, impressions_db)  # appends, never overwrites
    conn = _connect(impressions_db)
    assert conn.execute("SELECT COUNT(*) FROM decision").fetchone()[0] == 4
    conn.close()


def test_mark_executed_flips_flag(dbs):
    impressions_db, _ = dbs
    saved = save_decisions(VERDICT, impressions_db)
    mark_executed(saved[0]["id"], impressions_db)

    conn = _connect(impressions_db)
    rows = dict(conn.execute("SELECT id, executed FROM decision").fetchall())
    conn.close()
    assert rows[saved[0]["id"]] == 1
    assert rows[saved[1]["id"]] == 0


@pytest.mark.asyncio
async def test_decide_saves_gemini_verdict(dbs):
    impressions_db, ledger_db = dbs
    with patch("agents.decision.call_gemini",
               new_callable=AsyncMock) as gemini:
        gemini.return_value = VERDICT
        decisions = await decide(db_path=impressions_db,
                                 ledger_path=ledger_db)
    prompt = gemini.await_args.args[1]
    assert "TSM (TSMC, Chip Fabricator)" in prompt  # judged from the DB
    assert [d["action"] for d in decisions] == ["BUY", "HOLD"]


@pytest.mark.asyncio
async def test_decide_without_impressions_returns_nothing(tmp_path):
    decisions = await decide(db_path=tmp_path / "empty.db",
                             ledger_path=tmp_path / "t.db")
    assert decisions == []
