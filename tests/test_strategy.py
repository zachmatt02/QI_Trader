import sqlite3

import pytest

from agents.strategy import (_parse_ai_json, _score, build_prompt, connect,
                             news_digest, save_impressions)

IMPRESSIONS = {
    "industries": [
        {"name": "Chip Fabricator", "sentiment": 8, "recent_activity": 9,
         "summary": "Fab capacity sold out through 2027."},
        {"name": "EV Maker", "sentiment": 4, "recent_activity": 3},
    ],
    "companies": [
        {"ticker": "tsm", "name": "TSMC", "industry": "Chip Fabricator",
         "sentiment": 9, "recent_activity": 9, "notes": "Record bookings."},
        {"ticker": "TSLA", "industry": "EV Maker",
         "sentiment": 3, "recent_activity": 4},
    ],
}


def test_save_impressions_creates_linked_rows(tmp_path):
    db = tmp_path / "impressions.db"
    fundamentals = {"TSM": {"name": "Taiwan Semiconductor", "pe_ratio": 28.5,
                            "market_cap": 1.1e12, "share_price": 212.0,
                            "yoy_performance": 41.2}}

    industries, companies = save_impressions(IMPRESSIONS, fundamentals, db)
    assert (industries, companies) == (2, 2)

    conn = connect(db)
    row = conn.execute(
        """SELECT c.ticker, c.name, c.pe_ratio, c.share_price, i.name AS ind
           FROM company c JOIN industry i ON i.id = c.industry_id
           WHERE c.ticker = 'TSM'""").fetchone()
    conn.close()
    assert row["ind"] == "Chip Fabricator"
    assert row["name"] == "Taiwan Semiconductor"  # fundamentals win over AI
    assert row["pe_ratio"] == 28.5
    assert row["share_price"] == 212.0


def test_save_impressions_upserts_instead_of_duplicating(tmp_path):
    db = tmp_path / "impressions.db"
    save_impressions(IMPRESSIONS, db_path=db)

    updated = {
        "industries": [{"name": "Chip Fabricator", "sentiment": 2,
                        "recent_activity": 5}],
        "companies": [{"ticker": "TSM", "industry": "Chip Fabricator",
                       "sentiment": 2, "recent_activity": 5}],
    }
    save_impressions(updated, db_path=db)

    conn = connect(db)
    assert conn.execute("SELECT COUNT(*) FROM industry").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM company").fetchone()[0] == 2
    row = conn.execute(
        "SELECT sentiment, summary FROM industry WHERE name = 'Chip Fabricator'"
    ).fetchone()
    conn.close()
    assert row["sentiment"] == 2
    assert row["summary"] == "Fab capacity sold out through 2027."  # kept


def test_unknown_industry_is_seeded_from_company_scores(tmp_path):
    db = tmp_path / "impressions.db"
    impressions = {"industries": [], "companies": [
        {"ticker": "NVO", "industry": "Weight-Loss Drug Maker",
         "sentiment": 7, "recent_activity": 8}]}
    save_impressions(impressions, db_path=db)

    conn = connect(db)
    row = conn.execute(
        """SELECT i.name, i.sentiment FROM company c
           JOIN industry i ON i.id = c.industry_id
           WHERE c.ticker = 'NVO'""").fetchone()
    conn.close()
    assert row["name"] == "Weight-Loss Drug Maker"
    assert row["sentiment"] == 7


def test_stale_orphan_industries_are_pruned(tmp_path):
    db = tmp_path / "impressions.db"
    save_impressions(IMPRESSIONS, db_path=db)

    renamed = {
        "industries": [{"name": "Semiconductor Foundry", "sentiment": 8,
                        "recent_activity": 9},
                       {"name": "EV Maker", "sentiment": 4,
                        "recent_activity": 3}],
        "companies": [{"ticker": "TSM", "industry": "Semiconductor Foundry",
                       "sentiment": 8, "recent_activity": 9},
                      {"ticker": "TSLA", "industry": "EV Maker",
                       "sentiment": 3, "recent_activity": 4}],
    }
    save_impressions(renamed, db_path=db)

    conn = connect(db)
    names = {r[0] for r in conn.execute("SELECT name FROM industry")}
    conn.close()
    # "Chip Fabricator" lost its only company and wasn't re-mentioned
    assert names == {"Semiconductor Foundry", "EV Maker"}


def test_prompt_lists_known_industries_for_reuse():
    prompt = build_prompt([], ["Chip Fabricator", "EV Maker"])
    assert "Chip Fabricator, EV Maker" in prompt
    assert "(none yet)" in build_prompt([])


def test_scores_are_clamped_to_scale():
    assert _score(0) == 1
    assert _score(11) == 10
    assert _score(7) == 7


def test_schema_rejects_out_of_range_scores(tmp_path):
    conn = connect(tmp_path / "impressions.db")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO industry (name, sentiment, recent_activity)"
                     " VALUES ('X', 11, 5)")
    conn.close()


def test_news_digest_compacts_articles():
    articles = [{"title": "TSMC beats estimates", "tickers": ["TSM"],
                 "description": "Record quarter on AI demand.",
                 "insights": [{"ticker": "TSM", "sentiment": "positive"}]}]
    digest = news_digest(articles)
    assert "[TSM] TSMC beats estimates" in digest
    assert "Record quarter" in digest
    assert "TSM:positive" in digest


def test_parse_ai_json_unwraps_candidates():
    reply = {"candidates": [{"content": {"parts": [
        {"text": '{"industries": [], "companies": []}'}]}}]}
    assert _parse_ai_json(reply) == {"industries": [], "companies": []}

    with pytest.raises(RuntimeError):
        _parse_ai_json({"error": {"message": "quota"}})
