#!/usr/bin/env python3
# agents/strategy.py
"""Daily news impressions: fetches the day's market news from the Massive
API, has Gemini score it, and stores the result in one SQLite file
(data/impressions.db) with two tables.

`industry` holds niche industry groups (e.g. "Chip Fabricator", "AI Model
Developer" -- never broad buckets like "Tech") with a `sentiment` and a
`recent_activity` score, both 1-10 where 1 = doing poorly and 10 = doing
amazing. `company` holds the individual tickers the news mentioned, linked
to their industry via `industry_id`, with the same two scores plus
fundamentals pulled from Massive: P/E ratio, market cap, current share
price and Y/Y performance (percent).

Keys are read from the project .env: `MassiveKey` (Massive API) and
`AIKEY` (Gemini). Massive calls are spaced to MASSIVE_RPM per minute
(default 5, the free tier; set 0 for unlimited paid plans), and any
fundamental that still fails just stores NULL. Re-running upserts, so each
table keeps one current row per industry/ticker and backfills missing
fundamentals.

Run directly to score today's news and print the stored impressions:
  ./agents/strategy.py
"""
import asyncio
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

# Every LLM call goes through agents/ai.py; _parse_ai_json is re-exported here
# because the tests (and decision.py, historically) import it from strategy.
# Importing agents.ai also loads the project .env as a side effect.
from agents.ai import (_parse_ai_json, _require_key, active_label, generate_json)

DB_PATH = Path(os.environ.get("IMPRESSIONS_DB", _ROOT / "data" / "impressions.db"))
MASSIVE_BASE = os.environ.get("MASSIVE_BASE_URL", "https://api.massive.com")
NEWS_LIMIT = int(os.environ.get("NEWS_LIMIT", "80"))
MAX_COMPANIES = int(os.environ.get("MAX_COMPANIES", "15"))
MASSIVE_RPM = int(os.environ.get("MASSIVE_RPM", "5"))  # 0 = unlimited plan

_SCHEMA = """
CREATE TABLE IF NOT EXISTS industry (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL UNIQUE,
    sentiment       INTEGER NOT NULL CHECK (sentiment BETWEEN 1 AND 10),
    recent_activity INTEGER NOT NULL CHECK (recent_activity BETWEEN 1 AND 10),
    summary         TEXT,
    updated_at      TEXT    NOT NULL
                    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE TABLE IF NOT EXISTS company (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          VARCHAR(12) NOT NULL UNIQUE,
    name            TEXT,
    industry_id     INTEGER NOT NULL REFERENCES industry (id),
    sentiment       INTEGER NOT NULL CHECK (sentiment BETWEEN 1 AND 10),
    recent_activity INTEGER NOT NULL CHECK (recent_activity BETWEEN 1 AND 10),
    pe_ratio        REAL,
    market_cap      REAL,
    share_price     REAL,
    yoy_performance REAL,
    notes           TEXT,
    updated_at      TEXT    NOT NULL
                    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_company_industry ON company (industry_id);
"""


def connect(db_path=None):
    """Opens the impressions DB (creating file and tables); caller closes."""
    path = Path(db_path or DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_SCHEMA)
    return conn


# --------------------------------------------------------------------------
# Massive API: daily news + per-ticker fundamentals
# --------------------------------------------------------------------------

_throttle_lock = asyncio.Lock()
_next_call_at = 0.0


async def _throttle():
    """Spaces Massive calls to MASSIVE_RPM per minute across all tasks."""
    global _next_call_at
    if MASSIVE_RPM <= 0:
        return
    async with _throttle_lock:
        now = asyncio.get_running_loop().time()
        wait = _next_call_at - now
        _next_call_at = max(now, _next_call_at) + 60.0 / MASSIVE_RPM
        if wait > 0:
            await asyncio.sleep(wait)


async def _massive_get(session, path, **params):
    headers = {"Authorization": f"Bearer {_require_key('MassiveKey')}"}
    for attempt in (1, 2):
        await _throttle()
        async with session.get(f"{MASSIVE_BASE}{path}", params=params,
                               headers=headers,
                               timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 429 and attempt == 1:
                await asyncio.sleep(30)
                continue
            resp.raise_for_status()
            return await resp.json()


async def fetch_daily_news(session, limit=NEWS_LIMIT):
    """Returns the last 24h of news articles (newest first) from Massive."""
    since = (datetime.now(timezone.utc) - timedelta(days=1)
             ).strftime("%Y-%m-%dT%H:%M:%SZ")
    data = await _massive_get(session, "/v2/reference/news",
                              **{"published_utc.gte": since, "limit": limit,
                                 "order": "desc", "sort": "published_utc"})
    return data.get("results", [])


async def _fundamental(coro):
    """Awaits one fundamentals call, mapping any failure to None."""
    try:
        return await coro
    except Exception:
        return None


async def fetch_fundamentals(session, ticker):
    """Best-effort fundamentals for one ticker; missing pieces stay None."""
    today = datetime.now(timezone.utc).date()
    year_ago = today - timedelta(days=365)

    async def details():
        data = await _massive_get(session, f"/v3/reference/tickers/{ticker}")
        return data.get("results", {})

    async def prev_close():
        data = await _massive_get(session, f"/v2/aggs/ticker/{ticker}/prev")
        return data["results"][0]["c"]

    async def close_year_ago():
        data = await _massive_get(
            session,
            f"/v2/aggs/ticker/{ticker}/range/1/day"
            f"/{year_ago - timedelta(days=7)}/{year_ago}",
            sort="desc", limit=1)
        return data["results"][0]["c"]

    async def eps_ttm():
        data = await _massive_get(session, "/vX/reference/financials",
                                  ticker=ticker, timeframe="ttm", limit=1)
        income = data["results"][0]["financials"]["income_statement"]
        return income["diluted_earnings_per_share"]["value"]

    info, price, old_price, eps = await asyncio.gather(
        _fundamental(details()), _fundamental(prev_close()),
        _fundamental(close_year_ago()), _fundamental(eps_ttm()))

    return {
        "name": (info or {}).get("name"),
        "market_cap": (info or {}).get("market_cap"),
        "share_price": price,
        "yoy_performance": (round((price / old_price - 1) * 100, 2)
                            if price and old_price else None),
        "pe_ratio": (round(price / eps, 2) if price and eps and eps > 0
                     else None),
    }


# --------------------------------------------------------------------------
# Gemini: turn the news digest into industry/company impressions
# --------------------------------------------------------------------------

_RESPONSE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "industries": {"type": "ARRAY", "items": {
            "type": "OBJECT",
            "properties": {
                "name": {"type": "STRING"},
                "sentiment": {"type": "INTEGER"},
                "recent_activity": {"type": "INTEGER"},
                "summary": {"type": "STRING"},
            },
            "required": ["name", "sentiment", "recent_activity"],
        }},
        "companies": {"type": "ARRAY", "items": {
            "type": "OBJECT",
            "properties": {
                "ticker": {"type": "STRING"},
                "name": {"type": "STRING"},
                "industry": {"type": "STRING"},
                "sentiment": {"type": "INTEGER"},
                "recent_activity": {"type": "INTEGER"},
                "notes": {"type": "STRING"},
            },
            "required": ["ticker", "industry", "sentiment", "recent_activity"],
        }},
    },
    "required": ["industries", "companies"],
}

_PROMPT = """\
You are the research analyst for a trading system. Below is a digest of the
last 24 hours of market news. Build an impression of the market from it.

Rules:
- Group what you see into NICHE industries. Never use broad buckets like
  "Tech", "AI" or "Healthcare"; use specific ones like "Chip Fabricator",
  "Chip Designer", "AI Model Developer", "Weight-Loss Drug Maker".
- These industry names already exist from earlier runs; reuse one VERBATIM
  whenever it fits instead of inventing a near-duplicate: {known}
- Score every industry and company on two 1-10 scales:
  sentiment (tone of the news: 1 = very negative, 10 = very positive) and
  recent_activity (how it has been doing lately: 1 = doing poorly,
  10 = doing amazing).
- Only include industries and companies the digest gives real evidence for.
- Each company's `industry` must exactly match one of your industry names,
  and its `ticker` must be the exchange ticker from the digest.
- Return at most {max_companies} companies (the most newsworthy ones).

News digest:
{digest}
"""


def news_digest(articles):
    """Compacts articles into one text block small enough to prompt with."""
    lines = []
    for art in articles:
        tickers = ",".join(art.get("tickers", [])[:8]) or "-"
        desc = (art.get("description") or "")[:220]
        insights = "; ".join(
            f"{i.get('ticker')}:{i.get('sentiment')}"
            for i in art.get("insights", []) if i.get("sentiment"))
        line = f"[{tickers}] {art.get('title', '')} | {desc}"
        if insights:
            line += f" | insights: {insights}"
        lines.append(line)
    return "\n".join(lines)


def build_prompt(articles, known_industries=()):
    return _PROMPT.format(
        max_companies=MAX_COMPANIES,
        known=", ".join(known_industries) or "(none yet)",
        digest=news_digest(articles))


async def score_news(session, articles, known_industries=()):
    """Sends the news digest to the AI provider and returns its impressions
    dict; the provider and its request shape live in agents/ai.py."""
    return await generate_json(session, build_prompt(articles,
                                                     known_industries),
                               _RESPONSE_SCHEMA)


# --------------------------------------------------------------------------
# Persistence
# --------------------------------------------------------------------------

def _score(value):
    """Clamps a model-provided score onto the 1-10 scale."""
    return max(1, min(10, int(value)))


def save_impressions(impressions, fundamentals=None, db_path=None):
    """Upserts industries then companies; returns (industries, companies).

    `impressions` is the dict Gemini returns; `fundamentals` maps ticker ->
    fetch_fundamentals() dict. A company naming an unknown industry gets an
    industry row seeded from its own scores, so the FK always resolves."""
    fundamentals = fundamentals or {}
    conn = connect(db_path)
    try:
        with conn:
            industry_ids = {}

            def upsert_industry(name, sentiment, activity, summary=None):
                conn.execute(
                    """INSERT INTO industry
                       (name, sentiment, recent_activity, summary)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT (name) DO UPDATE SET
                         sentiment = excluded.sentiment,
                         recent_activity = excluded.recent_activity,
                         summary = COALESCE(excluded.summary, summary),
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')""",
                    (name, sentiment, activity, summary))
                industry_ids[name] = conn.execute(
                    "SELECT id FROM industry WHERE name = ?",
                    (name,)).fetchone()[0]

            for ind in impressions.get("industries", []):
                upsert_industry(ind["name"].strip(), _score(ind["sentiment"]),
                                _score(ind["recent_activity"]),
                                ind.get("summary"))

            companies = 0
            for comp in impressions.get("companies", []):
                ticker = comp["ticker"].strip().upper()
                industry = comp["industry"].strip()
                if industry not in industry_ids:
                    upsert_industry(industry, _score(comp["sentiment"]),
                                    _score(comp["recent_activity"]))
                facts = fundamentals.get(ticker, {})
                conn.execute(
                    """INSERT INTO company
                       (ticker, name, industry_id, sentiment, recent_activity,
                        pe_ratio, market_cap, share_price, yoy_performance,
                        notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (ticker) DO UPDATE SET
                         name = COALESCE(excluded.name, name),
                         industry_id = excluded.industry_id,
                         sentiment = excluded.sentiment,
                         recent_activity = excluded.recent_activity,
                         pe_ratio = COALESCE(excluded.pe_ratio, pe_ratio),
                         market_cap = COALESCE(excluded.market_cap, market_cap),
                         share_price = COALESCE(excluded.share_price, share_price),
                         yoy_performance = COALESCE(excluded.yoy_performance,
                                                    yoy_performance),
                         notes = COALESCE(excluded.notes, notes),
                         updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')""",
                    (ticker, facts.get("name") or comp.get("name"),
                     industry_ids[industry], _score(comp["sentiment"]),
                     _score(comp["recent_activity"]), facts.get("pe_ratio"),
                     facts.get("market_cap"), facts.get("share_price"),
                     facts.get("yoy_performance"), comp.get("notes")))
                companies += 1

            # Industries from earlier runs that this run didn't mention and
            # no company references any more are stale (usually renamed
            # near-duplicates) -- drop them.
            if industry_ids:
                marks = ",".join("?" * len(industry_ids))
                conn.execute(
                    f"""DELETE FROM industry
                        WHERE id NOT IN (SELECT industry_id FROM company)
                          AND name NOT IN ({marks})""",
                    list(industry_ids))
        return len(industry_ids), companies
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

async def run_daily(db_path=None):
    """Fetch news -> score with Gemini -> enrich tickers -> store; returns
    (articles, industries, companies) counts."""
    async with aiohttp.ClientSession() as session:
        articles = await fetch_daily_news(session)
        if not articles:
            print("No news articles in the last 24h; nothing to score.")
            return 0, 0, 0
        conn = connect(db_path)
        known = [r[0] for r in conn.execute(
            "SELECT name FROM industry ORDER BY name")]
        conn.close()
        print(f"Fetched {len(articles)} articles; scoring with {active_label()}...")
        impressions = await score_news(session, articles, known)

        impressions["companies"] = impressions.get("companies", [])[:MAX_COMPANIES]
        # dedup: a ticker the model repeats would burn 4 throttled Massive
        # calls (~48s at the default MASSIVE_RPM=5) fetching the same facts
        tickers = list(dict.fromkeys(c["ticker"].strip().upper()
                                     for c in impressions["companies"]))
        eta = ("" if MASSIVE_RPM <= 0 else
               f" (~{len(tickers) * 4 * 60 // MASSIVE_RPM // 60} min at "
               f"MASSIVE_RPM={MASSIVE_RPM}; set MASSIVE_RPM=0 for paid plans)")
        print(f"Enriching {len(tickers)} tickers with fundamentals...{eta}")
        facts = await asyncio.gather(
            *(fetch_fundamentals(session, t) for t in tickers))
        fundamentals = dict(zip(tickers, facts))

    industries, companies = save_impressions(impressions, fundamentals, db_path)
    return len(articles), industries, companies


def _print_tables(db_path=None):
    conn = connect(db_path)
    try:
        print("Industries:")
        for row in conn.execute(
                "SELECT * FROM industry ORDER BY sentiment DESC"):
            print(f"  {row['name']:<32} sentiment {row['sentiment']:>2}/10  "
                  f"activity {row['recent_activity']:>2}/10")
        print("Companies:")
        for row in conn.execute(
                """SELECT c.*, i.name AS industry FROM company c
                   JOIN industry i ON i.id = c.industry_id
                   ORDER BY c.sentiment DESC"""):
            pe = f"{row['pe_ratio']:.1f}" if row["pe_ratio"] else "-"
            price = f"{row['share_price']:.2f}" if row["share_price"] else "-"
            yoy = (f"{row['yoy_performance']:+.1f}%"
                   if row["yoy_performance"] is not None else "-")
            print(f"  {row['ticker']:<6} {row['industry']:<28} "
                  f"sentiment {row['sentiment']:>2}/10  "
                  f"activity {row['recent_activity']:>2}/10  "
                  f"P/E {pe:>7}  price {price:>9}  Y/Y {yoy:>8}")
    finally:
        conn.close()


def main():
    print(f"Impressions DB: {DB_PATH}")
    articles, industries, companies = asyncio.run(run_daily())
    if articles:
        print(f"Scored {industries} industries and {companies} companies "
              f"from {articles} articles.")
        _print_tables()


if __name__ == "__main__":
    main()
