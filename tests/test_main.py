from unittest.mock import AsyncMock, patch

import pytest

import main
from agents.decision import save_decisions, _connect


def _decisions(db_path):
    verdict = {"decisions": [
        {"ticker": "TSM", "action": "BUY", "confidence": 9,
         "limit_price": 212.5, "quantity": 2, "reasoning": "strong"},
        {"ticker": "JPM", "action": "HOLD", "confidence": 9,
         "limit_price": 290.0, "quantity": 1, "reasoning": "hold"},
        {"ticker": "SOFI", "action": "SELL", "confidence": 9,
         "limit_price": 8.5, "quantity": 5, "reasoning": "weak"},
        {"ticker": "NVDA", "action": "BUY", "confidence": 3,
         "limit_price": 195.0, "quantity": 1, "reasoning": "low conviction"},
        {"ticker": "AMZN", "action": "BUY", "confidence": 9,
         "quantity": 1, "reasoning": "no price given"},
    ]}
    return save_decisions(verdict, db_path)


@pytest.mark.asyncio
async def test_act_on_applies_guardrails(tmp_path):
    db = tmp_path / "impressions.db"
    decisions = _decisions(db)

    with patch("main.execution.execute_signal",
               new_callable=AsyncMock) as execute, \
         patch("main.transactions.position", return_value=3):
        placed = await main.act_on(decisions, db)

    # only the confident BUY with a price and the SELL go out
    assert placed == 2
    calls = [c.args for c in execute.await_args_list]
    assert calls[0] == ("BUY", "TSM", 212.5, 2)
    assert calls[1] == ("SELL", "SOFI", 8.5, 3)  # clipped to shares held

    conn = _connect(db)
    executed = {r["ticker"]: r["executed"]
                for r in conn.execute("SELECT ticker, executed FROM decision")}
    conn.close()
    assert executed == {"TSM": 1, "SOFI": 1, "JPM": 0, "NVDA": 0, "AMZN": 0}


@pytest.mark.asyncio
async def test_act_on_respects_order_cap_and_survives_errors(tmp_path):
    db = tmp_path / "impressions.db"
    verdict = {"decisions": [
        {"ticker": t, "action": "BUY", "confidence": 9, "limit_price": 10.0,
         "quantity": 1, "reasoning": "r"}
        for t in ("AAA", "BBB", "CCC", "DDD")]}
    decisions = save_decisions(verdict, db)

    with patch("main.execution.execute_signal",
               new_callable=AsyncMock) as execute:
        execute.side_effect = [RuntimeError("gateway down"), None, None, None]
        placed = await main.act_on(decisions, db)

    # the failed order doesn't kill the cycle or count against the cap
    assert placed == main.MAX_ORDERS_PER_CYCLE == 3
    assert execute.await_count == 4


@pytest.mark.asyncio
async def test_act_on_caps_quantity(tmp_path):
    db = tmp_path / "impressions.db"
    verdict = {"decisions": [
        {"ticker": "TSM", "action": "BUY", "confidence": 9,
         "limit_price": 212.5, "quantity": 500, "reasoning": "greedy"}]}
    decisions = save_decisions(verdict, db)

    with patch("main.execution.execute_signal",
               new_callable=AsyncMock) as execute:
        await main.act_on(decisions, db)

    assert execute.await_args.args == ("BUY", "TSM", 212.5, main.MAX_QTY)
