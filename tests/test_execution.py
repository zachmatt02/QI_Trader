import pytest
from unittest.mock import patch

from gateway import transactions
from gateway.execution import (build_order, get_account_id, place_order,
                              execute_signal, reconcile_fills,
                              wait_for_status)


class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    """Returns queued responses in order and records every request made."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url):
        self.calls.append(("GET", url, None))
        return FakeResponse(self._responses.pop(0))

    def post(self, url, json=None):
        self.calls.append(("POST", url, json))
        return FakeResponse(self._responses.pop(0))

    async def close(self):
        pass


def test_build_order_validates_input():
    order = build_order(76792991, "buy", 1, price=420.0)
    assert order["side"] == "BUY"
    assert order["orderType"] == "LMT"
    assert order["price"] == 420.0
    assert order["cOID"].startswith("qi-trader-")

    with pytest.raises(ValueError, match="side"):
        build_order(76792991, "HOLD", 1, price=420.0)
    with pytest.raises(ValueError, match="quantity"):
        build_order(76792991, "BUY", 999, price=420.0)
    with pytest.raises(ValueError, match="price"):
        build_order(76792991, "BUY", 1)


@pytest.mark.asyncio
async def test_get_account_id_refuses_live_account():
    with patch("gateway.execution.ALLOW_LIVE", False):
        session = FakeSession([{"accounts": ["U1234567"]}])
        with pytest.raises(RuntimeError, match="paper"):
            await get_account_id(session)

        session = FakeSession([{"accounts": ["DUR149933"]}])
        assert await get_account_id(session) == "DUR149933"


@pytest.mark.asyncio
async def test_place_order_answers_confirmation_prompts():
    session = FakeSession([
        [{"id": "q1", "message": ["price exceeds the percentage constraint"]}],
        [{"id": "q2", "message": ["another warning"]}],
        [{"order_id": "123", "order_status": "Submitted"}],
    ])
    order = build_order(76792991, "BUY", 1, price=420.0)
    result = await place_order(session, "DUR149933", order)

    assert result["order_id"] == "123"
    reply_calls = [c for c in session.calls if "/iserver/reply/" in c[1]]
    assert [c[1].rsplit("/", 1)[1] for c in reply_calls] == ["q1", "q2"]
    assert all(c[2] == {"confirmed": True} for c in reply_calls)


@pytest.mark.asyncio
async def test_place_order_raises_on_rejection():
    session = FakeSession([{"error": "insufficient funds"}])
    order = build_order(76792991, "BUY", 1, price=420.0)
    with pytest.raises(RuntimeError, match="insufficient funds"):
        await place_order(session, "DUR149933", order)


@pytest.mark.asyncio
async def test_execute_signal_dry_run_never_places_orders():
    session = FakeSession([
        {"accounts": ["DUR149933"]},
        [{"conid": "76792991"}],
        {"amount": {"total": "420.5 USD", "commission": "1 USD"}},
    ])
    with patch("gateway.execution.aiohttp") as fake_aiohttp, \
         patch("gateway.execution.DRY_RUN", True):
        fake_aiohttp.ClientSession.return_value = session
        result = await execute_signal("BUY", "TSLA", 420.0)

    assert result is None
    order_posts = [c for c in session.calls
                   if c[0] == "POST" and c[1].endswith("/orders")]
    assert order_posts == []


@pytest.mark.asyncio
async def test_execute_signal_places_and_tracks_when_live():
    session = FakeSession([
        {"accounts": ["DUR149933"]},
        [{"conid": "76792991"}],
        {"amount": {"total": "420.5 USD", "commission": "1 USD"}},
        [{"order_id": "7", "order_status": "Submitted"}],
        {"orders": [{"orderId": 7, "status": "Filled"}]},
    ])
    with patch("gateway.execution.aiohttp") as fake_aiohttp, \
         patch("gateway.execution.DRY_RUN", False):
        fake_aiohttp.ClientSession.return_value = session
        result = await execute_signal("BUY", "TSLA", 420.0)

    assert result["status"] == "Filled"


@pytest.mark.asyncio
async def test_reconcile_fills_backfills_missed_fills():
    # order 7 was already recorded by record_fill during its own cycle
    transactions.record_transaction("SMCI", 27.24, 5, "BUY", order_id="7")
    session = FakeSession([
        {"accounts": ["DUR149933"]},
        {"orders": []},  # first call warms the endpoint up; retried once
        {"orders": [
            {"orderId": 7, "status": "Filled", "ticker": "SMCI",
             "filledQuantity": 5, "avgPrice": "27.24", "side": "BUY"},
            # filled after wait_for_status stopped polling -> must be added
            {"orderId": 8, "status": "Filled", "ticker": "NVDA",
             "filledQuantity": 5, "avgPrice": "194.42", "side": "BUY",
             "cashCcy": "USD", "conid": 4815747,
             "lastExecutionTime_r": 1783346798000},
            # still working -> must NOT be recorded yet
            {"orderId": 9, "status": "Submitted", "ticker": "HOOD",
             "filledQuantity": 0, "price": 113.5, "side": "BUY"},
            # partial fill then cancelled -> the filled shares count
            {"orderId": 10, "status": "Cancelled", "ticker": "WRAP",
             "filledQuantity": 4, "avgPrice": 1.44, "side": "BUY"},
        ]},
    ])
    with patch("gateway.execution.aiohttp") as fake_aiohttp:
        fake_aiohttp.ClientSession.return_value = session
        added = await reconcile_fills()

    assert added == 2
    assert transactions.position("NVDA") == 5
    assert transactions.position("WRAP") == 4
    assert transactions.position("HOOD") == 0
    assert transactions.count_transactions("SMCI") == 1  # no duplicate
    nvda = transactions.list_transactions("NVDA")[0]
    assert nvda["share_price"] == 194.42
    assert nvda["order_id"] == "8"
    assert nvda["account_id"] == "DUR149933"
    assert nvda["datetime"] == "2026-07-06T14:06:38Z"  # broker fill time


@pytest.mark.asyncio
async def test_reconcile_fills_survives_gateway_outage():
    with patch("gateway.execution.aiohttp") as fake_aiohttp, \
         patch("gateway.execution.get_account_id",
               side_effect=RuntimeError("gateway returned HTTP 401")):
        fake_aiohttp.ClientSession.return_value = FakeSession([])
        added = await reconcile_fills()
    assert added == 0
    assert transactions.count_transactions() == 0
