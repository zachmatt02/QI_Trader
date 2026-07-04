import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from agents import dashboard, transactions


@pytest_asyncio.fixture
async def client():
    client = TestClient(TestServer(dashboard.create_app()))
    await client.start_server()
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_transactions_page_served(client):
    resp = await client.get("/transactions")
    assert resp.status == 200
    body = await resp.text()
    assert "Transaction ledger" in body
    assert "Net positions" in body


@pytest.mark.asyncio
async def test_api_transactions_returns_ledger(client):
    # the conftest fixture points the ledger at a throwaway file
    transactions.record_transaction("TSLA", 420.0, 3, "BUY",
                                    when="2026-07-03T14:30:00Z")
    transactions.record_transaction("TSLA", 430.0, 1, "SELL",
                                    when="2026-07-03T15:00:00Z")
    transactions.record_transaction("AAPL", 200.0, 2, "BUY",
                                    when="2026-07-03T15:30:00Z")

    resp = await client.get("/api/transactions")
    assert resp.status == 200
    data = await resp.json()
    assert data["total"] == 3
    assert len(data["transactions"]) == 3
    assert data["transactions"][0]["ticker"] == "AAPL"  # newest first
    assert data["positions"] == [
        {"ticker": "AAPL", "shares": 2, "trades": 1},
        {"ticker": "TSLA", "shares": 2, "trades": 2},
    ]

    resp = await client.get("/api/transactions?ticker=tsla")
    data = await resp.json()
    assert data["total"] == 2
    assert all(t["ticker"] == "TSLA" for t in data["transactions"])


@pytest.mark.asyncio
async def test_api_transactions_empty_ledger(client):
    resp = await client.get("/api/transactions")
    assert resp.status == 200
    data = await resp.json()
    assert data["transactions"] == []
    assert data["positions"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_api_transactions_rejects_bad_limit(client):
    resp = await client.get("/api/transactions?limit=abc")
    assert resp.status == 400
