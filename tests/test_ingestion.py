import aiohttp
import pytest
from datetime import datetime
from unittest.mock import patch

from gateway import ingestion
from gateway.ingestion import market_stream, main

@pytest.mark.asyncio
async def test_market_stream_requires_gateway():
    """Without a reachable gateway the stream raises instead of yielding
    simulated data.

    Points the gateway URL at an unreachable address so the test behaves the
    same whether or not a real gateway is running.
    """
    with patch("gateway.ingestion.GATEWAY_BASE_URL", "https://127.0.0.1:1/v1/api"):
        stream = market_stream()
        with pytest.raises(aiohttp.ClientError):
            await anext(stream)
        await stream.aclose()

@pytest.mark.asyncio
async def test_main(tmp_path, capsys, monkeypatch):
    """Test that main stores batches of 20 ticks correctly."""
    # Keep test ticks out of the real data/ticks directory
    monkeypatch.setattr(ingestion, "DATA_DIR", tmp_path / "ticks")

    # Mock the infinite stream with a finite one
    async def finite_market_stream():
        for i in range(25):
            yield {
                "timestamp": datetime.now(),
                "ticker": "TSLA",
                "price": 182.5,
                "volume": 100 + i
            }

    with patch("gateway.ingestion.market_stream", finite_market_stream):
        await main()

    captured = capsys.readouterr()

    assert "Starting Data Agent initialization..." in captured.out

    # We yield 25 ticks, so we should see exactly one batch of 20 stored.
    assert captured.out.count("Stored batch of 20 ticks.") == 1
