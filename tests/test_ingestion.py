import pytest
import asyncio
from datetime import datetime
from unittest.mock import patch

from agents.ingestion import mock_market_stream, main

@pytest.mark.asyncio
async def test_mock_market_stream():
    """Test that the mock_market_stream yields correctly formatted ticks.

    Points the gateway URL at an unreachable address so the test always
    exercises the simulated fallback, even if a real gateway is running.
    """
    with patch("agents.ingestion.GATEWAY_BASE_URL", "https://127.0.0.1:1/v1/api"):
        await _assert_simulated_stream()

async def _assert_simulated_stream():
    async for tick in mock_market_stream():
        assert "timestamp" in tick
        assert isinstance(tick["timestamp"], datetime)
        
        assert "ticker" in tick
        assert tick["ticker"] == "TSLA"
        
        assert "price" in tick
        assert isinstance(tick["price"], float)
        assert 180.0 <= tick["price"] <= 185.0
        
        assert "volume" in tick
        assert isinstance(tick["volume"], int)
        assert 10 <= tick["volume"] <= 500
        
        break  # Test only the first yielded item to avoid an infinite loop

@pytest.mark.asyncio
async def test_main(capsys):
    """Test that main processes batches of 20 ticks correctly."""
    # Mock the infinite stream with a finite one
    async def finite_mock_market_stream():
        for i in range(25):
            yield {
                "timestamp": datetime.now(),
                "ticker": "TSLA",
                "price": 182.5,
                "volume": 100 + i
            }
            
    with patch("agents.ingestion.mock_market_stream", finite_mock_market_stream):
        await main()
        
    captured = capsys.readouterr()
    
    assert "Starting Data Agent initialization..." in captured.out
    assert "Processed Batch of 20 ticks." in captured.out
    
    # We yield 25 ticks, so we should see exactly one batch of 20 processed.
    assert captured.out.count("Processed Batch of 20 ticks.") == 1
