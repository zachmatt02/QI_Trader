import pytest
import asyncio
from datetime import datetime
from unittest.mock import patch, AsyncMock
import polars as pl

from agents.strategy import call_ai_api, process_data_stream

@pytest.mark.asyncio
async def test_call_ai_api():
    """Test that call_ai_api returns a correctly structured decision dictionary."""
    # Create a mock DataFrame representing market data
    df = pl.DataFrame({
        "timestamp": [datetime.now()],
        "ticker": ["TSLA"],
        "price": [182.5],
        "volume": [100]
    })
    
    # Mock asyncio.sleep to run instantly so we don't wait for 1.0 second simulation
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        response = await call_ai_api(df)
        mock_sleep.assert_awaited_once_with(1.0)
        
    assert "timestamp" in response
    assert response["decision"] in ["BUY", "SELL", "HOLD"]
    assert isinstance(response["confidence"], float)
    assert 0.6 <= response["confidence"] <= 0.95
    assert "reasoning" in response
    assert isinstance(response["reasoning"], str)
    assert len(response["reasoning"]) > 0

@pytest.mark.asyncio
async def test_process_data_stream_actionable_signal(capsys):
    """Test process_data_stream prints routing messages for actionable BUY/SELL signals."""
    mock_response = {
        "timestamp": datetime.now().isoformat(),
        "decision": "BUY",
        "confidence": 0.9,
        "reasoning": "Simulated AI analysis indicating a BUY signal."
    }
    
    sleep_calls = 0
    async def mock_sleep(seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        # Stop the infinite loop on the second iteration
        if sleep_calls > 1:
            raise KeyboardInterrupt("Stop loop")
        return

    with patch("agents.strategy.call_ai_api", new_callable=AsyncMock) as mock_call, \
         patch("asyncio.sleep", side_effect=mock_sleep):
        
        mock_call.return_value = mock_response
        try:
            await process_data_stream()
        except KeyboardInterrupt:
            pass  # Expected way to exit the infinite loop in test
            
    captured = capsys.readouterr()
    assert "Strategy Agent waiting for next data batch..." in captured.out
    assert "AI Decision Received:" in captured.out
    assert "--> Actionable signal! Forwarding BUY order to Execution Agent..." in captured.out

@pytest.mark.asyncio
async def test_process_data_stream_hold_signal(capsys):
    """Test process_data_stream prints hold messages when decision is HOLD."""
    mock_response = {
        "timestamp": datetime.now().isoformat(),
        "decision": "HOLD",
        "confidence": 0.8,
        "reasoning": "Simulated AI analysis indicating a HOLD signal."
    }
    
    sleep_calls = 0
    async def mock_sleep(seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        # Stop the infinite loop on the second iteration
        if sleep_calls > 1:
            raise KeyboardInterrupt("Stop loop")
        return

    with patch("agents.strategy.call_ai_api", new_callable=AsyncMock) as mock_call, \
         patch("asyncio.sleep", side_effect=mock_sleep):
        
        mock_call.return_value = mock_response
        try:
            await process_data_stream()
        except KeyboardInterrupt:
            pass  # Expected way to exit the infinite loop in test
            
    captured = capsys.readouterr()
    assert "Strategy Agent waiting for next data batch..." in captured.out
    assert "AI Decision Received:" in captured.out
    assert "--> Holding position. No action taken." in captured.out
