# QI Trader Tests

The safety checkpoint. This holds automated diagnostic scripts that check your code for bugs and logic errors every time you make an update, ensuring a typo doesn't accidentally trigger a bad trade.

## Test Files

### 1. Ingestion Agent Test (`test_ingestion.py`)
This test suite verifies the functionality of the Data Ingestion Agent (`agents/ingestion.py`).

#### How the Tests Work
* **Environment Setup & Path Resolution**:
  - Dynamically adds the parent directory to Python's system path (`sys.path.insert(...)`) so it can import the `agents` modules.
* **Async Integration (`pytest-asyncio`)**:
  - Since the ingestion code is asynchronous (using async generator loops and coroutines), the tests are marked with `@pytest.mark.asyncio` to execute them inside a running async event loop.
* **Mock Market Stream Test (`test_mock_market_stream`)**:
  - Verifies the mock WebSocket feed (`mock_market_stream`) yields correctly structured tick dictionaries.
  - A `break` statement prevents the test from hanging on the infinite tick generator.
* **Batch Processing Test (`test_main`)**:
  - Overrides the infinite `mock_market_stream` generator with a temporary `finite_mock_market_stream` yielding exactly **25 ticks** using `unittest.mock.patch`.
  - Asserts that exactly **one** batch of 20 ticks is processed (with 5 left in the buffer).

### 2. Strategy Agent Test (`test_strategy.py`)
This test suite verifies the functionality of the AI Strategy Agent (`agents/strategy.py`).

#### How the Tests Work
* **AI API Calling Test (`test_call_ai_api`)**:
  - Instantiates a mock Polars DataFrame and passes it to `call_ai_api`.
  - Mocks `asyncio.sleep` to bypass the simulated 1.0 second network processing delay.
  - Asserts that the response is structured properly with keys: `"timestamp"`, `"decision"` (one of BUY, SELL, HOLD), `"confidence"`, and `"reasoning"`.
* **Data Stream Process Test (`test_process_data_stream_actionable_signal` & `test_process_data_stream_hold_signal`)**:
  - Patches `call_ai_api` to return specific mock responses (e.g., a `BUY` signal vs. a `HOLD` signal).
  - Patches `asyncio.sleep` with a helper that triggers a `KeyboardInterrupt` after the first loop iteration, which safely breaks out of the infinite polling loop.
  - Captures `sys.stdout` using pytest's `capsys` fixture to ensure the agent prints the correct decision logging and routes orders as expected (e.g., forwarding actionable signals to the Execution Agent).

## Running the Tests
To run the test suite, ensure the virtual environment is used:
```bash
./.venv/bin/pytest
```

