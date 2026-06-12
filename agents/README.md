# QI Trader Agents

The active workforce. This folder contains the independent, running programs that actually do the work: pulling live market data, predicting price movements, enforcing safety rules, and sending the final buy/sell orders to the broker.

## Agent Files

### 1. Data Ingestion Agent (`ingestion.py`)
* **Role**: Handles real-time market data ingestion.
* **Functionality**:
  * Simulates an incoming high-frequency WebSocket tick stream (mocking TSLA tick data for testing).
  * Buffers and batches incoming market ticks (e.g., in chunks of 20).
  * Converts raw tick records into structured Polars DataFrames using a strict schema.
  * In production, this agent is responsible for saving data (e.g., to TimescaleDB or Parquet files) so that strategy agents can read it.

### 2. AI Strategy Agent (`strategy.py`)
* **Role**: Analyzes market data and queries AI models to retrieve trading signals.
* **Functionality**:
  * Simulates retrieving batches of aggregated market data (polling-based).
  * Serializes Polars DataFrames to JSON and prepares a prompt structure for the LLM.
  * Calls an AI model API (includes examples for OpenAI, Gemini, etc.) to analyze the data and return a JSON trading decision (`BUY`, `SELL`, `HOLD`) with a confidence level and reasoning.
  * Logs the decisions and simulates forwarding actionable signals (`BUY` or `SELL`) to an Execution Agent.
