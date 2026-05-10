# agents/data/ingestion.py
import polars as pl
import asyncio
import random
from datetime import datetime

async def mock_market_stream():
    """Simulates an incoming high-frequency WebSocket tick stream."""
    while True:
        # Simulating TSLA data for testing
        yield {
            "timestamp": datetime.now(),
            "ticker": "TSLA",
            "price": round(random.uniform(180.0, 185.0), 2),
            "volume": random.randint(10, 500)
        }
        # Simulating a 100ms tick latency
        await asyncio.sleep(0.1) 

async def main():
    print("Starting Data Agent initialization...")
    
    # Define the strict schema for Polars
    schema = {
        "timestamp": pl.Datetime,
        "ticker": pl.String,
        "price": pl.Float64,
        "volume": pl.Int64
    }
    
    # Buffer to hold ticks before converting to a DataFrame
    buffer = []
    
    async for tick in mock_market_stream():
        buffer.append(tick)
        
        # Process and flush the buffer in batches of 20 ticks
        if len(buffer) >= 20:
            # Convert the list of dicts into a Polars DataFrame
            df = pl.DataFrame(buffer, schema=schema)
            
            # Example Polars operation: Calculate VWAP or simple rolling metrics
            # In a real system, you would append this DataFrame to a TimescaleDB instance 
            # or save it as a Parquet file for the Strategy Agent to read.
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Processed Batch of {len(df)} ticks.")
            print(df.head(5)) 
            print("-" * 40)
            
            buffer.clear()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Data Agent shut down safely.")