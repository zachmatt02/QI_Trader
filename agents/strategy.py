import asyncio
import json
import os
import random
from datetime import datetime
import polars as pl

# To use real HTTP calls, you would install aiohttp: `pip install aiohttp`
# import aiohttp 

async def call_ai_api(data: pl.DataFrame) -> dict:
    """
    Sends the aggregated market data to an AI API (e.g., OpenAI, Gemini, Anthropic) 
    and receives a trading signal.
    """
    # Convert Polars DataFrame to JSON string to send to the API
    # In a real system, you might only send summary statistics or technical indicators to save tokens.
    data_json = data.write_json()
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Sending batch of {len(data)} records to AI API...")
    
    # =====================================================================
    # REAL API CALL EXAMPLE (Using aiohttp)
    # =====================================================================
    # async with aiohttp.ClientSession() as session:
    #     api_key = os.environ.get("AI_API_KEY", "YOUR_API_KEY_HERE")
    #     headers = {
    #         "Authorization": f"Bearer {api_key}",
    #         "Content-Type": "application/json"
    #     }
    #     payload = {
    #         "model": "gpt-4o",  # Or your chosen model
    #         "messages": [
    #             {
    #                 "role": "system", 
    #                 "content": "You are a quantitative trading AI. Analyze the provided market data JSON. Output only a JSON object with 'decision' (BUY, SELL, HOLD), 'confidence' (0.0 to 1.0), and 'reasoning'."
    #             },
    #             {
    #                 "role": "user", 
    #                 "content": data_json
    #             }
    #         ],
    #         "response_format": {"type": "json_object"}
    #     }
    #     try:
    #         async with session.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers) as resp:
    #             if resp.status == 200:
    #                 result = await resp.json()
    #                 content = result['choices'][0]['message']['content']
    #                 return json.loads(content)
    #             else:
    #                 error_text = await resp.text()
    #                 print(f"API Error ({resp.status}): {error_text}")
    #     except Exception as e:
    #         print(f"Network error: {e}")
    # =====================================================================
    
    # --- MOCK API RESPONSE (Simulating the AI thinking) ---
    await asyncio.sleep(1.0)  # Simulate network and processing latency
    
    decision = random.choices(["BUY", "SELL", "HOLD"], weights=[0.15, 0.15, 0.70])[0]
    confidence = round(random.uniform(0.6, 0.95), 2)
    
    return {
        "timestamp": datetime.now().isoformat(),
        "decision": decision,
        "confidence": confidence,
        "reasoning": f"Simulated AI analysis based on {len(data)} recent ticks. Volume and price action indicated a {decision} signal."
    }

async def process_data_stream():
    """
    Simulates receiving data from the Ingestion Agent (e.g., reading from TimescaleDB, 
    Kafka, or a Parquet file), and then forwards it to the AI for a decision.
    """
    # Expected schema matching ingestion.py
    schema = {
        "timestamp": pl.Datetime,
        "ticker": pl.String,
        "price": pl.Float64,
        "volume": pl.Int64
    }
    
    while True:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Strategy Agent waiting for next data batch...")
        await asyncio.sleep(5)  # Polling interval
        
        # --- Simulating data retrieval ---
        # In production, this would read the latest data from the database/queue
        mock_buffer = []
        for _ in range(20):
            mock_buffer.append({
                "timestamp": datetime.now(),
                "ticker": "TSLA",
                "price": round(random.uniform(180.0, 185.0), 2),
                "volume": random.randint(10, 500)
            })
            
        df = pl.DataFrame(mock_buffer, schema=schema)
        
        # --- Sending data to AI ---
        ai_response = await call_ai_api(df)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] AI Decision Received:")
        print(json.dumps(ai_response, indent=2))
        
        if ai_response.get("decision") in ["BUY", "SELL"]:
            print(f"--> Actionable signal! Forwarding {ai_response['decision']} order to Execution Agent...")
        else:
            print("--> Holding position. No action taken.")
            
        print("-" * 60)

if __name__ == "__main__":
    print("Starting AI Strategy Agent initialization...")
    try:
        asyncio.run(process_data_stream())
    except KeyboardInterrupt:
        print("Strategy Agent shut down safely.")
