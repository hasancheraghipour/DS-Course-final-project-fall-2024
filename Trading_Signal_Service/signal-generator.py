from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional
import uvicorn
import requests
import json
import websockets
import asyncio
import logging

app = FastAPI()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("SignalProcessor")


class IndicatorData(BaseModel):
    stock_symbol: str
    opening_price: float
    closing_price: float
    high: float
    low: float
    volume: int
    timestamp: float
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    order_type: Optional[str] = None
    price: Optional[float] = None
    quantity: Optional[int] = None
    sentiment_score: Optional[float] = None
    sentiment_magnitude: Optional[float] = None
    indicator_name: Optional[str] = None
    value: Optional[float] = None
    moving_avg: float
    ema: float
    rsi: float

live_signals: Dict[str, dict] = {}

@app.post("/process_indicators")
async def process_indicators(data: IndicatorData):
    
    signal = {
        "stock": data.stock_symbol,
        "action": "HOLD",
        "reason": "No signal",
        "timestamp": data.timestamp
    }

   
    if data.rsi > 70:
        signal.update({"action": "SELL", "reason": "Overbought (RSI > 70)"})
    elif data.rsi < 30:
        signal.update({"action": "BUY", "reason": "Oversold (RSI < 30)"})

    
    elif data.closing_price > data.ema * 1.05:
        signal.update({"action": "SELL", "reason": "Price 5% above EMA"})
    elif data.closing_price < data.ema * 0.95:
        signal.update({"action": "BUY", "reason": "Price 5% below EMA"})

  
    if data.sentiment_score is not None:
        if data.sentiment_score < -0.5:
            signal.update({"action": "SELL", "reason": "Negative Sentiment"})
        elif data.sentiment_score > 0.5:
            signal.update({"action": "BUY", "reason": "Positive Sentiment"})

   
    live_signals[data.stock_symbol] = signal

   
    logger.info(f"Generated signal: {json.dumps(signal)}")

  
    try:
        response = requests.post(
            "http://notification-service:8000/notify",
            json=signal
        )
        if response.status_code == 200:
            logger.info(f"Notification sent successfully for {data.stock_symbol}")
        else:
            logger.warning(f"Notification failed for {data.stock_symbol}: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to send notification: {str(e)}")

    return {"status": "signal processed"}


async def websocket_handler(websocket, path):
    while True:
        await websocket.send(json.dumps(live_signals))
        await asyncio.sleep(1)

if name == "main":
    import threading

   
    async def start_websocket():
        start_server = websockets.serve(websocket_handler, "0.0.0.0", 8765)
        await start_server  # WebSocket server must run in an asyncio event loop


    def run_websocket():
        asyncio.run(start_websocket())

    websocket_thread = threading.Thread(target=run_websocket, daemon=True)
    websocket_thread.start()

   
    uvicorn.run(app, host="0.0.0.0", port=5000)
