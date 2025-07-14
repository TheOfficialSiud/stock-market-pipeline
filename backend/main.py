from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import os
import json
import asyncio
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from data_fetcher import StockDataFetcher
from database import StockDatabase
import logging

app = FastAPI(title="Stock Analytics API")

# Initialize components
data_fetcher = StockDataFetcher()
db = StockDatabase()
scheduler = BackgroundScheduler()

# Store active WebSocket connections
active_connections = []

class ConnectionManager:
    def __init__(self):
        self.active_connections = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                # Remove broken connections
                self.active_connections.remove(connection)

manager = ConnectionManager()

@app.on_event("startup")
async def startup_event():
    """Start background scheduler for data fetching"""
    scheduler.add_job(
        data_fetcher.fetch_real_time_data,
        'interval',
        minutes=2,  # Fetch every 2 minutes
        id='fetch_stock_data'
    )
    scheduler.start()
    logging.info("Background scheduler started")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop background scheduler"""
    scheduler.shutdown()

@app.get("/")
async def root():
    """Serve the dashboard HTML"""
    try:
        # Get the path to the HTML file
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content)
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Dashboard not found. Please check frontend/index.html</h1>")
    except Exception as e:
        return HTMLResponse(content=f"<h1>Error loading dashboard: {str(e)}</h1>")

@app.get("/api/market-summary")
async def get_market_summary():
    """Get current market summary"""
    return data_fetcher.get_market_summary()

@app.get("/api/stock/{symbol}")
async def get_stock_data(symbol: str):
    """Get specific stock data"""
    latest_data = db.get_latest_prices([symbol.upper()])
    history = db.get_price_history(symbol.upper(), hours=24)
    
    return {
        "symbol": symbol.upper(),
        "latest": latest_data.to_dict('records')[0] if not latest_data.empty else None,
        "history": history.to_dict('records')
    }

@app.get("/api/stocks/trending")
async def get_trending_stocks():
    """Get trending stocks based on volume"""
    summary = data_fetcher.get_market_summary()
    
    # Sort by absolute change percentage
    trending = sorted(
        summary['stocks'], 
        key=lambda x: abs(x['change_percent']), 
        reverse=True
    )
    
    return {
        "trending": trending[:5],  # Top 5 trending
        "gainers": [s for s in trending if s['change_percent'] > 0][:3],
        "losers": [s for s in trending if s['change_percent'] < 0][:3]
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time data"""
    await manager.connect(websocket)
    try:
        while True:
            # Send market summary every 30 seconds
            summary = data_fetcher.get_market_summary()
            await websocket.send_text(json.dumps(summary))
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Serve the HTML dashboard at root
@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """Serve the dashboard HTML"""
    try:
        with open("../frontend/index.html", "r") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Dashboard not found. Please create frontend/index.html</h1>")

# Mount static files for other assets
app.mount("/static", StaticFiles(directory="../frontend"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)