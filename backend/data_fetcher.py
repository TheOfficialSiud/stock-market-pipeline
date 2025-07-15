import yfinance as yf
import pandas as pd
from datetime import datetime
import time
import logging
from database import StockDatabase

class StockDataFetcher:
    def __init__(self):
        self.db = StockDatabase()
        self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NFLX', 'NVDA','MCRB','PAY','SOL-USD','USDC-USD','WBTC-USD']
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def fetch_real_time_data(self):
        """Fetch real-time data for all symbols"""
        try:
            for symbol in self.symbols:
                ticker = yf.Ticker(symbol)
                
                # Get real-time data
                info = ticker.info
                hist = ticker.history(period="1d", interval="1m")
                
                if not hist.empty:
                    latest_price = hist['Close'].iloc[-1]
                    latest_volume = hist['Volume'].iloc[-1]
                    
                    # Get additional info
                    market_cap = info.get('marketCap', None)
                    pe_ratio = info.get('trailingPE', None)
                    
                    # Store in database
                    self.db.insert_stock_data(
                        symbol=symbol,
                        price=float(latest_price),
                        volume=int(latest_volume),
                        market_cap=market_cap,
                        pe_ratio=pe_ratio
                    )
                    
                    self.logger.info(f"Updated {symbol}: ${latest_price:.2f}")
                
                # Be nice to the API
                time.sleep(1)
                
        except Exception as e:
            self.logger.error(f"Error fetching data: {e}")
    
    def fetch_historical_data(self, symbol, period="1mo"):
        """Fetch historical data for analysis"""
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period)
            return hist
        except Exception as e:
            self.logger.error(f"Error fetching historical data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_market_summary(self):
        """Get market summary with key metrics"""
        latest_data = self.db.get_latest_prices(self.symbols)
        
        summary = {
            'total_stocks': len(latest_data),
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stocks': []
        }
        
        for _, row in latest_data.iterrows():
            # Get historical data to calculate change
            hist = self.fetch_historical_data(row['symbol'], period="2d")
            
            if len(hist) >= 2:
                prev_close = hist['Close'].iloc[-2]
                current_price = row['price']
                change = current_price - prev_close
                change_percent = (change / prev_close) * 100
            else:
                change = 0
                change_percent = 0
            
            summary['stocks'].append({
                'symbol': row['symbol'],
                'price': row['price'],
                'change': change,
                'change_percent': change_percent,
                'timestamp': row['timestamp']
            })
        
        return summary