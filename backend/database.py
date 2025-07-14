import sqlite3
import pandas as pd
from datetime import datetime
import os

class StockDatabase:
    def __init__(self):
        self.db_path = "data/stocks.db"
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with required tables"""
        os.makedirs("data", exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create stocks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                volume INTEGER,
                market_cap REAL,
                pe_ratio REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create daily_summary table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume INTEGER,
                date DATE,
                UNIQUE(symbol, date)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def insert_stock_data(self, symbol, price, volume=None, market_cap=None, pe_ratio=None):
        """Insert real-time stock data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO stocks (symbol, price, volume, market_cap, pe_ratio)
            VALUES (?, ?, ?, ?, ?)
        ''', (symbol, price, volume, market_cap, pe_ratio))
        
        conn.commit()
        conn.close()
    
    def get_latest_prices(self, symbols=None):
        """Get latest prices for all or specific symbols"""
        conn = sqlite3.connect(self.db_path)
        
        if symbols:
            query = '''
                SELECT symbol, price, timestamp 
                FROM stocks 
                WHERE symbol IN ({})
                AND timestamp = (
                    SELECT MAX(timestamp) 
                    FROM stocks s2 
                    WHERE s2.symbol = stocks.symbol
                )
            '''.format(','.join(['?'] * len(symbols)))
            df = pd.read_sql_query(query, conn, params=symbols)
        else:
            query = '''
                SELECT symbol, price, timestamp 
                FROM stocks 
                WHERE timestamp = (
                    SELECT MAX(timestamp) 
                    FROM stocks s2 
                    WHERE s2.symbol = stocks.symbol
                )
            '''
            df = pd.read_sql_query(query, conn)
        
        conn.close()
        return df
    
    def get_price_history(self, symbol, hours=24):
        """Get price history for a symbol"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT price, timestamp 
            FROM stocks 
            WHERE symbol = ? 
            AND timestamp >= datetime('now', '-{} hours')
            ORDER BY timestamp
        '''.format(hours)
        
        df = pd.read_sql_query(query, conn, params=[symbol])
        conn.close()
        return df