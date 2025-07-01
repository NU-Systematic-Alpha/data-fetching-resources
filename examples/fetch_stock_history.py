#!/usr/bin/env python3
"""
Example: Fetching historical stock data using Polygon API
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
import pandas as pd
from src.polygon import StockDataFetcher

def main():
    # Initialize the stock data fetcher
    fetcher = StockDataFetcher()
    
    # Define parameters
    ticker = 'AAPL'
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)  # Last 30 days
    
    print(f"Fetching data for {ticker} from {start_date.date()} to {end_date.date()}")
    
    # Example 1: Fetch daily bars
    print("\n1. Fetching daily bars...")
    daily_bars = fetcher.fetch_bars(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        timeframe='day',
        adjusted=True
    )
    
    if not daily_bars.empty:
        print(f"   Retrieved {len(daily_bars)} daily bars")
        print("\n   Last 5 days:")
        print(daily_bars.tail())
        
        # Save to parquet
        fetcher.save_to_parquet(daily_bars, ticker, 'daily_bars')
    
    # Example 2: Fetch intraday (hourly) bars
    print("\n2. Fetching hourly bars for the last 5 days...")
    hourly_bars = fetcher.fetch_bars(
        ticker=ticker,
        start_date=end_date - timedelta(days=5),
        end_date=end_date,
        timeframe='hour',
        adjusted=True
    )
    
    if not hourly_bars.empty:
        print(f"   Retrieved {len(hourly_bars)} hourly bars")
        print(f"   Average hourly volume: {hourly_bars['volume'].mean():,.0f}")
        print(f"   Average hourly VWAP: ${hourly_bars['vwap'].mean():.2f}")
    
    # Example 3: Fetch snapshot (latest data)
    print("\n3. Fetching latest snapshot...")
    snapshot = fetcher.fetch_snapshot(ticker)
    
    if snapshot:
        print(f"   Latest price: ${snapshot['last_trade']['price']:.2f}")
        print(f"   Day's range: ${snapshot['day']['low']:.2f} - ${snapshot['day']['high']:.2f}")
        print(f"   Volume: {snapshot['day']['volume']:,}")
    
    # Example 4: Fetch multiple tickers
    print("\n4. Fetching data for multiple tickers...")
    tickers = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']
    multi_data = fetcher.fetch_multiple_bars(
        tickers=tickers,
        start_date=end_date - timedelta(days=7),
        end_date=end_date,
        timeframe='day'
    )
    
    print("\n   Last closing prices:")
    for ticker, data in multi_data.items():
        if not data.empty:
            last_close = data['close'].iloc[-1]
            print(f"   {ticker}: ${last_close:.2f}")
    
    # Example 5: Calculate simple statistics
    print("\n5. Calculating statistics for AAPL...")
    if not daily_bars.empty:
        returns = daily_bars['close'].pct_change().dropna()
        
        print(f"   Daily return statistics (last 30 days):")
        print(f"   - Mean return: {returns.mean()*100:.3f}%")
        print(f"   - Std deviation: {returns.std()*100:.3f}%")
        print(f"   - Sharpe ratio (annualized): {(returns.mean() / returns.std()) * (252**0.5):.2f}")
        
        # Moving averages
        daily_bars['MA_5'] = daily_bars['close'].rolling(5).mean()
        daily_bars['MA_20'] = daily_bars['close'].rolling(20).mean()
        
        last_row = daily_bars.iloc[-1]
        print(f"\n   Technical indicators:")
        print(f"   - Last close: ${last_row['close']:.2f}")
        print(f"   - 5-day MA: ${last_row['MA_5']:.2f}")
        print(f"   - 20-day MA: ${last_row['MA_20']:.2f}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure you have:")
        print("1. Created config/config.ini from config/config.ini.example")
        print("2. Added your Polygon API key to the config file")