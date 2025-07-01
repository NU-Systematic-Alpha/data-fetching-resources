#!/usr/bin/env python3
"""
Example: Fetching options data for a specific date range using Polygon API
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
import pandas as pd
from src.polygon import OptionsDataFetcher, StockDataFetcher

def main():
    # Initialize fetchers
    options_fetcher = OptionsDataFetcher()
    stock_fetcher = StockDataFetcher()
    
    # Define parameters
    ticker = 'SPY'
    analysis_date = datetime.now()
    lookback_days = 30
    
    print(f"Analyzing options for {ticker}")
    print(f"Analysis date: {analysis_date.date()}")
    print("-" * 50)
    
    # Example 1: Fetch current options chain
    print("\n1. Fetching current options chain...")
    
    # Get current stock price for reference
    stock_snapshot = stock_fetcher.fetch_snapshot(ticker)
    current_price = stock_snapshot['last_trade']['price'] if stock_snapshot else 450  # fallback
    print(f"   Current {ticker} price: ${current_price:.2f}")
    
    # Fetch options expiring in next 30-60 days, near the money
    min_strike = current_price * 0.95
    max_strike = current_price * 1.05
    
    options = options_fetcher.fetch_options_chain(
        underlying_ticker=ticker,
        strike_price_gte=min_strike,
        strike_price_lte=max_strike,
        limit=1000
    )
    
    print(f"   Found {len(options)} near-the-money options")
    
    # Group by expiration
    expirations = {}
    for opt in options:
        exp_date = opt.expiration_date
        if exp_date not in expirations:
            expirations[exp_date] = {'calls': [], 'puts': []}
        expirations[exp_date][f"{opt.contract_type}s"].append(opt)
    
    print("\n   Options by expiration:")
    for exp_date in sorted(expirations.keys())[:5]:  # Show first 5 expirations
        calls = len(expirations[exp_date]['calls'])
        puts = len(expirations[exp_date]['puts'])
        print(f"   {exp_date}: {calls} calls, {puts} puts")
    
    # Example 2: Fetch historical options data for specific contracts
    print("\n2. Fetching historical data for ATM options...")
    
    # Find ATM call and put
    atm_call = None
    atm_put = None
    
    for opt in options:
        if opt.days_to_expiration < 30:  # Skip very near-term
            continue
        if opt.contract_type == 'call' and not atm_call:
            if abs(opt.strike_price - current_price) < 1:
                atm_call = opt
        elif opt.contract_type == 'put' and not atm_put:
            if abs(opt.strike_price - current_price) < 1:
                atm_put = opt
        
        if atm_call and atm_put:
            break
    
    if atm_call:
        print(f"\n   ATM Call: {atm_call}")
        call_data = options_fetcher.fetch_contract_bars(
            contract_ticker=atm_call.ticker,
            start_date=analysis_date - timedelta(days=lookback_days),
            end_date=analysis_date,
            timeframe='day'
        )
        
        if not call_data.empty:
            print(f"   Retrieved {len(call_data)} days of data")
            print(f"   Last close: ${call_data['close'].iloc[-1]:.2f}")
            print(f"   30-day high: ${call_data['close'].max():.2f}")
            print(f"   30-day low: ${call_data['close'].min():.2f}")
    
    if atm_put:
        print(f"\n   ATM Put: {atm_put}")
        put_data = options_fetcher.fetch_contract_bars(
            contract_ticker=atm_put.ticker,
            start_date=analysis_date - timedelta(days=lookback_days),
            end_date=analysis_date,
            timeframe='day'
        )
        
        if not put_data.empty:
            print(f"   Retrieved {len(put_data)} days of data")
            print(f"   Last close: ${put_data['close'].iloc[-1]:.2f}")
    
    # Example 3: Analyze options for a specific event/date range
    print("\n3. Fetching all options that existed in the last 30 days...")
    
    contracts_dict = options_fetcher.fetch_contracts_in_range(
        underlying_ticker=ticker,
        start_date=analysis_date - timedelta(days=30),
        end_date=analysis_date,
        contract_type='both',
        min_strike=current_price * 0.9,
        max_strike=current_price * 1.1,
        min_days_to_expiry=7
    )
    
    print(f"   Found {len(contracts_dict.get('calls', []))} unique call contracts")
    print(f"   Found {len(contracts_dict.get('puts', []))} unique put contracts")
    
    # Example 4: Fetch and combine multiple contracts data
    print("\n4. Analyzing multiple strikes for the same expiration...")
    
    # Find options with same expiration
    if expirations:
        target_exp = sorted(expirations.keys())[0]  # Use nearest expiration
        exp_calls = expirations[target_exp]['calls']
        
        if len(exp_calls) >= 3:
            # Select 3 strikes
            selected_calls = sorted(exp_calls, key=lambda x: x.strike_price)[:3]
            
            multi_data = options_fetcher.fetch_multiple_contracts_bars(
                contracts=selected_calls,
                start_date=analysis_date - timedelta(days=7),
                end_date=analysis_date,
                timeframe='day',
                include_underlying=True,
                underlying_ticker=ticker
            )
            
            if not multi_data.empty:
                print(f"\n   Expiration: {target_exp}")
                print("   Strike prices and last prices:")
                
                for call in selected_calls:
                    if (call, 'close') in multi_data.columns:
                        last_price = multi_data[(call, 'close')].iloc[-1]
                        print(f"   Strike ${call.strike_price}: ${last_price:.2f}")
    
    # Example 5: Simple implied volatility analysis (placeholder)
    print("\n5. Options analytics...")
    
    if atm_call and not call_data.empty:
        # Calculate simple metrics
        call_returns = call_data['close'].pct_change().dropna()
        
        print(f"\n   ATM Call analytics:")
        print(f"   - 30-day historical volatility: {call_returns.std() * (252**0.5) * 100:.1f}%")
        print(f"   - Average daily volume: {call_data['volume'].mean():,.0f}")
        
        # Put-Call ratio analysis
        total_call_volume = sum(1 for opt in options if opt.contract_type == 'call')
        total_put_volume = sum(1 for opt in options if opt.contract_type == 'put')
        pc_ratio = total_put_volume / total_call_volume if total_call_volume > 0 else 0
        
        print(f"\n   Put-Call ratio: {pc_ratio:.2f}")
        print(f"   Market sentiment: {'Bearish' if pc_ratio > 1.2 else 'Bullish' if pc_ratio < 0.8 else 'Neutral'}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure you have:")
        print("1. Created config/config.ini from config/config.ini.example")
        print("2. Added your Polygon API key to the config file")