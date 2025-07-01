#!/usr/bin/env python3
"""
Example: Fetching treasury yield curve data using Polygon API
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from src.polygon import TreasuryDataFetcher

def main():
    # Initialize the treasury data fetcher
    fetcher = TreasuryDataFetcher()
    
    # Define parameters
    analysis_date = datetime.now()
    lookback_days = 90
    
    print("Treasury Yield Curve Analysis")
    print(f"Analysis date: {analysis_date.date()}")
    print("-" * 50)
    
    # Example 1: Fetch current yield curve
    print("\n1. Fetching current yield curve...")
    
    yield_curve = fetcher.fetch_yield_curve(analysis_date)
    
    if not yield_curve.empty:
        print("\n   Current Treasury Yields:")
        for maturity, row in yield_curve.iterrows():
            print(f"   {maturity:>4}: {row['yield']:>6.3f}%")
        
        # Plot yield curve
        try:
            plt.figure(figsize=(10, 6))
            plt.plot(yield_curve['years'], yield_curve['yield'], 'bo-', linewidth=2, markersize=8)
            plt.xlabel('Maturity (Years)')
            plt.ylabel('Yield (%)')
            plt.title(f'US Treasury Yield Curve - {analysis_date.date()}')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig('data/yield_curve.png')
            print("\n   Yield curve plot saved to data/yield_curve.png")
        except Exception as e:
            print(f"   Could not create plot: {e}")
    
    # Example 2: Fetch historical yields for specific maturities
    print("\n2. Fetching historical yields...")
    
    # Focus on key maturities
    key_maturities = ['3M', '2Y', '10Y', '30Y']
    
    for maturity in key_maturities:
        try:
            yield_data = fetcher.fetch_treasury_yield(
                maturity=maturity,
                start_date=analysis_date - timedelta(days=lookback_days),
                end_date=analysis_date,
                timeframe='day'
            )
            
            if not yield_data.empty:
                current_yield = yield_data['value'].iloc[-1]
                avg_yield = yield_data['value'].mean()
                min_yield = yield_data['value'].min()
                max_yield = yield_data['value'].max()
                
                print(f"\n   {maturity} Treasury:")
                print(f"   - Current: {current_yield:.3f}%")
                print(f"   - 90-day avg: {avg_yield:.3f}%")
                print(f"   - 90-day range: {min_yield:.3f}% - {max_yield:.3f}%")
                
        except Exception as e:
            print(f"   Error fetching {maturity}: {e}")
    
    # Example 3: Calculate yield spreads
    print("\n3. Calculating yield spreads...")
    
    # Fetch yield curve history
    yield_history = fetcher.fetch_yield_curve_history(
        start_date=analysis_date - timedelta(days=lookback_days),
        end_date=analysis_date,
        maturities=['3M', '2Y', '5Y', '10Y', '30Y'],
        timeframe='day'
    )
    
    if not yield_history.empty:
        # Calculate spreads
        spreads = fetcher.calculate_yield_spreads(yield_history)
        
        print("\n   Current yield spreads:")
        for spread_name, values in spreads.items():
            if not values.empty:
                current_spread = values.iloc[-1]
                avg_spread = values.mean()
                print(f"   {spread_name}: {current_spread:>6.3f}% (90-day avg: {avg_spread:.3f}%)")
        
        # Check for yield curve inversion
        if '10Y-2Y' in spreads.columns:
            ten_two_spread = spreads['10Y-2Y'].iloc[-1]
            if ten_two_spread < 0:
                print(f"\n   ⚠️  YIELD CURVE INVERSION: 10Y-2Y spread is {ten_two_spread:.3f}%")
                
                # Count days of inversion
                inversion_days = (spreads['10Y-2Y'] < 0).sum()
                print(f"   Inverted for {inversion_days} out of last {len(spreads)} days")
    
    # Example 4: Compare real vs nominal yields
    print("\n4. Fetching other yield indices...")
    
    # Fetch Fed Funds rate
    try:
        fed_funds = fetcher.fetch_other_yields(
            yield_type='FED_FUNDS',
            start_date=analysis_date - timedelta(days=30),
            end_date=analysis_date,
            timeframe='day'
        )
        
        if not fed_funds.empty:
            current_ff = fed_funds['value'].iloc[-1]
            print(f"\n   Federal Funds Rate: {current_ff:.3f}%")
    except Exception as e:
        print(f"   Could not fetch Fed Funds rate: {e}")
    
    # Fetch SOFR
    try:
        sofr = fetcher.fetch_other_yields(
            yield_type='SOFR',
            start_date=analysis_date - timedelta(days=30),
            end_date=analysis_date,
            timeframe='day'
        )
        
        if not sofr.empty:
            current_sofr = sofr['value'].iloc[-1]
            print(f"   SOFR: {current_sofr:.3f}%")
    except Exception as e:
        print(f"   Could not fetch SOFR: {e}")
    
    # Example 5: Yield curve dynamics over time
    print("\n5. Analyzing yield curve dynamics...")
    
    if not yield_history.empty:
        # Calculate curve steepness over time
        if '30Y' in yield_history.columns and '2Y' in yield_history.columns:
            curve_steepness = yield_history['30Y'] - yield_history['2Y']
            
            print(f"\n   Yield curve steepness (30Y-2Y):")
            print(f"   - Current: {curve_steepness.iloc[-1]:.3f}%")
            print(f"   - 90-day average: {curve_steepness.mean():.3f}%")
            print(f"   - Trend: {'Steepening' if curve_steepness.iloc[-1] > curve_steepness.iloc[-30] else 'Flattening'}")
        
        # Calculate volatility of yields
        print("\n   Yield volatility (annualized):")
        for maturity in ['2Y', '10Y']:
            if maturity in yield_history.columns:
                returns = yield_history[maturity].pct_change().dropna()
                vol = returns.std() * (252**0.5) * 100
                print(f"   - {maturity}: {vol:.1f}%")
        
        # Save the data
        fetcher.save_yield_data(yield_history, "90day_history")
        print("\n   Data saved to data/treasuries/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure you have:")
        print("1. Created config/config.ini from config/config.ini.example")
        print("2. Added your Polygon API key to the config file")