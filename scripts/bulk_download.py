#!/usr/bin/env python3
"""
Bulk data downloader for fetching large amounts of historical data
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
from datetime import datetime, timedelta
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.polygon import StockDataFetcher, OptionsDataFetcher, TreasuryDataFetcher
from loguru import logger

# Configure logger
logger.add("logs/bulk_download_{time}.log", rotation="100 MB")


def download_stock_data(ticker, start_date, end_date, fetcher):
    """Download stock data for a single ticker."""
    try:
        logger.info(f"Downloading stock data for {ticker}")
        data = fetcher.fetch_bars(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            timeframe='day',
            adjusted=True
        )
        
        if not data.empty:
            fetcher.save_to_parquet(data, ticker, 'daily')
            logger.success(f"Downloaded {len(data)} days for {ticker}")
            return ticker, len(data)
        else:
            logger.warning(f"No data found for {ticker}")
            return ticker, 0
            
    except Exception as e:
        logger.error(f"Error downloading {ticker}: {e}")
        return ticker, -1


def download_options_data(ticker, start_date, end_date, fetcher):
    """Download options chain data for a single ticker."""
    try:
        logger.info(f"Downloading options data for {ticker}")
        
        # Get current options chain
        options = fetcher.fetch_options_chain(
            underlying_ticker=ticker,
            limit=1000
        )
        
        if options:
            # Group by expiration
            expirations = {}
            for opt in options:
                exp = opt.expiration_date
                if exp not in expirations:
                    expirations[exp] = []
                expirations[exp].append(opt)
            
            logger.info(f"Found {len(options)} contracts across {len(expirations)} expirations for {ticker}")
            
            # Save summary
            summary_data = {
                'ticker': ticker,
                'total_contracts': len(options),
                'expirations': len(expirations),
                'download_date': datetime.now()
            }
            
            return ticker, len(options)
        else:
            logger.warning(f"No options found for {ticker}")
            return ticker, 0
            
    except Exception as e:
        logger.error(f"Error downloading options for {ticker}: {e}")
        return ticker, -1


def bulk_download_stocks(tickers, start_date, end_date, max_workers=5):
    """Download stock data for multiple tickers in parallel."""
    fetcher = StockDataFetcher()
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_stock_data, ticker, start_date, end_date, fetcher): ticker 
            for ticker in tickers
        }
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            
            # Respect rate limits
            time.sleep(0.5)
    
    return results


def bulk_download_treasuries(start_date, end_date):
    """Download all treasury yield data."""
    fetcher = TreasuryDataFetcher()
    maturities = ['1M', '3M', '6M', '1Y', '2Y', '3Y', '5Y', '7Y', '10Y', '20Y', '30Y']
    
    all_data = {}
    
    for maturity in maturities:
        try:
            logger.info(f"Downloading {maturity} treasury yields")
            data = fetcher.fetch_treasury_yield(
                maturity=maturity,
                start_date=start_date,
                end_date=end_date
            )
            
            if not data.empty:
                all_data[maturity] = data
                logger.success(f"Downloaded {len(data)} days for {maturity} treasury")
            
            # Respect rate limits
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error downloading {maturity} treasury: {e}")
    
    # Save combined data
    if all_data:
        combined = pd.DataFrame({k: v['value'] for k, v in all_data.items()})
        fetcher.save_yield_data(combined, 'all_maturities')
        logger.success(f"Saved treasury data with {len(combined)} days")
    
    return len(all_data)


def main():
    parser = argparse.ArgumentParser(description='Bulk download financial data')
    parser.add_argument('--type', choices=['stocks', 'options', 'treasuries', 'all'], 
                        default='stocks', help='Type of data to download')
    parser.add_argument('--tickers', nargs='+', help='List of tickers (for stocks/options)')
    parser.add_argument('--file', help='File containing ticker list (one per line)')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=365, help='Number of days to look back')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel workers')
    
    args = parser.parse_args()
    
    # Determine date range
    if args.end:
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
    else:
        end_date = datetime.now()
    
    if args.start:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
    else:
        start_date = end_date - timedelta(days=args.days)
    
    logger.info(f"Downloading data from {start_date.date()} to {end_date.date()}")
    
    # Get ticker list
    tickers = []
    if args.tickers:
        tickers = args.tickers
    elif args.file:
        with open(args.file, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
    elif args.type in ['stocks', 'options']:
        # Default tickers if none provided
        tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'JNJ']
    
    # Download based on type
    if args.type == 'stocks':
        logger.info(f"Downloading stock data for {len(tickers)} tickers")
        results = bulk_download_stocks(tickers, start_date, end_date, args.workers)
        
        # Summary
        successful = sum(1 for _, count in results if count > 0)
        failed = sum(1 for _, count in results if count < 0)
        logger.info(f"Downloaded data for {successful} tickers, {failed} failed")
        
    elif args.type == 'options':
        logger.info(f"Downloading options data for {len(tickers)} tickers")
        fetcher = OptionsDataFetcher()
        
        for ticker in tickers:
            download_options_data(ticker, start_date, end_date, fetcher)
            time.sleep(1)  # Respect rate limits
            
    elif args.type == 'treasuries':
        logger.info("Downloading all treasury yields")
        count = bulk_download_treasuries(start_date, end_date)
        logger.info(f"Downloaded {count} treasury maturities")
        
    elif args.type == 'all':
        # Download everything
        logger.info("Downloading all data types")
        
        # Stocks
        if tickers:
            logger.info(f"Downloading stocks...")
            bulk_download_stocks(tickers, start_date, end_date, args.workers)
        
        # Treasuries
        logger.info("Downloading treasuries...")
        bulk_download_treasuries(start_date, end_date)
        
        # Options (limited to avoid rate limits)
        if tickers[:5]:  # Only first 5 to avoid rate limits
            logger.info("Downloading options for first 5 tickers...")
            fetcher = OptionsDataFetcher()
            for ticker in tickers[:5]:
                download_options_data(ticker, start_date, end_date, fetcher)
                time.sleep(2)
    
    logger.success("Bulk download completed!")


if __name__ == "__main__":
    main()