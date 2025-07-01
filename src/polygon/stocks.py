"""
Stock data fetching module for Polygon.io API.
Provides functions for fetching historical bars, quotes, trades, and snapshots.
"""

from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Union, Literal
import pandas as pd
from loguru import logger

from .base import PolygonBase


class StockDataFetcher(PolygonBase):
    """Fetch stock market data from Polygon.io."""
    
    @PolygonBase.rate_limiter
    def fetch_bars(
        self,
        ticker: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        timeframe: Literal['minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'] = 'day',
        multiplier: int = 1,
        adjusted: bool = True,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Fetch aggregated bar data (OHLCV) for a stock.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date for data
            end_date: End date for data
            timeframe: Bar timeframe
            multiplier: Size of the timespan multiplier
            adjusted: Whether to adjust for splits and dividends
            limit: Maximum number of bars to return
            
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume, vwap, transactions
        """
        # Convert dates to datetime objects
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        logger.info(f"Fetching {timeframe} bars for {ticker} from {start_dt.date()} to {end_dt.date()}")
        
        # Use caching decorator
        @self.with_cache('stock')
        def _fetch_bars():
            bars = []
            
            for bar in self.client.list_aggs(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timeframe,
                from_=start_dt,
                to=end_dt,
                adjusted=adjusted,
                sort='asc',
                limit=50000 if limit is None else limit
            ):
                bars.append(bar)
                
            if not bars:
                logger.warning(f"No bar data found for {ticker}")
                return pd.DataFrame()
                
            # Convert to DataFrame
            df = pd.DataFrame(bars)
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            
            # Set timestamp as index
            df.set_index('timestamp', inplace=True)
            
            # Drop unnecessary columns if present
            columns_to_drop = ['otc'] if 'otc' in df.columns else []
            if columns_to_drop:
                df.drop(columns=columns_to_drop, inplace=True)
                
            # Rename columns for consistency
            column_mapping = {
                'o': 'open',
                'h': 'high',
                'l': 'low',
                'c': 'close',
                'v': 'volume',
                'vw': 'vwap',
                'n': 'transactions'
            }
            df.rename(columns=column_mapping, inplace=True)
            
            logger.info(f"Fetched {len(df)} bars for {ticker}")
            
            return df
            
        return _fetch_bars()
        
    @PolygonBase.rate_limiter
    def fetch_quotes(
        self,
        ticker: str,
        date: Union[str, date, datetime],
        timestamp_gte: Optional[datetime] = None,
        timestamp_lte: Optional[datetime] = None,
        limit: int = 50000
    ) -> pd.DataFrame:
        """
        Fetch quote data (bid/ask) for a stock on a specific date.
        
        Args:
            ticker: Stock ticker symbol
            date: Date to fetch quotes for
            timestamp_gte: Filter for quotes after this time
            timestamp_lte: Filter for quotes before this time
            limit: Maximum number of quotes to return
            
        Returns:
            DataFrame with bid/ask prices and sizes
        """
        date_obj = pd.to_datetime(date).date()
        
        logger.info(f"Fetching quotes for {ticker} on {date_obj}")
        
        quotes = []
        
        for quote in self.client.list_quotes(
            ticker=ticker,
            timestamp_gte=timestamp_gte,
            timestamp_lte=timestamp_lte,
            order='asc',
            limit=limit
        ):
            quotes.append(quote)
            
        if not quotes:
            logger.warning(f"No quote data found for {ticker} on {date_obj}")
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame(quotes)
        
        # Convert timestamps
        timestamp_cols = ['sip_timestamp', 'participant_timestamp', 'trf_timestamp']
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], unit='ns', utc=True)
                
        # Set primary timestamp as index
        if 'sip_timestamp' in df.columns:
            df.set_index('sip_timestamp', inplace=True)
            
        logger.info(f"Fetched {len(df)} quotes for {ticker}")
        
        return df
        
    @PolygonBase.rate_limiter
    def fetch_trades(
        self,
        ticker: str,
        date: Union[str, date, datetime],
        timestamp_gte: Optional[datetime] = None,
        timestamp_lte: Optional[datetime] = None,
        limit: int = 50000
    ) -> pd.DataFrame:
        """
        Fetch trade data for a stock on a specific date.
        
        Args:
            ticker: Stock ticker symbol
            date: Date to fetch trades for
            timestamp_gte: Filter for trades after this time
            timestamp_lte: Filter for trades before this time
            limit: Maximum number of trades to return
            
        Returns:
            DataFrame with trade prices, sizes, and conditions
        """
        date_obj = pd.to_datetime(date).date()
        
        logger.info(f"Fetching trades for {ticker} on {date_obj}")
        
        trades = []
        
        for trade in self.client.list_trades(
            ticker=ticker,
            timestamp_gte=timestamp_gte,
            timestamp_lte=timestamp_lte,
            order='asc',
            limit=limit
        ):
            trades.append(trade)
            
        if not trades:
            logger.warning(f"No trade data found for {ticker} on {date_obj}")
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame(trades)
        
        # Convert timestamps
        timestamp_cols = ['sip_timestamp', 'participant_timestamp', 'trf_timestamp']
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], unit='ns', utc=True)
                
        # Set primary timestamp as index
        if 'sip_timestamp' in df.columns:
            df.set_index('sip_timestamp', inplace=True)
            
        logger.info(f"Fetched {len(df)} trades for {ticker}")
        
        return df
        
    @PolygonBase.rate_limiter
    def fetch_snapshot(self, ticker: str) -> Dict[str, Any]:
        """
        Fetch the latest snapshot data for a stock.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary containing latest price, volume, and other snapshot data
        """
        logger.info(f"Fetching snapshot for {ticker}")
        
        try:
            snapshot = self.client.get_snapshot_ticker(ticker)
            
            # Convert to dictionary format
            result = {
                'ticker': ticker,
                'day': {
                    'open': snapshot.day.open,
                    'high': snapshot.day.high,
                    'low': snapshot.day.low,
                    'close': snapshot.day.close,
                    'volume': snapshot.day.volume,
                    'vwap': snapshot.day.vwap
                } if snapshot.day else None,
                'last_quote': {
                    'bid': snapshot.last_quote.bid_price if snapshot.last_quote else None,
                    'ask': snapshot.last_quote.ask_price if snapshot.last_quote else None,
                    'bid_size': snapshot.last_quote.bid_size if snapshot.last_quote else None,
                    'ask_size': snapshot.last_quote.ask_size if snapshot.last_quote else None,
                } if snapshot.last_quote else None,
                'last_trade': {
                    'price': snapshot.last_trade.price if snapshot.last_trade else None,
                    'size': snapshot.last_trade.size if snapshot.last_trade else None,
                } if snapshot.last_trade else None,
                'updated': datetime.now()
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching snapshot for {ticker}: {e}")
            raise
            
    def fetch_multiple_bars(
        self,
        tickers: List[str],
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        timeframe: Literal['minute', 'hour', 'day', 'week', 'month'] = 'day',
        adjusted: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch bar data for multiple tickers.
        
        Args:
            tickers: List of stock ticker symbols
            start_date: Start date for data
            end_date: End date for data
            timeframe: Bar timeframe
            adjusted: Whether to adjust for splits and dividends
            
        Returns:
            Dictionary mapping ticker to DataFrame
        """
        results = {}
        
        for ticker in tickers:
            try:
                df = self.fetch_bars(
                    ticker=ticker,
                    start_date=start_date,
                    end_date=end_date,
                    timeframe=timeframe,
                    adjusted=adjusted
                )
                results[ticker] = df
            except Exception as e:
                logger.error(f"Error fetching data for {ticker}: {e}")
                results[ticker] = pd.DataFrame()
                
        return results
        
    def save_to_parquet(self, df: pd.DataFrame, ticker: str, data_type: str = "bars"):
        """
        Save DataFrame to parquet file.
        
        Args:
            df: DataFrame to save
            ticker: Stock ticker symbol
            data_type: Type of data (bars, quotes, trades)
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, not saving {ticker} {data_type}")
            return
            
        # Create filename with date range
        start_date = df.index.min().strftime('%Y%m%d')
        end_date = df.index.max().strftime('%Y%m%d')
        
        filename = f"{ticker}_{data_type}_{start_date}_{end_date}.parquet"
        filepath = self.data_dir / "stocks" / filename
        
        # Create directory if needed
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to parquet
        df.to_parquet(filepath, compression='snappy')
        
        logger.info(f"Saved {ticker} {data_type} to {filepath}")
        
    def load_from_parquet(self, ticker: str, data_type: str = "bars") -> Optional[pd.DataFrame]:
        """
        Load the most recent parquet file for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            data_type: Type of data (bars, quotes, trades)
            
        Returns:
            DataFrame if file exists, None otherwise
        """
        pattern = f"{ticker}_{data_type}_*.parquet"
        files = list((self.data_dir / "stocks").glob(pattern))
        
        if not files:
            return None
            
        # Get the most recent file
        latest_file = max(files, key=lambda x: x.stat().st_mtime)
        
        logger.info(f"Loading {ticker} {data_type} from {latest_file}")
        
        return pd.read_parquet(latest_file)