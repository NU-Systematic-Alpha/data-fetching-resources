"""
Options data fetching module for Polygon.io API.
Provides functions for fetching options chains, contracts, and historical data.
"""

from datetime import datetime, date, timedelta, time
from typing import Optional, List, Dict, Any, Union, Literal, Tuple
from zoneinfo import ZoneInfo
import pandas as pd
from loguru import logger

from .base import PolygonBase
from ..common import Option


class OptionsDataFetcher(PolygonBase):
    """Fetch options market data from Polygon.io."""
    
    @PolygonBase.rate_limiter
    def fetch_options_chain(
        self,
        underlying_ticker: str,
        expiration_date: Optional[Union[str, date]] = None,
        contract_type: Optional[Literal['call', 'put']] = None,
        strike_price_gte: Optional[float] = None,
        strike_price_lte: Optional[float] = None,
        as_of_date: Optional[Union[str, date]] = None,
        expired: bool = False,
        limit: int = 1000
    ) -> List[Option]:
        """
        Fetch options chain for an underlying ticker.
        
        Args:
            underlying_ticker: The underlying stock ticker
            expiration_date: Filter for specific expiration date
            contract_type: Filter for calls or puts only
            strike_price_gte: Minimum strike price
            strike_price_lte: Maximum strike price
            as_of_date: Historical options chain as of this date
            expired: Include expired contracts
            limit: Maximum number of contracts to return
            
        Returns:
            List of Option objects
        """
        logger.info(f"Fetching options chain for {underlying_ticker}")
        
        # Convert dates if provided as strings
        if expiration_date and isinstance(expiration_date, str):
            expiration_date = pd.to_datetime(expiration_date).date()
        if as_of_date and isinstance(as_of_date, str):
            as_of_date = pd.to_datetime(as_of_date).date()
            
        contracts = []
        
        kwargs = {
            'underlying_ticker': underlying_ticker,
            'limit': limit,
            'order': 'asc',
            'sort': 'expiration_date'
        }
        
        # Add optional filters
        if expiration_date:
            kwargs['expiration_date'] = expiration_date
        if contract_type:
            kwargs['contract_type'] = contract_type
        if strike_price_gte is not None:
            kwargs['strike_price_gte'] = strike_price_gte
        if strike_price_lte is not None:
            kwargs['strike_price_lte'] = strike_price_lte
        if as_of_date:
            kwargs['as_of'] = as_of_date
        if expired:
            kwargs['expired'] = expired
            
        for contract in self.client.list_options_contracts(**kwargs):
            contracts.append(Option.from_polygon_contract(contract))
            
        logger.info(f"Found {len(contracts)} contracts for {underlying_ticker}")
        
        return contracts
        
    @PolygonBase.rate_limiter
    def fetch_contracts_in_range(
        self,
        underlying_ticker: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        contract_type: Literal['call', 'put', 'both'] = 'both',
        min_strike: Optional[float] = None,
        max_strike: Optional[float] = None,
        min_days_to_expiry: int = 0
    ) -> Dict[str, List[Option]]:
        """
        Fetch all unique options contracts that existed during a date range.
        
        Args:
            underlying_ticker: The underlying stock ticker
            start_date: Start of the date range
            end_date: End of the date range
            contract_type: Type of contracts to fetch
            min_strike: Minimum strike price
            max_strike: Maximum strike price
            min_days_to_expiry: Minimum days until expiration
            
        Returns:
            Dictionary with 'calls' and/or 'puts' keys containing lists of Option objects
        """
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        logger.info(
            f"Fetching {contract_type} contracts for {underlying_ticker} "
            f"from {start_dt.date()} to {end_dt.date()}"
        )
        
        results = {}
        contract_types = []
        
        if contract_type in ['call', 'both']:
            contract_types.append('call')
        if contract_type in ['put', 'both']:
            contract_types.append('put')
            
        for ctype in contract_types:
            all_contracts = {}
            current_date = start_dt
            
            # Iterate through the date range
            while current_date <= end_dt and current_date <= datetime.now(ZoneInfo('UTC')):
                # Calculate minimum expiration date
                min_expiry = current_date.date() + timedelta(days=min_days_to_expiry)
                
                # Fetch contracts for this date
                contracts = self.fetch_options_chain(
                    underlying_ticker=underlying_ticker,
                    contract_type=ctype,
                    strike_price_gte=min_strike,
                    strike_price_lte=max_strike,
                    as_of_date=current_date.date(),
                    expired=False
                )
                
                # Filter by expiration date and add unique contracts
                for contract in contracts:
                    if contract.expiration_date >= min_expiry:
                        all_contracts[contract.ticker] = contract
                        
                # Move to next day
                current_date += timedelta(days=1)
                
            results[f"{ctype}s"] = list(all_contracts.values())
            logger.info(f"Found {len(all_contracts)} unique {ctype} contracts")
            
        return results
        
    @PolygonBase.rate_limiter
    def fetch_contract_bars(
        self,
        contract_ticker: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        timeframe: Literal['minute', 'hour', 'day', 'week', 'month'] = 'day',
        multiplier: int = 1,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Fetch historical bar data for a specific options contract.
        
        Args:
            contract_ticker: The options contract ticker
            start_date: Start date for data
            end_date: End date for data
            timeframe: Bar timeframe
            multiplier: Size of the timespan multiplier
            limit: Maximum number of bars to return
            
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume, vwap
        """
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        logger.info(
            f"Fetching {timeframe} bars for {contract_ticker} "
            f"from {start_dt.date()} to {end_dt.date()}"
        )
        
        bars = []
        
        # Ensure we don't query future data
        end_dt = min(end_dt, datetime.now(ZoneInfo('UTC')))
        
        for bar in self.client.list_aggs(
            ticker=contract_ticker,
            multiplier=multiplier,
            timespan=timeframe,
            from_=start_dt,
            to=end_dt,
            adjusted=True,
            sort='asc',
            limit=50000 if limit is None else limit
        ):
            bars.append(bar)
            
        if not bars:
            logger.warning(f"No bar data found for {contract_ticker}")
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame(bars)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('timestamp', inplace=True)
        
        # Drop unnecessary columns if present
        if 'otc' in df.columns:
            df.drop(columns=['otc'], inplace=True)
            
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
        
        logger.info(f"Fetched {len(df)} bars for {contract_ticker}")
        
        return df
        
    def fetch_multiple_contracts_bars(
        self,
        contracts: List[Union[str, Option]],
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        timeframe: Literal['day', 'hour', 'minute'] = 'day',
        include_underlying: bool = True,
        underlying_ticker: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Fetch bar data for multiple options contracts and combine into a single DataFrame.
        
        Args:
            contracts: List of contract tickers or Option objects
            start_date: Start date for data
            end_date: End date for data
            timeframe: Bar timeframe
            include_underlying: Include underlying stock data
            underlying_ticker: Underlying ticker (required if include_underlying=True)
            
        Returns:
            DataFrame with MultiIndex columns (Option/ticker, data_field)
        """
        all_dfs = []
        
        # Fetch data for each contract
        for contract in contracts:
            ticker = contract.ticker if isinstance(contract, Option) else contract
            
            df = self.fetch_contract_bars(
                contract_ticker=ticker,
                start_date=start_date,
                end_date=end_date,
                timeframe=timeframe
            )
            
            if not df.empty:
                # Create Option object if we have a string ticker
                if isinstance(contract, str):
                    contract = Option.from_ticker(contract)
                    
                # Create MultiIndex columns
                df.columns = pd.MultiIndex.from_tuples(
                    [(contract, col) for col in df.columns]
                )
                
                all_dfs.append(df)
                
        if not all_dfs:
            logger.warning("No data found for any contracts")
            return pd.DataFrame()
            
        # Combine all DataFrames
        result = pd.concat(all_dfs, axis=1)
        
        # Add underlying stock data if requested
        if include_underlying and underlying_ticker:
            from .stocks import StockDataFetcher
            stock_fetcher = StockDataFetcher()
            
            stock_df = stock_fetcher.fetch_bars(
                ticker=underlying_ticker,
                start_date=start_date,
                end_date=end_date,
                timeframe=timeframe
            )
            
            if not stock_df.empty:
                # Reindex to match options data
                stock_df = stock_df.reindex(result.index)
                
                # Add to result with special key
                for col in stock_df.columns:
                    result[(underlying_ticker, f'stock_{col}')] = stock_df[col]
                    
        return result
        
    def calculate_implied_volatility(
        self,
        contract_data: pd.DataFrame,
        underlying_price: Union[float, pd.Series],
        risk_free_rate: float = 0.05,
        dividend_yield: float = 0.0
    ) -> pd.Series:
        """
        Calculate implied volatility for options contracts.
        
        Args:
            contract_data: DataFrame with contract prices
            underlying_price: Current price of underlying
            risk_free_rate: Risk-free interest rate
            dividend_yield: Dividend yield of underlying
            
        Returns:
            Series of implied volatility values
        """
        # This is a placeholder - actual IV calculation would require
        # Black-Scholes or other pricing models
        logger.warning("IV calculation not yet implemented - returning placeholder")
        return pd.Series(index=contract_data.index, data=0.3)
        
    def save_options_data(
        self,
        data: pd.DataFrame,
        underlying_ticker: str,
        data_description: str
    ):
        """
        Save options data to parquet file.
        
        Args:
            data: DataFrame to save
            underlying_ticker: Underlying ticker symbol
            data_description: Description for filename
        """
        if data.empty:
            logger.warning("Empty DataFrame, not saving")
            return
            
        # Create filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{underlying_ticker}_options_{data_description}_{timestamp}.parquet"
        filepath = self.data_dir / "options" / filename
        
        # Create directory if needed
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to parquet
        data.to_parquet(filepath, compression='snappy')
        
        logger.info(f"Saved options data to {filepath}")