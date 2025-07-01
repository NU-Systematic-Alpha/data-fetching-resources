"""
Treasury and yield data fetching module for Polygon.io API.
Provides functions for fetching treasury yields, yield curves, and fixed income data.
"""

from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Union, Literal
import pandas as pd
import numpy as np
from loguru import logger

from .base import PolygonBase


# Treasury ticker mappings for Polygon
TREASURY_TICKERS = {
    '1M': 'I:DGS1MO',   # 1-Month Treasury Rate
    '3M': 'I:DGS3MO',   # 3-Month Treasury Rate
    '6M': 'I:DGS6MO',   # 6-Month Treasury Rate
    '1Y': 'I:DGS1',     # 1-Year Treasury Rate
    '2Y': 'I:DGS2',     # 2-Year Treasury Rate
    '3Y': 'I:DGS3',     # 3-Year Treasury Rate
    '5Y': 'I:DGS5',     # 5-Year Treasury Rate
    '7Y': 'I:DGS7',     # 7-Year Treasury Rate
    '10Y': 'I:DGS10',   # 10-Year Treasury Rate
    '20Y': 'I:DGS20',   # 20-Year Treasury Rate
    '30Y': 'I:DGS30',   # 30-Year Treasury Rate
}

# Additional yield indices
YIELD_INDICES = {
    'REAL_10Y': 'I:DFII10',     # 10-Year Treasury Inflation-Indexed
    'TIPS_5Y': 'I:DFII5',       # 5-Year Treasury Inflation-Indexed
    'FED_FUNDS': 'I:DFF',       # Federal Funds Rate
    'SOFR': 'I:SOFR',           # Secured Overnight Financing Rate
    'PRIME': 'I:DPRIME',        # Bank Prime Loan Rate
}


class TreasuryDataFetcher(PolygonBase):
    """Fetch treasury and yield curve data from Polygon.io."""
    
    @PolygonBase.rate_limiter
    def fetch_treasury_yield(
        self,
        maturity: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        timeframe: Literal['day', 'week', 'month'] = 'day'
    ) -> pd.DataFrame:
        """
        Fetch treasury yield data for a specific maturity.
        
        Args:
            maturity: Treasury maturity (e.g., '1M', '3M', '1Y', '10Y', '30Y')
            start_date: Start date for data
            end_date: End date for data
            timeframe: Data frequency
            
        Returns:
            DataFrame with columns: timestamp, value (yield percentage)
        """
        if maturity not in TREASURY_TICKERS:
            raise ValueError(
                f"Invalid maturity '{maturity}'. "
                f"Valid options: {list(TREASURY_TICKERS.keys())}"
            )
            
        ticker = TREASURY_TICKERS[maturity]
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        logger.info(
            f"Fetching {maturity} treasury yield from {start_dt.date()} to {end_dt.date()}"
        )
        
        @self.with_cache('treasury')
        def _fetch_yield():
            values = []
            
            for value in self.client.list_aggs(
                ticker=ticker,
                multiplier=1,
                timespan=timeframe,
                from_=start_dt,
                to=end_dt,
                sort='asc',
                limit=50000
            ):
                values.append({
                    'timestamp': pd.to_datetime(value.timestamp, unit='ms', utc=True),
                    'value': value.close,  # Close price represents the yield
                    'open': value.open,
                    'high': value.high,
                    'low': value.low
                })
                
            if not values:
                logger.warning(f"No yield data found for {maturity}")
                return pd.DataFrame()
                
            df = pd.DataFrame(values)
            df.set_index('timestamp', inplace=True)
            
            logger.info(f"Fetched {len(df)} data points for {maturity} treasury")
            
            return df
            
        return _fetch_yield()
        
    def fetch_yield_curve(
        self,
        date: Union[str, date, datetime],
        maturities: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Fetch the full yield curve for a specific date.
        
        Args:
            date: Date to fetch yield curve for
            maturities: List of maturities to include (default: all)
            
        Returns:
            DataFrame with maturities as index and yields as values
        """
        target_date = pd.to_datetime(date)
        
        if maturities is None:
            maturities = list(TREASURY_TICKERS.keys())
            
        logger.info(f"Fetching yield curve for {target_date.date()}")
        
        curve_data = {}
        
        for maturity in maturities:
            # Fetch data for a small window around the target date
            df = self.fetch_treasury_yield(
                maturity=maturity,
                start_date=target_date - timedelta(days=5),
                end_date=target_date + timedelta(days=1),
                timeframe='day'
            )
            
            if not df.empty:
                # Find the closest date to our target
                closest_idx = df.index.get_indexer([target_date], method='nearest')[0]
                if closest_idx < len(df):
                    curve_data[maturity] = df.iloc[closest_idx]['value']
                    
        if not curve_data:
            logger.warning(f"No yield curve data found for {target_date.date()}")
            return pd.DataFrame()
            
        # Create DataFrame
        df = pd.DataFrame(
            list(curve_data.items()),
            columns=['maturity', 'yield']
        )
        
        # Add numeric maturity in years for sorting
        maturity_years = {
            '1M': 1/12, '3M': 0.25, '6M': 0.5,
            '1Y': 1, '2Y': 2, '3Y': 3, '5Y': 5,
            '7Y': 7, '10Y': 10, '20Y': 20, '30Y': 30
        }
        
        df['years'] = df['maturity'].map(maturity_years)
        df = df.sort_values('years')
        df.set_index('maturity', inplace=True)
        
        return df
        
    def fetch_yield_curve_history(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        maturities: Optional[List[str]] = None,
        timeframe: Literal['day', 'week', 'month'] = 'day'
    ) -> pd.DataFrame:
        """
        Fetch historical yield curve data over a date range.
        
        Args:
            start_date: Start date for data
            end_date: End date for data
            maturities: List of maturities to include (default: all)
            timeframe: Data frequency
            
        Returns:
            DataFrame with dates as index and maturities as columns
        """
        if maturities is None:
            maturities = list(TREASURY_TICKERS.keys())
            
        logger.info(
            f"Fetching yield curve history from "
            f"{pd.to_datetime(start_date).date()} to {pd.to_datetime(end_date).date()}"
        )
        
        all_data = {}
        
        for maturity in maturities:
            df = self.fetch_treasury_yield(
                maturity=maturity,
                start_date=start_date,
                end_date=end_date,
                timeframe=timeframe
            )
            
            if not df.empty:
                all_data[maturity] = df['value']
                
        if not all_data:
            logger.warning("No yield curve history data found")
            return pd.DataFrame()
            
        # Combine into single DataFrame
        result = pd.DataFrame(all_data)
        
        # Sort columns by maturity
        maturity_order = ['1M', '3M', '6M', '1Y', '2Y', '3Y', '5Y', '7Y', '10Y', '20Y', '30Y']
        cols = [col for col in maturity_order if col in result.columns]
        result = result[cols]
        
        return result
        
    def calculate_yield_spreads(
        self,
        yield_data: pd.DataFrame,
        spreads: Optional[List[Tuple[str, str]]] = None
    ) -> pd.DataFrame:
        """
        Calculate yield spreads between different maturities.
        
        Args:
            yield_data: DataFrame with yield data (maturities as columns)
            spreads: List of tuples (long_maturity, short_maturity) to calculate
            
        Returns:
            DataFrame with spread calculations
        """
        if spreads is None:
            # Default important spreads
            spreads = [
                ('10Y', '2Y'),   # 10-2 spread (recession indicator)
                ('10Y', '3M'),   # 10Y-3M spread
                ('30Y', '5Y'),   # 30-5 spread
                ('5Y', '2Y'),    # 5-2 spread
            ]
            
        spread_data = {}
        
        for long_mat, short_mat in spreads:
            if long_mat in yield_data.columns and short_mat in yield_data.columns:
                spread_name = f'{long_mat}-{short_mat}'
                spread_data[spread_name] = yield_data[long_mat] - yield_data[short_mat]
                
        return pd.DataFrame(spread_data)
        
    @PolygonBase.rate_limiter
    def fetch_other_yields(
        self,
        yield_type: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        timeframe: Literal['day', 'week', 'month'] = 'day'
    ) -> pd.DataFrame:
        """
        Fetch other yield indices like TIPS, Fed Funds, SOFR, etc.
        
        Args:
            yield_type: Type of yield (see YIELD_INDICES keys)
            start_date: Start date for data
            end_date: End date for data
            timeframe: Data frequency
            
        Returns:
            DataFrame with timestamp and yield values
        """
        if yield_type not in YIELD_INDICES:
            raise ValueError(
                f"Invalid yield type '{yield_type}'. "
                f"Valid options: {list(YIELD_INDICES.keys())}"
            )
            
        ticker = YIELD_INDICES[yield_type]
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        logger.info(f"Fetching {yield_type} data from {start_dt.date()} to {end_dt.date()}")
        
        values = []
        
        for value in self.client.list_aggs(
            ticker=ticker,
            multiplier=1,
            timespan=timeframe,
            from_=start_dt,
            to=end_dt,
            sort='asc',
            limit=50000
        ):
            values.append({
                'timestamp': pd.to_datetime(value.timestamp, unit='ms', utc=True),
                'value': value.close,
                'open': value.open,
                'high': value.high,
                'low': value.low
            })
            
        if not values:
            logger.warning(f"No data found for {yield_type}")
            return pd.DataFrame()
            
        df = pd.DataFrame(values)
        df.set_index('timestamp', inplace=True)
        
        return df
        
    def calculate_real_yields(
        self,
        nominal_yields: pd.DataFrame,
        inflation_expectations: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Calculate real yields from nominal yields.
        
        Args:
            nominal_yields: DataFrame with nominal yield data
            inflation_expectations: DataFrame with inflation expectations
                                   (if None, uses TIPS spreads)
            
        Returns:
            DataFrame with real yield calculations
        """
        if inflation_expectations is None:
            # Use 10Y TIPS spread as proxy for inflation expectations
            tips_10y = self.fetch_other_yields(
                'REAL_10Y',
                start_date=nominal_yields.index.min(),
                end_date=nominal_yields.index.max()
            )
            
            if '10Y' in nominal_yields.columns and not tips_10y.empty:
                # Align data
                aligned_tips = tips_10y.reindex(nominal_yields.index, method='ffill')
                inflation_exp = nominal_yields['10Y'] - aligned_tips['value']
                
                # Apply to all maturities (simplified approach)
                real_yields = nominal_yields.subtract(inflation_exp, axis=0)
                return real_yields
                
        else:
            # Use provided inflation expectations
            real_yields = nominal_yields.subtract(
                inflation_expectations['value'], axis=0
            )
            return real_yields
            
        logger.warning("Could not calculate real yields")
        return pd.DataFrame()
        
    def save_yield_data(self, data: pd.DataFrame, description: str):
        """
        Save yield data to parquet file.
        
        Args:
            data: DataFrame to save
            description: Description for filename
        """
        if data.empty:
            logger.warning("Empty DataFrame, not saving")
            return
            
        # Create filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"yields_{description}_{timestamp}.parquet"
        filepath = self.data_dir / "treasuries" / filename
        
        # Create directory if needed
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to parquet
        data.to_parquet(filepath, compression='snappy')
        
        logger.info(f"Saved yield data to {filepath}")