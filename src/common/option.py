"""
Option class for representing options contracts.
"""

from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, Literal
import re


@dataclass(frozen=True)
class Option:
    """
    Represents an options contract with all relevant attributes.
    
    Attributes:
        underlying_ticker: The ticker symbol of the underlying asset
        contract_type: Either 'call' or 'put'
        strike_price: The strike price of the option
        expiration_date: The expiration date of the option
        ticker: The full options contract ticker (e.g., 'AAPL230120C00150000')
    """
    underlying_ticker: str
    contract_type: Literal['call', 'put']
    strike_price: float
    expiration_date: date
    ticker: str
    
    @classmethod
    def from_polygon_contract(cls, contract) -> 'Option':
        """
        Create an Option instance from a Polygon OptionsContract object.
        
        Args:
            contract: A Polygon OptionsContract object
            
        Returns:
            Option instance
        """
        return cls(
            underlying_ticker=contract.underlying_ticker,
            contract_type=contract.contract_type,
            strike_price=contract.strike_price,
            expiration_date=contract.expiration_date,
            ticker=contract.ticker
        )
        
    @classmethod
    def from_ticker(cls, ticker: str) -> 'Option':
        """
        Parse an options ticker symbol and create an Option instance.
        
        Args:
            ticker: Options ticker in OCC format (e.g., 'AAPL230120C00150000')
            
        Returns:
            Option instance
        """
        # OCC option symbol format: UUUUUUYYMMDDTSSSSSSSS
        # Where:
        # U = Underlying symbol (up to 6 chars, padded with spaces)
        # YY = Year (last 2 digits)
        # MM = Month
        # DD = Day
        # T = Call (C) or Put (P)
        # S = Strike price (8 digits, in cents)
        
        pattern = r'^([A-Z]+)(\d{2})(\d{2})(\d{2})([CP])(\d{8})$'
        match = re.match(pattern, ticker)
        
        if not match:
            raise ValueError(f"Invalid options ticker format: {ticker}")
            
        underlying = match.group(1)
        year = 2000 + int(match.group(2))  # Assumes 20XX
        month = int(match.group(3))
        day = int(match.group(4))
        contract_type = 'call' if match.group(5) == 'C' else 'put'
        strike = int(match.group(6)) / 1000  # Convert from cents/1000 to dollars
        
        return cls(
            underlying_ticker=underlying,
            contract_type=contract_type,
            strike_price=strike,
            expiration_date=date(year, month, day),
            ticker=ticker
        )
        
    @property
    def days_to_expiration(self) -> int:
        """Calculate days until expiration from today."""
        return (self.expiration_date - date.today()).days
        
    @property
    def is_expired(self) -> bool:
        """Check if the option has expired."""
        return self.expiration_date < date.today()
        
    def __str__(self) -> str:
        """String representation of the option."""
        return (
            f"{self.underlying_ticker} "
            f"{self.strike_price} "
            f"{self.contract_type.upper()} "
            f"exp:{self.expiration_date.strftime('%Y-%m-%d')}"
        )
        
    def __repr__(self) -> str:
        """Detailed representation of the option."""
        return (
            f"Option(ticker='{self.ticker}', "
            f"underlying='{self.underlying_ticker}', "
            f"type='{self.contract_type}', "
            f"strike={self.strike_price}, "
            f"expiration={self.expiration_date})"
        )