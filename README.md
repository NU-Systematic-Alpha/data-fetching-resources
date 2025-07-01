# Data Fetching Resources for Quant Club

A comprehensive Python/Node.js library for fetching financial market data from multiple sources. This toolkit provides easy-to-use interfaces for retrieving stock, options, treasury, and forex data for quantitative analysis and trading strategies.

## Features

- **Multiple Data Sources**
  - Polygon.io API for stocks, options, and treasury data
  - Dukascopy for high-quality tick data (forex, commodities, indices)
  
- **Asset Classes Supported**
  - Stocks and ETFs (historical bars, quotes, trades)
  - Options (chains, historical contracts, Greeks)
  - Treasury yields and yield curves
  - Forex tick and bar data
  - Commodities (Gold, Silver, Oil)
  - Indices (S&P 500, NASDAQ, Dow Jones)
  - Cryptocurrencies (BTC, ETH)

- **Key Features**
  - Built-in rate limiting and caching
  - Multiple output formats (JSON, CSV, Parquet)
  - Data validation and error handling
  - Ready-to-use example scripts
  - Modular design for easy extension

## Quick Start

### Prerequisites

- Python 3.8+
- Node.js 14+
- Polygon.io API key (free tier available at [polygon.io](https://polygon.io))

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd data-fetching-resources
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Install Node.js dependencies:
```bash
npm install
```

4. Set up configuration:
```bash
cp config/config.ini.example config/config.ini
# Edit config/config.ini and add your Polygon API key
```

### Basic Usage

#### Fetching Stock Data
```python
from src.polygon import StockDataFetcher

# Initialize fetcher
fetcher = StockDataFetcher()

# Fetch daily bars for AAPL
data = fetcher.fetch_bars(
    ticker='AAPL',
    start_date='2023-01-01',
    end_date='2023-12-31',
    timeframe='day'
)

print(f"Fetched {len(data)} days of data")
print(data.head())
```

#### Fetching Options Data
```python
from src.polygon import OptionsDataFetcher

# Initialize fetcher
options_fetcher = OptionsDataFetcher()

# Fetch options chain
options = options_fetcher.fetch_options_chain(
    underlying_ticker='SPY',
    strike_price_gte=400,
    strike_price_lte=450
)

print(f"Found {len(options)} option contracts")
```

#### Fetching Treasury Yields
```python
from src.polygon import TreasuryDataFetcher

# Initialize fetcher
treasury_fetcher = TreasuryDataFetcher()

# Fetch yield curve
yield_curve = treasury_fetcher.fetch_yield_curve('2024-01-01')
print(yield_curve)
```

#### Fetching Forex Tick Data
```javascript
const { fetchTickData } = require('./src/dukascopy/tick_fetcher');

// Fetch EUR/USD tick data
const result = await fetchTickData({
  instrument: 'EUR/USD',
  from: new Date('2024-01-01'),
  to: new Date('2024-01-02'),
  timeframe: 'tick',
  format: 'json'
});

console.log(`Fetched ${result.dataPoints} ticks`);
```

## Project Structure

```
data-fetching-resources/
├── config/                 # Configuration files
│   ├── config.ini.example  # Example configuration
│   └── README.md          # Config documentation
├── src/                   # Source code
│   ├── polygon/           # Polygon.io modules
│   │   ├── base.py        # Base client with auth & caching
│   │   ├── stocks.py      # Stock data fetcher
│   │   ├── options.py     # Options data fetcher
│   │   └── treasuries.py  # Treasury data fetcher
│   ├── dukascopy/         # Dukascopy modules
│   │   ├── tick_fetcher.js # Tick data fetching
│   │   └── aggregator.js  # Data aggregation utilities
│   └── common/            # Common utilities
│       └── option.py      # Option class definition
├── examples/              # Example scripts
│   ├── fetch_stock_history.py
│   ├── fetch_options_range.py
│   ├── fetch_treasury_curve.py
│   └── fetch_forex_ticks.js
├── data/                  # Data storage (git-ignored)
├── cache/                 # Cache storage (git-ignored)
└── logs/                  # Log files (git-ignored)
```

## API Documentation

### Polygon.io Modules

#### StockDataFetcher
- `fetch_bars()` - Get OHLCV bar data
- `fetch_quotes()` - Get bid/ask quotes
- `fetch_trades()` - Get individual trades
- `fetch_snapshot()` - Get latest market snapshot
- `fetch_multiple_bars()` - Fetch data for multiple tickers

#### OptionsDataFetcher
- `fetch_options_chain()` - Get options chain for a ticker
- `fetch_contracts_in_range()` - Get all contracts in a date range
- `fetch_contract_bars()` - Get historical data for a contract
- `fetch_multiple_contracts_bars()` - Get data for multiple contracts

#### TreasuryDataFetcher
- `fetch_treasury_yield()` - Get yield for specific maturity
- `fetch_yield_curve()` - Get full yield curve for a date
- `fetch_yield_curve_history()` - Get historical yield curves
- `calculate_yield_spreads()` - Calculate spreads (2-10, etc.)

### Dukascopy Modules

#### tick_fetcher.js
- `fetchTickData()` - Fetch tick or bar data
- `fetchMultipleInstruments()` - Fetch data for multiple instruments
- `getAvailableInstruments()` - List available instruments

#### aggregator.js
- `aggregateTicksToBars()` - Convert ticks to OHLCV bars
- `calculateTickStatistics()` - Calculate spread and price statistics
- `resampleBars()` - Change bar timeframe

## Examples

Run the example scripts to see the library in action:

```bash
# Stock data example
python examples/fetch_stock_history.py

# Options data example
python examples/fetch_options_range.py

# Treasury yields example
python examples/fetch_treasury_curve.py

# Forex tick data example
node examples/fetch_forex_ticks.js
```

## Configuration

### API Keys

Edit `config/config.ini` to add your API keys:

```ini
[polygon]
api_key = YOUR_POLYGON_API_KEY_HERE

[rate_limits]
polygon_rpm = 5  # Requests per minute (based on your plan)

[cache]
stock_data_ttl = 86400  # Cache TTL in seconds
```

### Rate Limits

The library respects API rate limits automatically:
- Free Polygon tier: 5 requests/minute
- Basic tier: 100 requests/minute
- Starter and above: unlimited

## Best Practices

1. **Use Caching**: The library automatically caches responses to reduce API calls
2. **Batch Requests**: Use `fetch_multiple_*` methods when possible
3. **Handle Errors**: Always wrap API calls in try-except blocks
4. **Save Data**: Use the built-in save methods to store data locally
5. **Respect Limits**: Don't exceed your API plan limits

## Troubleshooting

### Common Issues

1. **"API key not found"**
   - Make sure you've created `config/config.ini` from the example
   - Verify your API key is correctly entered

2. **"Rate limit exceeded"**
   - Check your `polygon_rpm` setting matches your plan
   - The library will automatically wait between requests

3. **"No data returned"**
   - Verify the ticker symbol is correct
   - Check if the market was open on the requested dates
   - Some data may not be available on free tiers

### Logging

Logs are stored in the `logs/` directory:
- `polygon_*.log` - Polygon API logs
- `dukascopy*.log` - Dukascopy logs

## Contributing

Feel free to submit issues, fork the repository, and create pull requests. Please follow the existing code style and add tests for new features.

## Additional Resources

- [Polygon.io Documentation](https://polygon.io/docs)
- [Dukascopy Node Documentation](https://www.dukascopy-node.app/)
- [Options Trading Basics](https://www.investopedia.com/options-basics-tutorial-4583012)
- [Treasury Yields Explained](https://www.investopedia.com/terms/t/treasury-yield.asp)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This library is for educational and research purposes only. Always verify data accuracy before using in production trading systems. Past performance does not guarantee future results.