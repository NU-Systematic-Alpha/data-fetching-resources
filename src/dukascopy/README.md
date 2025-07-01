# Dukascopy Data Fetcher

High-quality tick data fetching for forex, commodities, and indices using the Dukascopy Node library.

## Features

- Fetch tick-by-tick data with bid/ask prices
- Aggregate tick data to various timeframes (1m, 5m, 15m, 30m, 1h, 4h, 1d)
- Support for multiple instruments:
  - Forex pairs (EUR/USD, GBP/USD, etc.)
  - Commodities (Gold, Silver, Oil)
  - Indices (S&P 500, NASDAQ, Dow Jones)
  - Cryptocurrencies (BTC/USD, ETH/USD)
- Export data in JSON or CSV format
- Calculate statistics from tick data

## Usage

### Fetching Tick Data

```javascript
const { fetchTickData } = require('./tick_fetcher');

// Fetch tick data
const result = await fetchTickData({
  instrument: 'EUR/USD',
  from: new Date('2023-01-01'),
  to: new Date('2023-01-31'),
  timeframe: 'tick',  // or 'm1', 'm5', 'm15', 'm30', 'h1', 'd1'
  volumes: true,
  format: 'json'      // or 'csv'
});

console.log(`Fetched ${result.dataPoints} ticks`);
```

### Command Line Usage

```bash
# Fetch tick data
node src/dukascopy/tick_fetcher.js EUR/USD 2023-01-01 2023-01-31 tick json

# Fetch 1-minute bars
node src/dukascopy/tick_fetcher.js EUR/USD 2023-01-01 2023-01-31 m1 csv

# Aggregate tick data to 5-minute bars
node src/dukascopy/aggregator.js EUR_USD_tick_20230101_20230131.json 5m csv
```

### Available Instruments

#### Forex Majors
- EUR/USD
- GBP/USD
- USD/JPY
- USD/CHF
- AUD/USD
- USD/CAD
- NZD/USD

#### Forex Crosses
- EUR/GBP
- EUR/JPY
- GBP/JPY

#### Commodities
- XAU/USD (Gold)
- XAG/USD (Silver)
- WTI (Oil)

#### Indices
- SPX500 (S&P 500)
- NAS100 (NASDAQ 100)
- US30 (Dow Jones)

#### Cryptocurrencies
- BTC/USD
- ETH/USD

### Data Aggregation

The aggregator utility can convert tick data to bar data:

```javascript
const { aggregateTicksToBars, loadTickData } = require('./aggregator');

// Load tick data
const ticks = await loadTickData('EUR_USD_tick_20230101_20230131.json');

// Aggregate to 5-minute bars
const bars = aggregateTicksToBars(ticks, '5m');

// Calculate statistics
const stats = calculateTickStatistics(ticks);
console.log('Spread statistics:', stats.spread);
console.log('Price statistics:', stats.price);
console.log('Returns statistics:', stats.returns);
```

### Output Format

#### Tick Data Format
```json
{
  "timestamp": "2023-01-01T00:00:00.000Z",
  "bid": 1.0701,
  "ask": 1.0702,
  "bidVolume": 1000000,
  "askVolume": 1500000
}
```

#### Bar Data Format
```json
{
  "timestamp": "2023-01-01T00:00:00.000Z",
  "open": 1.0701,
  "high": 1.0705,
  "low": 1.0700,
  "close": 1.0703,
  "volume": 25000000,
  "tickCount": 150,
  "spread": 0.0001
}
```

## Notes

- Dukascopy provides free historical data but has rate limits
- Data quality is very high with real tick-by-tick data
- Weekend data may be sparse for forex pairs
- Older data (before 2000) may not be available for all instruments