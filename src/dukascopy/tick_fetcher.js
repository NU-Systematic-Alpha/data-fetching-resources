/**
 * Tick data fetcher using Dukascopy Node
 * Fetches high-quality tick data for forex, commodities, and indices
 */

const { getHistoricalRates } = require('@dukascopy/dukascopy-node');
const fs = require('fs').promises;
const path = require('path');
const { format } = require('date-fns');
const winston = require('winston');

// Configure logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'logs/dukascopy-error.log', level: 'error' }),
    new winston.transports.File({ filename: 'logs/dukascopy.log' }),
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    })
  ]
});

// Common instruments mapping
const INSTRUMENTS = {
  // Forex Majors
  'EUR/USD': 'eurusd',
  'GBP/USD': 'gbpusd',
  'USD/JPY': 'usdjpy',
  'USD/CHF': 'usdchf',
  'AUD/USD': 'audusd',
  'USD/CAD': 'usdcad',
  'NZD/USD': 'nzdusd',
  
  // Forex Crosses
  'EUR/GBP': 'eurgbp',
  'EUR/JPY': 'eurjpy',
  'GBP/JPY': 'gbpjpy',
  
  // Commodities
  'XAU/USD': 'xauusd',  // Gold
  'XAG/USD': 'xagusd',  // Silver
  'WTI': 'wtiusd',      // Oil
  
  // Indices
  'SPX500': 'spx500',
  'NAS100': 'nas100',
  'US30': 'us30',       // Dow Jones
  
  // Crypto
  'BTC/USD': 'btcusd',
  'ETH/USD': 'ethusd'
};

/**
 * Fetch tick data from Dukascopy
 * @param {Object} options - Fetch options
 * @param {string} options.instrument - Instrument to fetch (e.g., 'EUR/USD')
 * @param {Date} options.from - Start date
 * @param {Date} options.to - End date
 * @param {string} options.timeframe - Timeframe: 'tick', 'm1', 'm5', 'm15', 'm30', 'h1', 'd1'
 * @param {boolean} options.volumes - Include volume data
 * @param {string} options.format - Output format: 'json' or 'csv'
 * @returns {Promise<Object>} Result object with data and metadata
 */
async function fetchTickData(options) {
  const {
    instrument,
    from,
    to,
    timeframe = 'tick',
    volumes = true,
    format = 'json'
  } = options;
  
  // Validate instrument
  const instrumentCode = INSTRUMENTS[instrument] || instrument.toLowerCase();
  
  logger.info(`Fetching ${timeframe} data for ${instrument} from ${from} to ${to}`);
  
  try {
    // Fetch data from Dukascopy
    const { data, metadata } = await getHistoricalRates({
      instrument: instrumentCode,
      dates: {
        from: from,
        to: to
      },
      timeframe: timeframe,
      volumes: volumes,
      format: 'json'  // Always fetch as JSON first
    });
    
    logger.info(`Fetched ${data.length} data points for ${instrument}`);
    
    // Process the data
    const processedData = processTickData(data, timeframe);
    
    // Save to file
    const filename = await saveData(processedData, {
      instrument: instrument,
      from: from,
      to: to,
      timeframe: timeframe,
      format: format
    });
    
    return {
      success: true,
      instrument: instrument,
      timeframe: timeframe,
      dataPoints: processedData.length,
      from: from,
      to: to,
      filename: filename,
      data: format === 'json' ? processedData : null
    };
    
  } catch (error) {
    logger.error(`Error fetching data for ${instrument}: ${error.message}`);
    throw error;
  }
}

/**
 * Process raw tick data
 * @param {Array} data - Raw data from Dukascopy
 * @param {string} timeframe - Data timeframe
 * @returns {Array} Processed data
 */
function processTickData(data, timeframe) {
  if (timeframe === 'tick') {
    // For tick data, format timestamps and organize fields
    return data.map(tick => ({
      timestamp: new Date(tick.timestamp),
      bid: tick.bid,
      ask: tick.ask,
      bidVolume: tick.bidVolume || null,
      askVolume: tick.askVolume || null
    }));
  } else {
    // For aggregated data (bars)
    return data.map(bar => ({
      timestamp: new Date(bar.timestamp),
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
      volume: bar.volume || null
    }));
  }
}

/**
 * Save data to file
 * @param {Array} data - Data to save
 * @param {Object} options - Save options
 * @returns {Promise<string>} Filename
 */
async function saveData(data, options) {
  const { instrument, from, to, timeframe, format } = options;
  
  // Create filename
  const fromStr = format(from, 'yyyyMMdd');
  const toStr = format(to, 'yyyyMMdd');
  const instrumentClean = instrument.replace('/', '_');
  const filename = `${instrumentClean}_${timeframe}_${fromStr}_${toStr}.${format}`;
  
  // Ensure data directory exists
  const dataDir = path.join(__dirname, '../../data/dukascopy');
  await fs.mkdir(dataDir, { recursive: true });
  
  const filepath = path.join(dataDir, filename);
  
  if (format === 'csv') {
    // Convert to CSV
    const csv = await jsonToCSV(data);
    await fs.writeFile(filepath, csv);
  } else {
    // Save as JSON
    await fs.writeFile(filepath, JSON.stringify(data, null, 2));
  }
  
  logger.info(`Saved data to ${filepath}`);
  
  return filename;
}

/**
 * Convert JSON data to CSV
 * @param {Array} data - JSON data
 * @returns {Promise<string>} CSV string
 */
async function jsonToCSV(data) {
  if (data.length === 0) return '';
  
  // Get headers from first object
  const headers = Object.keys(data[0]);
  const csvRows = [headers.join(',')];
  
  // Convert each object to CSV row
  for (const row of data) {
    const values = headers.map(header => {
      const value = row[header];
      // Format dates
      if (value instanceof Date) {
        return value.toISOString();
      }
      // Handle null/undefined
      if (value === null || value === undefined) {
        return '';
      }
      // Quote strings that contain commas
      if (typeof value === 'string' && value.includes(',')) {
        return `"${value}"`;
      }
      return value;
    });
    csvRows.push(values.join(','));
  }
  
  return csvRows.join('\n');
}

/**
 * Fetch data for multiple instruments
 * @param {Array<string>} instruments - List of instruments
 * @param {Object} options - Common options for all instruments
 * @returns {Promise<Array>} Results for each instrument
 */
async function fetchMultipleInstruments(instruments, options) {
  const results = [];
  
  for (const instrument of instruments) {
    try {
      const result = await fetchTickData({
        ...options,
        instrument: instrument
      });
      results.push(result);
      
      // Add delay to avoid overwhelming the API
      await new Promise(resolve => setTimeout(resolve, 1000));
      
    } catch (error) {
      results.push({
        success: false,
        instrument: instrument,
        error: error.message
      });
    }
  }
  
  return results;
}

/**
 * Get available instruments
 * @returns {Object} Available instruments mapping
 */
function getAvailableInstruments() {
  return INSTRUMENTS;
}

// Export functions
module.exports = {
  fetchTickData,
  fetchMultipleInstruments,
  getAvailableInstruments,
  INSTRUMENTS
};

// CLI interface
if (require.main === module) {
  const args = process.argv.slice(2);
  
  if (args.length < 3) {
    console.log('Usage: node tick_fetcher.js <instrument> <from_date> <to_date> [timeframe] [format]');
    console.log('Example: node tick_fetcher.js EUR/USD 2023-01-01 2023-01-31 m1 csv');
    console.log('\nAvailable instruments:');
    Object.keys(INSTRUMENTS).forEach(inst => console.log(`  ${inst}`));
    console.log('\nTimeframes: tick, m1, m5, m15, m30, h1, d1');
    console.log('Formats: json, csv');
    process.exit(1);
  }
  
  const [instrument, fromDate, toDate, timeframe = 'tick', format = 'json'] = args;
  
  fetchTickData({
    instrument: instrument,
    from: new Date(fromDate),
    to: new Date(toDate),
    timeframe: timeframe,
    format: format
  })
  .then(result => {
    console.log('Success:', result);
    process.exit(0);
  })
  .catch(error => {
    console.error('Error:', error.message);
    process.exit(1);
  });
}