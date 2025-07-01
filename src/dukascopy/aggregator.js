/**
 * Data aggregation utilities for Dukascopy tick data
 * Converts tick data to various timeframes and calculates indicators
 */

const fs = require('fs').promises;
const path = require('path');
const { format } = require('date-fns');

/**
 * Aggregate tick data to bars (OHLCV)
 * @param {Array} ticks - Array of tick data
 * @param {string} timeframe - Target timeframe ('1m', '5m', '15m', '30m', '1h', '4h', '1d')
 * @returns {Array} Aggregated bar data
 */
function aggregateTicksToBars(ticks, timeframe) {
  if (ticks.length === 0) return [];
  
  // Parse timeframe
  const intervalMinutes = parseTimeframe(timeframe);
  
  // Sort ticks by timestamp
  ticks.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  const bars = [];
  let currentBar = null;
  
  for (const tick of ticks) {
    const tickTime = new Date(tick.timestamp);
    const barTime = getBarTime(tickTime, intervalMinutes);
    
    // Calculate mid price
    const midPrice = (tick.bid + tick.ask) / 2;
    
    if (!currentBar || currentBar.timestamp.getTime() !== barTime.getTime()) {
      // Start new bar
      if (currentBar) {
        bars.push(currentBar);
      }
      
      currentBar = {
        timestamp: barTime,
        open: midPrice,
        high: midPrice,
        low: midPrice,
        close: midPrice,
        volume: 0,
        tickCount: 1,
        spread: tick.ask - tick.bid
      };
    } else {
      // Update current bar
      currentBar.high = Math.max(currentBar.high, midPrice);
      currentBar.low = Math.min(currentBar.low, midPrice);
      currentBar.close = midPrice;
      currentBar.tickCount++;
      currentBar.spread = ((currentBar.spread * (currentBar.tickCount - 1)) + (tick.ask - tick.bid)) / currentBar.tickCount;
    }
    
    // Add volume if available
    if (tick.bidVolume && tick.askVolume) {
      currentBar.volume += (tick.bidVolume + tick.askVolume) / 2;
    }
  }
  
  // Don't forget the last bar
  if (currentBar) {
    bars.push(currentBar);
  }
  
  return bars;
}

/**
 * Parse timeframe string to minutes
 * @param {string} timeframe - Timeframe string (e.g., '5m', '1h')
 * @returns {number} Number of minutes
 */
function parseTimeframe(timeframe) {
  const match = timeframe.match(/^(\d+)([mhd])$/);
  if (!match) {
    throw new Error(`Invalid timeframe: ${timeframe}`);
  }
  
  const [, value, unit] = match;
  const num = parseInt(value);
  
  switch (unit) {
    case 'm': return num;
    case 'h': return num * 60;
    case 'd': return num * 60 * 24;
    default: throw new Error(`Invalid timeframe unit: ${unit}`);
  }
}

/**
 * Get bar timestamp for a given tick time
 * @param {Date} tickTime - Tick timestamp
 * @param {number} intervalMinutes - Bar interval in minutes
 * @returns {Date} Bar timestamp
 */
function getBarTime(tickTime, intervalMinutes) {
  const ms = tickTime.getTime();
  const intervalMs = intervalMinutes * 60 * 1000;
  const barMs = Math.floor(ms / intervalMs) * intervalMs;
  return new Date(barMs);
}

/**
 * Calculate various statistics from tick data
 * @param {Array} ticks - Array of tick data
 * @returns {Object} Statistics
 */
function calculateTickStatistics(ticks) {
  if (ticks.length === 0) return null;
  
  const spreads = ticks.map(t => t.ask - t.bid);
  const midPrices = ticks.map(t => (t.bid + t.ask) / 2);
  
  // Calculate returns
  const returns = [];
  for (let i = 1; i < midPrices.length; i++) {
    returns.push((midPrices[i] - midPrices[i-1]) / midPrices[i-1]);
  }
  
  return {
    tickCount: ticks.length,
    timeRange: {
      from: new Date(ticks[0].timestamp),
      to: new Date(ticks[ticks.length - 1].timestamp)
    },
    spread: {
      min: Math.min(...spreads),
      max: Math.max(...spreads),
      mean: spreads.reduce((a, b) => a + b, 0) / spreads.length,
      std: calculateStdDev(spreads)
    },
    price: {
      min: Math.min(...midPrices),
      max: Math.max(...midPrices),
      mean: midPrices.reduce((a, b) => a + b, 0) / midPrices.length,
      std: calculateStdDev(midPrices)
    },
    returns: {
      mean: returns.reduce((a, b) => a + b, 0) / returns.length,
      std: calculateStdDev(returns),
      skew: calculateSkewness(returns),
      kurtosis: calculateKurtosis(returns)
    }
  };
}

/**
 * Calculate standard deviation
 * @param {Array<number>} values - Array of numbers
 * @returns {number} Standard deviation
 */
function calculateStdDev(values) {
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const squaredDiffs = values.map(x => Math.pow(x - mean, 2));
  const variance = squaredDiffs.reduce((a, b) => a + b, 0) / values.length;
  return Math.sqrt(variance);
}

/**
 * Calculate skewness
 * @param {Array<number>} values - Array of numbers
 * @returns {number} Skewness
 */
function calculateSkewness(values) {
  const n = values.length;
  const mean = values.reduce((a, b) => a + b, 0) / n;
  const std = calculateStdDev(values);
  
  const cubedDiffs = values.map(x => Math.pow((x - mean) / std, 3));
  return (n / ((n - 1) * (n - 2))) * cubedDiffs.reduce((a, b) => a + b, 0);
}

/**
 * Calculate kurtosis
 * @param {Array<number>} values - Array of numbers
 * @returns {number} Kurtosis (excess)
 */
function calculateKurtosis(values) {
  const n = values.length;
  const mean = values.reduce((a, b) => a + b, 0) / n;
  const std = calculateStdDev(values);
  
  const fourthDiffs = values.map(x => Math.pow((x - mean) / std, 4));
  const rawKurtosis = (n * (n + 1) / ((n - 1) * (n - 2) * (n - 3))) * 
                      fourthDiffs.reduce((a, b) => a + b, 0) -
                      (3 * Math.pow(n - 1, 2) / ((n - 2) * (n - 3)));
  
  return rawKurtosis - 3; // Excess kurtosis
}

/**
 * Resample bar data to a different timeframe
 * @param {Array} bars - Array of bar data
 * @param {string} targetTimeframe - Target timeframe
 * @returns {Array} Resampled bars
 */
function resampleBars(bars, targetTimeframe) {
  if (bars.length === 0) return [];
  
  // Sort bars by timestamp
  bars.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  const targetMinutes = parseTimeframe(targetTimeframe);
  const resampledBars = [];
  let currentBar = null;
  
  for (const bar of bars) {
    const barTime = getBarTime(new Date(bar.timestamp), targetMinutes);
    
    if (!currentBar || currentBar.timestamp.getTime() !== barTime.getTime()) {
      // Start new bar
      if (currentBar) {
        resampledBars.push(currentBar);
      }
      
      currentBar = {
        timestamp: barTime,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume || 0,
        tickCount: bar.tickCount || 1
      };
    } else {
      // Update current bar
      currentBar.high = Math.max(currentBar.high, bar.high);
      currentBar.low = Math.min(currentBar.low, bar.low);
      currentBar.close = bar.close;
      currentBar.volume += bar.volume || 0;
      currentBar.tickCount += bar.tickCount || 1;
    }
  }
  
  // Don't forget the last bar
  if (currentBar) {
    resampledBars.push(currentBar);
  }
  
  return resampledBars;
}

/**
 * Load tick data from file
 * @param {string} filename - Filename to load
 * @returns {Promise<Array>} Tick data
 */
async function loadTickData(filename) {
  const filepath = path.join(__dirname, '../../data/dukascopy', filename);
  const content = await fs.readFile(filepath, 'utf8');
  
  if (filename.endsWith('.json')) {
    return JSON.parse(content);
  } else if (filename.endsWith('.csv')) {
    // Parse CSV
    const lines = content.split('\n');
    const headers = lines[0].split(',');
    
    return lines.slice(1)
      .filter(line => line.trim())
      .map(line => {
        const values = line.split(',');
        const obj = {};
        headers.forEach((header, i) => {
          const value = values[i];
          // Parse numbers
          if (header !== 'timestamp' && !isNaN(value)) {
            obj[header] = parseFloat(value);
          } else {
            obj[header] = value;
          }
        });
        return obj;
      });
  }
  
  throw new Error('Unsupported file format');
}

// Export functions
module.exports = {
  aggregateTicksToBars,
  calculateTickStatistics,
  resampleBars,
  loadTickData,
  parseTimeframe
};

// CLI interface
if (require.main === module) {
  const args = process.argv.slice(2);
  
  if (args.length < 2) {
    console.log('Usage: node aggregator.js <tick_file> <timeframe> [output_format]');
    console.log('Example: node aggregator.js EUR_USD_tick_20230101_20230131.json 5m csv');
    process.exit(1);
  }
  
  const [tickFile, timeframe, outputFormat = 'json'] = args;
  
  loadTickData(tickFile)
    .then(ticks => {
      console.log(`Loaded ${ticks.length} ticks`);
      
      // Calculate statistics
      const stats = calculateTickStatistics(ticks);
      console.log('Statistics:', stats);
      
      // Aggregate to bars
      const bars = aggregateTicksToBars(ticks, timeframe);
      console.log(`Aggregated to ${bars.length} ${timeframe} bars`);
      
      // Save aggregated data
      const outputFile = tickFile.replace('_tick_', `_${timeframe}_`);
      const outputPath = path.join(__dirname, '../../data/dukascopy', outputFile);
      
      if (outputFormat === 'json') {
        return fs.writeFile(outputPath, JSON.stringify(bars, null, 2));
      } else {
        // Convert to CSV
        const headers = Object.keys(bars[0]);
        const csv = [headers.join(',')];
        bars.forEach(bar => {
          const values = headers.map(h => bar[h] instanceof Date ? bar[h].toISOString() : bar[h]);
          csv.push(values.join(','));
        });
        return fs.writeFile(outputPath, csv.join('\n'));
      }
    })
    .then(() => {
      console.log('Aggregation complete');
      process.exit(0);
    })
    .catch(error => {
      console.error('Error:', error.message);
      process.exit(1);
    });
}