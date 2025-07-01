#!/usr/bin/env node
/**
 * Example: Fetching forex tick data using Dukascopy
 */

const path = require('path');
const { 
  fetchTickData, 
  fetchMultipleInstruments, 
  getAvailableInstruments 
} = require('../src/dukascopy/tick_fetcher');
const { 
  aggregateTicksToBars, 
  calculateTickStatistics,
  loadTickData 
} = require('../src/dukascopy/aggregator');

async function main() {
  console.log('Forex Tick Data Fetching Examples');
  console.log('='.repeat(50));
  
  // Define parameters
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - 7); // Last 7 days
  
  try {
    // Example 1: Fetch tick data for EUR/USD
    console.log('\n1. Fetching EUR/USD tick data...');
    
    const tickResult = await fetchTickData({
      instrument: 'EUR/USD',
      from: startDate,
      to: endDate,
      timeframe: 'tick',
      volumes: true,
      format: 'json'
    });
    
    console.log(`   Fetched ${tickResult.dataPoints} ticks`);
    console.log(`   Saved to: ${tickResult.filename}`);
    
    // Show sample of tick data
    if (tickResult.data && tickResult.data.length > 0) {
      console.log('\n   Sample tick data (first 5 ticks):');
      tickResult.data.slice(0, 5).forEach((tick, i) => {
        console.log(`   ${i + 1}. Time: ${tick.timestamp}, Bid: ${tick.bid}, Ask: ${tick.ask}`);
      });
      
      // Calculate spread statistics
      const spreads = tickResult.data.map(t => (t.ask - t.bid) * 10000); // Convert to pips
      const avgSpread = spreads.reduce((a, b) => a + b, 0) / spreads.length;
      const minSpread = Math.min(...spreads);
      const maxSpread = Math.max(...spreads);
      
      console.log('\n   Spread statistics (in pips):');
      console.log(`   - Average: ${avgSpread.toFixed(2)}`);
      console.log(`   - Min: ${minSpread.toFixed(2)}`);
      console.log(`   - Max: ${maxSpread.toFixed(2)}`);
    }
    
    // Example 2: Fetch 1-minute bars
    console.log('\n2. Fetching EUR/USD 1-minute bars...');
    
    const barResult = await fetchTickData({
      instrument: 'EUR/USD',
      from: startDate,
      to: endDate,
      timeframe: 'm1',
      volumes: true,
      format: 'json'
    });
    
    console.log(`   Fetched ${barResult.dataPoints} bars`);
    
    if (barResult.data && barResult.data.length > 0) {
      // Calculate daily ranges
      const dailyData = {};
      
      barResult.data.forEach(bar => {
        const date = bar.timestamp.split('T')[0];
        if (!dailyData[date]) {
          dailyData[date] = { high: bar.high, low: bar.low };
        } else {
          dailyData[date].high = Math.max(dailyData[date].high, bar.high);
          dailyData[date].low = Math.min(dailyData[date].low, bar.low);
        }
      });
      
      console.log('\n   Daily ranges (in pips):');
      Object.entries(dailyData).slice(-5).forEach(([date, range]) => {
        const rangePips = (range.high - range.low) * 10000;
        console.log(`   ${date}: ${rangePips.toFixed(1)} pips`);
      });
    }
    
    // Example 3: Fetch multiple currency pairs
    console.log('\n3. Fetching multiple currency pairs...');
    
    const instruments = ['EUR/USD', 'GBP/USD', 'USD/JPY'];
    const multiResults = await fetchMultipleInstruments(instruments, {
      from: new Date(Date.now() - 24 * 60 * 60 * 1000), // Last 24 hours
      to: new Date(),
      timeframe: 'h1',
      format: 'json'
    });
    
    console.log('\n   Results:');
    multiResults.forEach(result => {
      if (result.success) {
        console.log(`   ${result.instrument}: ${result.dataPoints} hourly bars`);
      } else {
        console.log(`   ${result.instrument}: Failed - ${result.error}`);
      }
    });
    
    // Example 4: Aggregate tick data to different timeframes
    console.log('\n4. Aggregating tick data...');
    
    if (tickResult.data && tickResult.data.length > 100) {
      // Aggregate to 5-minute bars
      const fiveMinBars = aggregateTicksToBars(tickResult.data, '5m');
      console.log(`   Created ${fiveMinBars.length} 5-minute bars from ticks`);
      
      // Aggregate to 15-minute bars
      const fifteenMinBars = aggregateTicksToBars(tickResult.data, '15m');
      console.log(`   Created ${fifteenMinBars.length} 15-minute bars from ticks`);
      
      // Calculate statistics
      const stats = calculateTickStatistics(tickResult.data);
      
      console.log('\n   Tick data statistics:');
      console.log(`   - Total ticks: ${stats.tickCount}`);
      console.log(`   - Time range: ${stats.timeRange.from.toISOString()} to ${stats.timeRange.to.toISOString()}`);
      console.log(`   - Spread (pips): min=${(stats.spread.min * 10000).toFixed(2)}, avg=${(stats.spread.mean * 10000).toFixed(2)}, max=${(stats.spread.max * 10000).toFixed(2)}`);
      console.log(`   - Price range: ${stats.price.min.toFixed(5)} - ${stats.price.max.toFixed(5)}`);
      console.log(`   - Returns: mean=${(stats.returns.mean * 10000).toFixed(2)} pips, volatility=${(stats.returns.std * 10000).toFixed(2)} pips`);
    }
    
    // Example 5: Fetch commodity data (Gold)
    console.log('\n5. Fetching Gold (XAU/USD) data...');
    
    const goldResult = await fetchTickData({
      instrument: 'XAU/USD',
      from: new Date(Date.now() - 24 * 60 * 60 * 1000), // Last 24 hours
      to: new Date(),
      timeframe: 'h1',
      format: 'csv'
    });
    
    if (goldResult.success) {
      console.log(`   Fetched ${goldResult.dataPoints} hourly bars`);
      console.log(`   Saved as CSV to: ${goldResult.filename}`);
    }
    
    // Show available instruments
    console.log('\n6. Available instruments:');
    const instruments_available = getAvailableInstruments();
    
    console.log('\n   Forex:');
    ['EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/CHF'].forEach(inst => {
      console.log(`   - ${inst}`);
    });
    
    console.log('\n   Commodities:');
    ['XAU/USD', 'XAG/USD', 'WTI'].forEach(inst => {
      console.log(`   - ${inst}`);
    });
    
    console.log('\n   Indices:');
    ['SPX500', 'NAS100', 'US30'].forEach(inst => {
      console.log(`   - ${inst}`);
    });
    
    console.log('\n   Crypto:');
    ['BTC/USD', 'ETH/USD'].forEach(inst => {
      console.log(`   - ${inst}`);
    });
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

// Run the example
if (require.main === module) {
  main()
    .then(() => {
      console.log('\nExample completed successfully!');
      process.exit(0);
    })
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}