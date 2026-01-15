/**
 * Bitcoin Data Analysis Demo
 * 
 * Demonstrates Mornye's data analysis features on real Bitcoin historical data.
 * File: 7.38M rows of 1-minute OHLCV data from 2012-2024
 */

import { readCsv} from '../src';

const DATA_FILE = './artifac/btcusd_1-min_data.csv';

// Memory tracking helper
function getMemoryUsage(): { heapUsed: number; heapTotal: number; rss: number } {
  const mem = process.memoryUsage();
  return {
    heapUsed: mem.heapUsed / 1024 / 1024,  // MB
    heapTotal: mem.heapTotal / 1024 / 1024,
    rss: mem.rss / 1024 / 1024,
  };
}

function logMemory(label: string) {
  const mem = getMemoryUsage();
  console.log(`   [Memory] ${label}: ${mem.heapUsed.toFixed(1)}MB heap, ${mem.rss.toFixed(1)}MB RSS`);
}

async function main() {
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║         Bitcoin Data Analysis with Mornye                 ║');
  console.log('╚═══════════════════════════════════════════════════════════╝\n');

  // ─────────────────────────────────────────────────────────────
  // 1. Load the data
  // ─────────────────────────────────────────────────────────────
  logMemory('Before load');
  
  console.log('📁 Loading 387MB CSV file...');
  const start = performance.now();
  const { df, parseErrors, hasErrors } = await readCsv(DATA_FILE);
  const loadTime = performance.now() - start;
  
  logMemory('After load');
  console.log(`✅ Loaded in ${(loadTime / 1000).toFixed(2)}s`);
  console.log(`   Shape: ${df.shape[0].toLocaleString()} rows × ${df.shape[1]} columns`);
  console.log(`   Columns: ${df.columns().join(', ')}`);
  
  // Report any parse errors
  if (hasErrors && parseErrors) {
    console.log('\n⚠️  Parse Errors Detected:');
    for (const [col, failures] of parseErrors) {
      console.log(`   ${String(col)}: ${failures.failureCount} failures (${(failures.successRate * 100).toFixed(2)}% success)`);
    }
  }
  console.log('');

  // ─────────────────────────────────────────────────────────────
  // 2. Basic statistics
  // ─────────────────────────────────────────────────────────────
  console.log('📊 Basic Statistics:');
  console.log('────────────────────');
  
  const close = df.col('Close');
  const volume = df.col('Volume');
  
  console.log(`Close Price:`);
  console.log(`  Mean:   $${close.mean().toFixed(2)}`);
  console.log(`  Min:    $${close.min().toFixed(2)}`);
  console.log(`  Max:    $${close.max().toFixed(2)}`);
  console.log(`  Std:    $${close.std().toFixed(2)}`);
  console.log('');
  
  console.log(`Volume:`);
  console.log(`  Total:  ${volume.sum().toLocaleString()} BTC traded`);
  console.log(`  Mean:   ${volume.mean().toFixed(4)} BTC/min`);
  console.log('');

  // ─────────────────────────────────────────────────────────────
  // 3. Filtering - High price moments
  // ─────────────────────────────────────────────────────────────
  console.log('🔍 Filtering - Moments when BTC was above $60,000:');
  console.log('─────────────────────────────────────────────────');
  
  const highPriceMoments = df.where('Close', '>', 60000);
  console.log(`   Found ${highPriceMoments.shape[0].toLocaleString()} minutes above $60k`);
  console.log(`   That's ${((highPriceMoments.shape[0] / df.shape[0]) * 100).toFixed(2)}% of the dataset`);
  console.log('');
  
  // ─────────────────────────────────────────────────────────────
  // 4. Filtering - High volume periods
  // ─────────────────────────────────────────────────────────────
  console.log('📈 High Volume Analysis (volume > 100 BTC/min):');
  console.log('───────────────────────────────────────────────');
  
  const highVolume = df.where('Volume', '>', 100);
  const highVolClose = highVolume.col('Close');
  
  console.log(`   High-volume minutes: ${highVolume.shape[0].toLocaleString()}`);
  console.log(`   Average price during high volume: $${highVolClose.mean().toFixed(2)}`);
  console.log('');

  // ─────────────────────────────────────────────────────────────
  // 5. First 10 rows
  // ─────────────────────────────────────────────────────────────
  console.log('📋 First 10 rows (early 2012):');
  console.log('──────────────────────────────');
  df.head(10).print();
  console.log('');

  // ─────────────────────────────────────────────────────────────
  // 6. Last 10 rows  
  // ─────────────────────────────────────────────────────────────
  console.log('📋 Last 10 rows (most recent):');
  console.log('──────────────────────────────');
  df.tail(10).print();
  console.log('');

  // ─────────────────────────────────────────────────────────────
  // 7. Price range analysis
  // ─────────────────────────────────────────────────────────────
  console.log('💰 Price Range Analysis:');
  console.log('────────────────────────');
  
  const priceRanges = [
    { label: 'Under $100', min: 0, max: 100 },
    { label: '$100 - $1,000', min: 100, max: 1000 },
    { label: '$1,000 - $10,000', min: 1000, max: 10000 },
    { label: '$10,000 - $50,000', min: 10000, max: 50000 },
    { label: 'Above $50,000', min: 50000, max: Infinity },
  ];
  
  for (const range of priceRanges) {
    let count: number;
    if (range.max === Infinity) {
      count = df.where('Close', '>', range.min).shape[0];
    } else {
      count = df.filter(row => row.Close != null && row.Close >= range.min && row.Close < range.max).shape[0];
    }
    const pct = ((count / df.shape[0]) * 100).toFixed(1);
    console.log(`   ${range.label.padEnd(20)}: ${count.toLocaleString().padStart(12)} minutes (${pct}%)`);
  }
  console.log('');

  // ─────────────────────────────────────────────────────────────
  // 8. Daily volatility (High - Low spread)
  // ─────────────────────────────────────────────────────────────
  console.log('📉 Volatility Analysis:');
  console.log('───────────────────────');
  
  // Calculate spread per minute using apply
  const spreads = df.apply(row => {
    if (row.Low === 0 || row.High === 0 || !row.Low || !row.High) return 0;
    return ((row.High - row.Low) / row.Low) * 100;
  });
  
  // Use reduce to avoid stack overflow with 7M+ values
  let avgSpread = 0;
  let maxSpread = 0;
  for (const s of spreads) {
    avgSpread += s;
    if (s > maxSpread) maxSpread = s;
  }
  avgSpread /= spreads.length;
  
  console.log(`   Average per-minute spread: ${avgSpread.toFixed(4)}%`);
  console.log(`   Max per-minute spread:     ${maxSpread.toFixed(2)}%`);
  console.log('');

  // ─────────────────────────────────────────────────────────────
  // 9. DataFrame describe
  // ─────────────────────────────────────────────────────────────
  console.log('📊 Full Statistics (describe):');
  console.log('──────────────────────────────');
  const desc = df.describe();
  for (const [col, stats] of Object.entries(desc)) {
    console.log(`\n   ${col}:`);
    console.log(`     Count: ${stats.count.toLocaleString()}`);
    console.log(`     Mean:  ${stats.mean.toFixed(4)}`);
    console.log(`     Std:   ${stats.std.toFixed(4)}`);
    console.log(`     Min:   ${stats.min.toFixed(4)}`);
    console.log(`     Max:   ${stats.max.toFixed(4)}`);
  }
  console.log('');

  console.log('═══════════════════════════════════════════════════════════');
  console.log('✅ Analysis complete!');
}

main().catch(console.error);
