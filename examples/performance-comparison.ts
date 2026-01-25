/**
 * Performance comparison: LazyFrame vs Direct DataFrame operations
 *
 * This benchmark demonstrates the overhead (or lack thereof) of using
 * LazyFrame for query building vs direct DataFrame operations.
 */

import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { DType, LazyFrame, filter, getRowCount, groupby, readCsv, select } from '../src';

async function main() {
  const tmpDir = mkdtempSync(join(tmpdir(), 'molniya-compare-'));

  try {
    // Create test data: 100K rows
    const csvPath = join(tmpDir, 'data.csv');
    const rows = 100_000;
    let csvContent = 'id,price,volume,category\n';
    for (let i = 0; i < rows; i++) {
      csvContent += `${i},${50000 + Math.random() * 10000},${10 + (i % 100)},cat${i % 20}\n`;
    }
    writeFileSync(csvPath, csvContent);

    const schema = {
      id: DType.Int32,
      price: DType.Float64,
      volume: DType.Int32,
      category: DType.String,
    };

    console.log('🔥 Performance Comparison: LazyFrame vs Direct Operations');
    console.log(`Dataset: ${rows.toLocaleString()} rows\n`);

    // Test 1: LazyFrame approach
    console.log('--- LazyFrame (Declarative Query) ---');
    const lazyStart = performance.now();

    const lazyResult = await LazyFrame.scanCsv(csvPath, schema)
      .filter('price', '>', 52000)
      .filter('volume', '>', 50)
      .select(['category', 'volume'])
      .groupby(['category'], [{ col: 'volume', func: 'sum', outName: 'total_volume' }])
      .collect();

    const lazyDuration = performance.now() - lazyStart;

    if (lazyResult.ok) {
      const lazyRows = getRowCount(lazyResult.data);
      console.log(`✅ Result: ${lazyRows} groups`);
      console.log(`⏱️  Time: ${lazyDuration.toFixed(2)}ms`);
      console.log(`🚀 Rate: ${((rows / lazyDuration) * 1000).toFixed(0)} rows/sec\n`);
    }

    // Test 2: Direct DataFrame operations
    console.log('--- Direct DataFrame Operations ---');
    const directStart = performance.now();

    const csvData = csvContent;
    const readResult = await readCsv(csvData, { schema });
    if (!readResult.ok) {
      console.error('Failed to read CSV:', readResult.error);
      return;
    }

    let df = readResult.data;

    // Apply operations manually
    const filter1 = filter(df, 'price', '>', 52000);
    if (!filter1.ok) {
      console.error('Filter 1 failed');
      return;
    }
    df = filter1.data;

    const filter2 = filter(df, 'volume', '>', 50);
    if (!filter2.ok) {
      console.error('Filter 2 failed');
      return;
    }
    df = filter2.data;

    const selectResult = select(df, ['category', 'volume']);
    if (!selectResult.ok) {
      console.error('Select failed');
      return;
    }
    df = selectResult.data;

    const groupbyResult = groupby(
      df,
      ['category'],
      [{ col: 'volume', func: 'sum', outName: 'total_volume' }],
    );

    const directDuration = performance.now() - directStart;

    if (groupbyResult.ok) {
      const directRows = getRowCount(groupbyResult.data);
      console.log(`✅ Result: ${directRows} groups`);
      console.log(`⏱️  Time: ${directDuration.toFixed(2)}ms`);
      console.log(`🚀 Rate: ${((rows / directDuration) * 1000).toFixed(0)} rows/sec\n`);
    }

    // Compare
    console.log('--- Comparison ---');
    const overhead = ((lazyDuration - directDuration) / directDuration) * 100;
    console.log(`LazyFrame overhead: ${overhead > 0 ? '+' : ''}${overhead.toFixed(1)}%`);

    if (Math.abs(overhead) < 5) {
      console.log('✨ Negligible overhead - LazyFrame is essentially free!');
    } else if (overhead < 0) {
      console.log('🎉 LazyFrame is actually FASTER (likely due to query planning)');
    } else if (overhead < 10) {
      console.log('✅ Minimal overhead - LazyFrame benefits outweigh the cost');
    }

    console.log('\n💡 LazyFrame Benefits:');
    console.log('  • Declarative query building');
    console.log('  • Query plan visualization (explain())');
    console.log('  • Schema inference');
    console.log('  • Future optimization potential (predicate pushdown, column pruning)');
  } finally {
    rmSync(tmpDir, { recursive: true, force: true });
  }
}

main().catch(console.error);
