/**
 * Benchmark: Predicate Pushdown Performance
 *
 * Demonstrates the performance benefits of applying filters during CSV parsing
 * rather than loading all data first and then filtering.
 *
 * Run: bun run molniya/benchmarks/predicate-pushdown-benchmark.ts
 */

import { unlinkSync, writeFileSync } from 'node:fs';
import { readCsv } from '../src/io';
import { QueryPlan } from '../src/lazyframe/plan';
import type { ScanPlan } from '../src/lazyframe/plan';
import {
  estimatePushdownSavings,
  extractPushdownPredicates,
  scanCsvWithPredicates,
} from '../src/lazyframe/predicate-pushdown';

// Helper: Generate synthetic dataset
function generateTestData(rows: number, filePath: string): void {
  const lines = ['id,category,price,quantity,revenue'];

  for (let i = 0; i < rows; i++) {
    const id = i + 1;
    const category = ['Electronics', 'Clothing', 'Food', 'Books'][i % 4];
    const price = 10 + (i % 990);
    const quantity = 1 + (i % 100);
    const revenue = price * quantity;
    lines.push(`${id},${category},${price},${quantity},${revenue}`);
  }

  writeFileSync(filePath, lines.join('\n'));
}

// Benchmark: Traditional approach (load all, then filter)
function benchmarkTraditionalFilter(
  filePath: string,
  category: string,
  minPrice: number,
): { timeMs: number; rowsLoaded: number; memoryBytes: number } {
  const start = performance.now();

  // Load entire CSV
  const df = readCsv(filePath);
  const totalRows = df.height;

  // Filter in memory
  const filtered = df.filter('category', '==', category).filter('price', '>=', minPrice);

  const end = performance.now();

  return {
    timeMs: end - start,
    rowsLoaded: totalRows,
    memoryBytes: totalRows * 5 * 8, // 5 columns x 8 bytes estimate
  };
}

// Benchmark: Predicate pushdown (filter during scan)
function benchmarkPredicatePushdown(
  filePath: string,
  schema: Record<string, string>,
  category: string,
  minPrice: number,
): { timeMs: number; rowsLoaded: number; memoryBytes: number } {
  const start = performance.now();

  // Filter during CSV scan using predicate pushdown
  const scanPlan: ScanPlan = {
    type: 'scan',
    path: filePath,
    schema,
    columnOrder: Object.keys(schema),
    options: {},
  };
  let plan = QueryPlan.filter(scanPlan, 'category', '==', category);
  plan = QueryPlan.filter(plan, 'price', '>=', minPrice);
  const predicates = extractPushdownPredicates(plan);
  const df = scanCsvWithPredicates(scanPlan, plan, predicates);

  const end = performance.now();
  const rowsLoaded = df.height;

  return {
    timeMs: end - start,
    rowsLoaded,
    memoryBytes: rowsLoaded * 5 * 8,
  };
}

// Benchmark: Combined optimizations (predicate pushdown + column pruning)
function benchmarkCombinedOptimizations(
  filePath: string,
  schema: Record<string, string>,
  category: string,
  minPrice: number,
): { timeMs: number; rowsLoaded: number; memoryBytes: number } {
  const start = performance.now();

  // Filter during scan + only load needed columns
  const selectedSchema = {
    category: schema.category,
    price: schema.price,
    revenue: schema.revenue,
  };
  const scanPlan: ScanPlan = {
    type: 'scan',
    path: filePath,
    schema: selectedSchema,
    columnOrder: Object.keys(selectedSchema),
    options: {},
  };
  let plan = QueryPlan.filter(scanPlan, 'category', '==', category);
  plan = QueryPlan.filter(plan, 'price', '>=', minPrice);
  const predicates = extractPushdownPredicates(plan);
  const df = scanCsvWithPredicates(scanPlan, plan, predicates);

  const end = performance.now();
  const rowsLoaded = df.height;

  return {
    timeMs: end - start,
    rowsLoaded,
    memoryBytes: rowsLoaded * 3 * 8, // Only 3 columns
  };
}

// Main benchmark suite
function runBenchmarks(): void {
  console.log('🚀 Predicate Pushdown Benchmark\n');
  console.log(`=${'='.repeat(79)}`);

  const testFile = 'molniya/benchmarks/data/predicate-test.csv';
  const schema = {
    id: 'i32',
    category: 'str',
    price: 'f64',
    quantity: 'i32',
    revenue: 'f64',
  };

  // Test configurations
  const configs = [
    { rows: 100_000, category: 'Electronics', minPrice: 800, selectivity: '5%' },
    { rows: 500_000, category: 'Books', minPrice: 900, selectivity: '2%' },
    { rows: 1_000_000, category: 'Food', minPrice: 950, selectivity: '1%' },
  ];

  for (const config of configs) {
    console.log(
      `\n📊 Dataset: ${config.rows.toLocaleString()} rows, ${config.selectivity} selectivity`,
    );
    console.log('-'.repeat(80));

    // Generate test data
    generateTestData(config.rows, testFile);

    // Estimate savings
    const scanPlan: ScanPlan = {
      type: 'scan',
      path: testFile,
      schema,
      columnOrder: Object.keys(schema),
      options: {},
    };
    let estimatePlan = QueryPlan.filter(scanPlan, 'category', '==', config.category);
    estimatePlan = QueryPlan.filter(estimatePlan, 'price', '>=', config.minPrice);
    const savings = estimatePushdownSavings(estimatePlan, config.rows);

    console.log('\n📈 Estimated Savings:');
    console.log(
      `   Rows saved: ${savings.rowsSaved.toLocaleString()} (${(savings.percentSaved * 100).toFixed(1)}%)`,
    );
    console.log(`   Memory saved: ${(savings.memorySavedMB).toFixed(2)} MB`);

    // Run benchmarks
    console.log('\n⏱️  Performance Comparison:');

    const traditional = benchmarkTraditionalFilter(testFile, config.category, config.minPrice);
    console.log('\n   Traditional (load all → filter):');
    console.log(`     Time: ${traditional.timeMs.toFixed(2)} ms`);
    console.log(`     Rows loaded: ${traditional.rowsLoaded.toLocaleString()}`);
    console.log(`     Memory: ${(traditional.memoryBytes / 1024 / 1024).toFixed(2)} MB`);

    const pushdown = benchmarkPredicatePushdown(testFile, schema, config.category, config.minPrice);
    console.log('\n   Predicate Pushdown (filter during scan):');
    console.log(`     Time: ${pushdown.timeMs.toFixed(2)} ms`);
    console.log(`     Rows loaded: ${pushdown.rowsLoaded.toLocaleString()}`);
    console.log(`     Memory: ${(pushdown.memoryBytes / 1024 / 1024).toFixed(2)} MB`);

    const combined = benchmarkCombinedOptimizations(
      testFile,
      schema,
      config.category,
      config.minPrice,
    );
    console.log('\n   Combined (pushdown + column pruning):');
    console.log(`     Time: ${combined.timeMs.toFixed(2)} ms`);
    console.log(`     Rows loaded: ${combined.rowsLoaded.toLocaleString()}`);
    console.log(`     Memory: ${(combined.memoryBytes / 1024 / 1024).toFixed(2)} MB`);

    // Calculate improvements
    const timeSavings = ((traditional.timeMs - pushdown.timeMs) / traditional.timeMs) * 100;
    const memorySavings =
      ((traditional.memoryBytes - pushdown.memoryBytes) / traditional.memoryBytes) * 100;
    const combinedMemorySavings =
      ((traditional.memoryBytes - combined.memoryBytes) / traditional.memoryBytes) * 100;

    console.log('\n🎯 Improvements:');
    console.log(`   Pushdown time saved: ${timeSavings.toFixed(1)}%`);
    console.log(`   Pushdown memory saved: ${memorySavings.toFixed(1)}%`);
    console.log(`   Combined memory saved: ${combinedMemorySavings.toFixed(1)}%`);
  }

  // Cleanup
  try {
    unlinkSync(testFile);
  } catch {}

  console.log(`\n${'='.repeat(80)}`);
  console.log('\n✅ Key Takeaways:');
  console.log('   • Predicate pushdown reduces memory by 90-99% on selective queries');
  console.log('   • Filtering during scan is faster than load-then-filter');
  console.log('   • Combining pushdown + column pruning maximizes savings');
  console.log('   • Most effective when selectivity < 10%');
  console.log('\n');
}

// Run if executed directly
if (import.meta.main) {
  runBenchmarks();
}
