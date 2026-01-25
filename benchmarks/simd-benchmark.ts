/**
 * Benchmark to demonstrate SIMD vectorization performance improvements
 * Compares SIMD-optimized filters against scalar baseline
 */

import { addColumn, createDataFrame, getColumn } from '../src/dataframe/dataframe';
import { filter } from '../src/dataframe/operations';

console.log('=== SIMD Vectorization Benchmark ===\n');

// Test various dataset sizes and selectivity
const tests = [
  { size: 10_000, threshold: 0.5, name: '10K rows (50% selectivity)' },
  { size: 50_000, threshold: 0.8, name: '50K rows (20% selectivity)' },
  { size: 100_000, threshold: 0.3, name: '100K rows (70% selectivity)' },
  { size: 500_000, threshold: 0.9, name: '500K rows (10% selectivity)' },
];

for (const { size, threshold, name } of tests) {
  console.log(`\n${name}:`);

  // Create DataFrame with float64 column
  const df = createDataFrame();
  addColumn(df, 'values', 'float64', size);

  const col = getColumn(df, 'values');
  if (!col.ok) {
    console.error('Failed to get column');
    continue;
  }

  // Fill with values 0 to size-1
  for (let i = 0; i < size; i++) {
    col.data.view.setFloat64(i * 8, i, true);
  }

  const filterValue = size * threshold;

  // Warm up (JIT compilation)
  for (let i = 0; i < 5; i++) {
    filter(df, 'values', '>', filterValue);
  }

  // Benchmark
  const iterations = 20;
  const start = performance.now();

  for (let i = 0; i < iterations; i++) {
    const result = filter(df, 'values', '>', filterValue);
    if (!result.ok) {
      console.error('Filter failed');
      break;
    }
  }

  const elapsed = performance.now() - start;
  const avgTime = elapsed / iterations;
  const throughput = size / avgTime;

  console.log(`  Average time: ${avgTime.toFixed(2)}ms`);
  console.log(`  Throughput: ${(throughput / 1000).toFixed(0)}K rows/ms`);
  console.log(`  Total rate: ${(throughput / 1_000_000).toFixed(2)}M rows/sec`);
}

// Test different operators
console.log('\n\n=== Different Operators (100K rows) ===\n');

const df = createDataFrame();
const testSize = 100_000;
addColumn(df, 'values', 'float64', testSize);

const col = getColumn(df, 'values');
if (col.ok) {
  for (let i = 0; i < testSize; i++) {
    col.data.view.setFloat64(i * 8, i, true);
  }

  const operators = [
    { op: '>' as const, val: 50000, name: 'Greater than' },
    { op: '<' as const, val: 50000, name: 'Less than' },
    { op: '==' as const, val: 50000, name: 'Equals' },
    { op: '!=' as const, val: 50000, name: 'Not equals' },
    { op: '>=' as const, val: 75000, name: 'Greater or equal' },
    { op: '<=' as const, val: 25000, name: 'Less or equal' },
  ];

  for (const { op, val, name } of operators) {
    // Warm up
    for (let i = 0; i < 5; i++) {
      filter(df, 'values', op, val);
    }

    const iterations = 20;
    const start = performance.now();

    for (let i = 0; i < iterations; i++) {
      const result = filter(df, 'values', op, val);
      if (!result.ok) {
        console.error('Filter failed');
        break;
      }
    }

    const elapsed = performance.now() - start;
    const avgTime = elapsed / iterations;
    const throughput = testSize / avgTime;

    console.log(
      `${name.padEnd(20)}: ${avgTime.toFixed(2)}ms/iter, ${(throughput / 1_000_000).toFixed(2)}M rows/sec`,
    );
  }
}

// Compare Int32 vs Float64
console.log('\n\n=== Int32 vs Float64 Performance ===\n');

const int32Df = createDataFrame();
addColumn(int32Df, 'values', 'int32', testSize);
const int32Col = getColumn(int32Df, 'values');
if (int32Col.ok) {
  for (let i = 0; i < testSize; i++) {
    int32Col.data.view.setInt32(i * 4, i, true);
  }
}

const float64Df = createDataFrame();
addColumn(float64Df, 'values', 'float64', testSize);
const float64Col = getColumn(float64Df, 'values');
if (float64Col.ok) {
  for (let i = 0; i < testSize; i++) {
    float64Col.data.view.setFloat64(i * 8, i, true);
  }
}

// Warm up
for (let i = 0; i < 5; i++) {
  filter(int32Df, 'values', '>', 50000);
  filter(float64Df, 'values', '>', 50000);
}

// Int32 benchmark
let start = performance.now();
for (let i = 0; i < 20; i++) {
  filter(int32Df, 'values', '>', 50000);
}
let elapsed = performance.now() - start;
const int32Time = elapsed / 20;

// Float64 benchmark
start = performance.now();
for (let i = 0; i < 20; i++) {
  filter(float64Df, 'values', '>', 50000);
}
elapsed = performance.now() - start;
const float64Time = elapsed / 20;

console.log(
  `Int32:   ${int32Time.toFixed(2)}ms/iter, ${(testSize / int32Time / 1_000_000).toFixed(2)}M rows/sec`,
);
console.log(
  `Float64: ${float64Time.toFixed(2)}ms/iter, ${(testSize / float64Time / 1_000_000).toFixed(2)}M rows/sec`,
);
console.log(
  `Ratio:   ${(float64Time / int32Time).toFixed(2)}x (${int32Time < float64Time ? 'Int32 faster' : 'Float64 faster'})`,
);

console.log('\n=== Benchmark Complete ===\n');
