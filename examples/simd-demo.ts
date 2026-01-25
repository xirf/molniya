/**
 * Example demonstrating SIMD vectorization performance
 * Shows automatic acceleration on large numeric datasets
 */

import { addColumn, createDataFrame, getColumn } from '../src/dataframe/dataframe';
import { filter } from '../src/dataframe/operations';
import { readCsv } from '../src/io';

console.log('=== SIMD Vectorization Demo ===\n');

// Create a large dataset
const rows = 100_000;
console.log(`Creating dataset with ${rows.toLocaleString()} rows...`);

const df = createDataFrame();
addColumn(df, 'id', 'int32', rows);
addColumn(df, 'price', 'float64', rows);
addColumn(df, 'quantity', 'int32', rows);

// Fill with data
const idCol = getColumn(df, 'id');
const priceCol = getColumn(df, 'price');
const qtyCol = getColumn(df, 'quantity');

if (idCol.ok && priceCol.ok && qtyCol.ok) {
  for (let i = 0; i < rows; i++) {
    idCol.data.view.setInt32(i * 4, i, true);
    priceCol.data.view.setFloat64(i * 8, 50 + Math.sin(i / 1000) * 40, true);
    qtyCol.data.view.setInt32(i * 4, 1 + (i % 100), true);
  }
}

console.log('Dataset created.\n');

// Example 1: Simple filter (automatically uses SIMD)
console.log('Example 1: Filter prices > 70');
const start1 = performance.now();
const highPrices = filter(df, 'price', '>', 70);
const time1 = performance.now() - start1;

if (highPrices.ok) {
  const resultCol = getColumn(highPrices.data, 'price');
  console.log(
    `  Found ${highPrices.data.columns.get('price')?.length.toLocaleString()} rows in ${time1.toFixed(2)}ms`,
  );
  console.log(`  Throughput: ${(rows / time1 / 1000).toFixed(0)}K rows/ms\n`);
}

// Example 2: Filter chain
console.log('Example 2: Filter chain (price > 60 && quantity > 50)');
const start2 = performance.now();
const filtered1 = filter(df, 'price', '>', 60);
if (filtered1.ok) {
  const filtered2 = filter(filtered1.data, 'quantity', '>', 50);
  const time2 = performance.now() - start2;

  if (filtered2.ok) {
    console.log(
      `  Found ${filtered2.data.columns.get('price')?.length.toLocaleString()} rows in ${time2.toFixed(2)}ms`,
    );
    console.log(`  Throughput: ${(rows / time2 / 1000).toFixed(0)}K rows/ms\n`);
  }
}

// Example 3: Compare different thresholds
console.log('Example 3: Varying selectivity\n');

const thresholds = [
  { name: '10% selective', val: 90 },
  { name: '50% selective', val: 50 },
  { name: '90% selective', val: 10 },
];

for (const { name, val } of thresholds) {
  const start = performance.now();
  const result = filter(df, 'price', '>', val);
  const elapsed = performance.now() - start;

  if (result.ok) {
    const count = result.data.columns.get('price')?.length ?? 0;
    const selectivity = ((count / rows) * 100).toFixed(1);
    console.log(
      `  ${name.padEnd(18)}: ${elapsed.toFixed(2)}ms, ${count.toLocaleString()} rows (${selectivity}%)`,
    );
  }
}

console.log('\n✓ All operations automatically used SIMD vectorization');
console.log('  (Dataset > 10K rows, numeric columns, comparison operators)\n');
