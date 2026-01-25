/**
 * Benchmark demonstrating column pruning performance improvements
 * Shows speedup when selecting few columns from wide tables
 */

import { unlinkSync, writeFileSync } from 'node:fs';
import { readCsv } from '../src/io/csv-reader';
import { estimatePruningSavings, scanCsvWithPruning } from '../src/lazyframe/csv-pruning';
import type { DType } from '../src/types/dtypes';

console.log('=== Column Pruning Benchmark ===\n');

// Create wide CSV file (20 columns)
const numColumns = 20;
const numRows = 50_000;

console.log(`Creating test CSV: ${numRows.toLocaleString()} rows x ${numColumns} columns`);

// Generate headers
const headers = Array.from({ length: numColumns }, (_, i) => `col${i}`).join(',');

// Generate rows
const rows: string[] = [headers];
for (let r = 0; r < numRows; r++) {
  const row = Array.from({ length: numColumns }, (_, i) => (r * numColumns + i).toString()).join(
    ',',
  );
  rows.push(row);
}

const csvContent = rows.join('\n');
const testFile = './wide-table-benchmark.csv';
writeFileSync(testFile, csvContent);

console.log(`Test file created: ${(csvContent.length / 1024 / 1024).toFixed(2)} MB\n`);

try {
  // Test 1: Load all columns (baseline)
  console.log('Test 1: Load ALL columns (baseline)');

  // Build schema for all columns
  const allColumnsSchema: Record<string, DType> = {};
  for (let i = 0; i < numColumns; i++) {
    allColumnsSchema[`col${i}`] = 'int32';
  }

  const start1 = performance.now();

  for (let i = 0; i < 5; i++) {
    const result = await readCsv(csvContent, { schema: allColumnsSchema });
    if (!result.ok) {
      console.error('Failed to load all columns');
      break;
    }
  }

  const time1 = (performance.now() - start1) / 5;
  console.log(`  Average time: ${time1.toFixed(2)}ms`);
  console.log(`  Throughput: ${(numRows / time1 / 1000).toFixed(0)}K rows/ms\n`);

  // Test 2: Load only 3 columns (15% of columns)
  console.log('Test 2: Load ONLY 3 columns (15% pruning ratio)');
  const requiredCols = new Set(['col0', 'col5', 'col10']);

  const start2 = performance.now();

  for (let i = 0; i < 5; i++) {
    const result = scanCsvWithPruning(testFile, {
      requiredColumns: requiredCols,
      schema: new Map([
        ['col0', 'int32'],
        ['col5', 'int32'],
        ['col10', 'int32'],
      ]),
    });
    if (!result.ok) {
      console.error('Failed to load pruned columns');
      break;
    }
  }

  const time2 = (performance.now() - start2) / 5;
  console.log(`  Average time: ${time2.toFixed(2)}ms`);
  console.log(`  Throughput: ${(numRows / time2 / 1000).toFixed(0)}K rows/ms`);
  console.log(`  Speedup: ${(time1 / time2).toFixed(2)}x\n`);

  // Test 3: Load only 1 column (5% pruning ratio)
  console.log('Test 3: Load ONLY 1 column (5% pruning ratio)');
  const singleCol = new Set(['col10']);

  const start3 = performance.now();

  for (let i = 0; i < 5; i++) {
    const result = scanCsvWithPruning(testFile, {
      requiredColumns: singleCol,
      schema: new Map([['col10', 'int32']]),
    });
    if (!result.ok) {
      console.error('Failed to load single column');
      break;
    }
  }

  const time3 = (performance.now() - start3) / 5;
  console.log(`  Average time: ${time3.toFixed(2)}ms`);
  console.log(`  Throughput: ${(numRows / time3 / 1000).toFixed(0)}K rows/ms`);
  console.log(`  Speedup: ${(time1 / time3).toFixed(2)}x\n`);

  // Memory savings estimation
  console.log('=== Memory Savings Estimation ===\n');

  const scenarios = [
    { name: '3 of 20 columns (15%)', required: 3 },
    { name: '5 of 20 columns (25%)', required: 5 },
    { name: '1 of 20 columns (5%)', required: 1 },
  ];

  for (const { name, required } of scenarios) {
    const savings = estimatePruningSavings(numColumns, required, numRows, 8);
    console.log(`${name}:`);
    console.log(`  Total memory: ${(savings.totalBytes / 1024 / 1024).toFixed(2)} MB`);
    console.log(`  Required memory: ${(savings.requiredBytes / 1024 / 1024).toFixed(2)} MB`);
    console.log(`  Savings: ${savings.savingsPercent.toFixed(1)}%\n`);
  }

  // Real-world scenario
  console.log('=== Real-World Scenario ===\n');
  console.log('Wide table: 100 columns, 1M rows');
  console.log('Query: SELECT col1, col5, col25 WHERE col25 > 1000\n');

  const wideScenario = estimatePruningSavings(100, 3, 1_000_000, 8);
  console.log(`Without pruning: ${(wideScenario.totalBytes / 1024 / 1024).toFixed(2)} MB`);
  console.log(`With pruning: ${(wideScenario.requiredBytes / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Memory saved: ${(wideScenario.savingsPercent).toFixed(1)}%`);
  console.log(`Estimated speedup: ${(100 / 3).toFixed(1)}x (proportional to column reduction)\n`);
} finally {
  // Cleanup
  unlinkSync(testFile);
  console.log('✓ Benchmark complete');
}
