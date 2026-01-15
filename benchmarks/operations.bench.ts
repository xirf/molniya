/**
 * DataFrame Operations Benchmark
 * 
 * Benchmarks common DataFrame operations using real-world data from Kaggle (Online Retail II).
 * 
 * Note: The dataset contains some dirty data (e.g. "TEST" in Quantity column), so
 * columns like 'Quantity' and 'Price' are inferred as strings.
 * We use 'Invoice' (float64) for numeric benchmarks and demonstrate cleaning for others.
 */

import { bench, group, run } from 'mitata';
import { ensureDataset } from './setup';
import { readCsv } from '../src';

// Load dataset
const dataPath = await ensureDataset('retail-2010');
console.log('\n📊 Loading dataset for operations benchmark...');
const { df } = await readCsv(dataPath);
console.log(`   Loaded ${df.shape[0].toLocaleString()} rows × ${df.shape[1]} columns\n`);
console.log('=' .repeat(60));

// Prepare columns
const invoiceCol = df.col('Invoice'); // Already float64
const quantityRaw = df.col('Quantity'); // String due to dirty data

group('DataFrame Operations', () => {
  bench('head(10)', () => {
    return df.head(10);
  });

  bench('tail(10)', () => {
    return df.tail(10);
  });

  bench('select(2 columns)', () => {
    return df.select('Invoice', 'Description');
  });

  bench('filter(numeric)', () => {
    // Filter on the numeric Invoice column
    return df.filter((row) => (row as unknown as { Invoice: number }).Invoice > 500000);
  });

  bench('sort(numeric)', () => {
    return df.sort('Invoice', false);
  });

  bench('describe()', () => {
    return df.describe();
  });
});

group('Series Operations', () => {
  bench('astype(float64)', () => {
    return quantityRaw.astype('float64');
  });

  bench('sum()', () => {
    return invoiceCol.sum();
  });

  bench('mean()', () => {
    return invoiceCol.mean();
  });

  bench('min()', () => {
    return invoiceCol.min();
  });

  bench('max()', () => {
    return invoiceCol.max();
  });

  bench('std()', () => {
    return invoiceCol.std();
  });

  bench('filter(numeric)', () => {
    return invoiceCol.filter((v) => (v as number) > 500000);
  });
});

await run({
  colors: true,
});

console.log('\n✅ Operations benchmark complete!');
