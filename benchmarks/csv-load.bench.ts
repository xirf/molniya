/**
 * CSV Loading Benchmark
 * 
 * Compares Mornye CSV loading performance against raw Node.js fs.
 * Uses real-world Online Retail II dataset.
 */

import { bench, group, run } from 'mitata';
import * as fs from 'node:fs';
import { ensureDataset } from './setup';
import { readCsv } from '../src';

// Ensure real datasets exist
const dataset2010 = await ensureDataset('retail-2010');
const dataset2011 = await ensureDataset('retail-2011');

console.log('\n📊 CSV Loading Benchmarks (Real World Data)\n');
console.log('=' .repeat(60));

// Warm up
await readCsv(dataset2010);

group('Retail 2009-2010 (~44MB)', () => {
  bench('raw fs.readFileSync', () => {
    const content = fs.readFileSync(dataset2010, 'utf-8');
    const lines = content.split('\n');
    return lines.length;
  });

  bench('Mornye readCsv', async () => {
    const { df } = await readCsv(dataset2010);
    return df.shape[0];
  });
});

group('Retail 2010-2011 (~45MB)', () => {
  bench('raw fs.readFileSync', () => {
    const content = fs.readFileSync(dataset2011, 'utf-8');
    const lines = content.split('\n');
    return lines.length;
  });

  bench('Mornye readCsv', async () => {
    const { df } = await readCsv(dataset2011);
    return df.shape[0];
  });
});

await run({
  colors: true,
});

console.log('\n✅ Benchmark complete!');
