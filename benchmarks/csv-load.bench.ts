/**
 * CSV Loading Benchmark
 *
 * Compares Molniya CSV loading performance against raw Node.js fs.
 * Uses real-world Online Retail II dataset.
 */

import { bench, group, run } from 'mitata';
import { readCsv } from '../src';
import { ensureDataset } from './setup';

// Ensure real datasets exist
const dataset2010 = await ensureDataset('retail-2010');
const dataset2011 = await ensureDataset('retail-2011');

console.log('\n📊 CSV Read Benchmarks\n');
console.log('='.repeat(60));

// Warm up
await readCsv(dataset2010);

group('Retail 2009-2010 (~44MB)', () => {
  bench('raw Bun.file.text', async () => {
    const content = await Bun.file(dataset2010).text();
    const lines = content.split('\n');
    return lines.length;
  });

  bench('Molniya readCsv', async () => {
    const { df } = await readCsv(dataset2010);
    return df.shape[0];
  });
});

group('Retail 2010-2011 (~45MB)', () => {
  bench('raw Bun.file.text', async () => {
    const content = await Bun.file(dataset2011).text();
    const lines = content.split('\n');
    return lines.length;
  });

  bench('Molniya readCsv', async () => {
    const { df } = await readCsv(dataset2011);
    return df.shape[0];
  });
});

await run({
  colors: true,
});

console.log('\n✅ Benchmark complete!');
