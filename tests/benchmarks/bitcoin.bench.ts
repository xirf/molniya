/**
 * Real-World Benchmark: Bitcoin 387MB
 * -----------------------------------
 * Data: https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data
 *
 * Molniya runs on Bun (its target runtime)
 * Arquero and Danfo.js run on Node (their target runtime)
 *
 * Usage: bun run bench
 */

import { $ } from 'bun';
import { readCsv } from '../../src';

const RUNS = 3;
const DATA_FILE = `${process.cwd()}/artifac/btcusd_1-min_data.csv`;

async function benchmarkMolniyaBun(filePath: string, runs: number): Promise<number[]> {
  const times: number[] = [];

  console.log('  Warm-up...');
  await readCsv(filePath, { maxRows: 1000 });

  for (let i = 0; i < runs; i++) {
    const start = performance.now();
    const { df } = await readCsv(filePath);
    const elapsed = performance.now() - start;
    times.push(elapsed);
    console.log(
      `  Run ${i + 1}: ${(elapsed / 1000).toFixed(2)}s (${df.shape[0].toLocaleString()} rows)`,
    );
  }

  return times;
}

async function benchmarkArqueroNode(filePath: string, runs: number): Promise<number[]> {
  const times: number[] = [];

  console.log('  Running (this takes ~12s per run)...');

  for (let i = 0; i < runs; i++) {
    try {
      const result = await $`node --max-old-space-size=8192 -e "
        import * as fs from 'node:fs';
        import * as aq from 'arquero';
        const start = performance.now();
        const content = fs.readFileSync('${filePath}', 'utf-8');
        const dt = aq.fromCSV(content);
        console.log(JSON.stringify({ elapsed: performance.now() - start, rows: dt.numRows() }));
      "`.text();

      const data = JSON.parse(result.trim());
      times.push(data.elapsed);
      console.log(
        `  Run ${i + 1}: ${(data.elapsed / 1000).toFixed(2)}s (${data.rows.toLocaleString()} rows)`,
      );
    } catch (e: unknown) {
      console.log(`  Run ${i + 1}: ❌ Failed`);
    }
  }

  return times;
}

async function benchmarkDanfoNode(filePath: string, runs: number): Promise<number[]> {
  const times: number[] = [];

  console.log('  Running (this takes ~70s per run)...');

  for (let i = 0; i < runs; i++) {
    try {
      const result = await $`node --max-old-space-size=8192 -e "
        import * as dfd from 'danfojs-node';
        const start = performance.now();
        const df = await dfd.readCSV('${filePath}');
        console.log(JSON.stringify({ elapsed: performance.now() - start, rows: df?.shape?.[0] ?? 0 }));
      "`.text();

      const data = JSON.parse(result.trim());
      times.push(data.elapsed);
      console.log(
        `  Run ${i + 1}: ${(data.elapsed / 1000).toFixed(2)}s (${data.rows.toLocaleString()} rows)`,
      );
    } catch (e: unknown) {
      console.log(`  Run ${i + 1}: ❌ Failed`);
    }
  }

  return times;
}

function stats(times: number[]): { avg: number; min: number } | null {
  if (times.length === 0) return null;
  return {
    avg: times.reduce((a, b) => a + b, 0) / times.length,
    min: Math.min(...times),
  };
}

async function main() {
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║   Real-World Benchmark: Bitcoin Historical Data          ║');
  console.log('║   387MB CSV / 7.38 Million Rows                          ║');
  console.log('║                                                          ║');
  console.log('║   Molniya → Bun (target runtime)                         ║');
  console.log('║   Arquero/Danfo → Node (their target runtime)            ║');
  console.log('╚══════════════════════════════════════════════════════════╝\n');

  console.log(`File: ${DATA_FILE}\n`);

  // Molniya on Bun
  console.log('📊 Molniya (readCsv) on Bun:');
  const molniyaTimes = await benchmarkMolniyaBun(DATA_FILE, RUNS);
  const molniyaStats = stats(molniyaTimes);
  console.log('');

  // Arquero on Node
  console.log('📊 Arquero on Node:');
  const arqueroTimes = await benchmarkArqueroNode(DATA_FILE, RUNS);
  const arqueroStats = stats(arqueroTimes);
  console.log('');

  // Danfo on Node (can skip with --skip-danfo flag since it's very slow)
  let danfoStats: { avg: number; min: number } | null = null;
  if (!process.argv.includes('--skip-danfo')) {
    console.log('📊 Danfo.js on Node:');
    const danfoTimes = await benchmarkDanfoNode(DATA_FILE, RUNS);
    danfoStats = stats(danfoTimes);
    console.log('');
  } else {
    console.log('📊 Danfo.js: Skipped (--skip-danfo flag)\n');
  }

  // Results
  console.log('┌────────────────────────────────────────────────────────────┐');
  console.log('│                        RESULTS                             │');
  console.log('├────────────────────────────────────────────────────────────┤');
  console.log('│ File:              387MB / 7.38M rows                      │');
  console.log('├────────────────────────────────────────────────────────────┤');

  if (molniyaStats) {
    console.log(
      `│ Molniya (Bun):     ${(molniyaStats.avg / 1000).toFixed(2).padStart(8)}s avg                      │`,
    );
  }
  if (arqueroStats) {
    console.log(
      `│ Arquero (Node):    ${(arqueroStats.avg / 1000).toFixed(2).padStart(8)}s avg                      │`,
    );
  }
  if (danfoStats) {
    console.log(
      `│ Danfo.js (Node):   ${(danfoStats.avg / 1000).toFixed(2).padStart(8)}s avg                      │`,
    );
  }

  console.log('├────────────────────────────────────────────────────────────┤');

  if (molniyaStats && arqueroStats) {
    const speedup = arqueroStats.avg / molniyaStats.avg;
    console.log(
      `│ vs Arquero:        ${speedup.toFixed(1).padStart(8)}x faster                     │`,
    );
  }
  if (molniyaStats && danfoStats) {
    const speedup = danfoStats.avg / molniyaStats.avg;
    console.log(
      `│ vs Danfo.js:       ${speedup.toFixed(1).padStart(8)}x faster                     │`,
    );
  }

  console.log('└────────────────────────────────────────────────────────────┘');

  console.log('\n✅ Benchmark complete!');
}

main().catch(console.error);
