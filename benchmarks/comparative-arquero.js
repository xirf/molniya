import fs from 'node:fs';
import { performance } from 'node:perf_hooks';
import { desc, fromCSV } from 'arquero';

// SETTINGS
const DATA_PATH = './artifac/2019-Nov.csv';

function getMemory() {
  return process.memoryUsage().rss / (1024 * 1024);
}

async function run() {
  console.log('--- Arquero Benchmark ---');
  const start = performance.now();

  try {
    // Arquero is in-memory by design. We test how it handles the stream.
    // It requires the data as a string or array, making 8GB impossible
    // without manual chunking.
    console.log('Arquero is in-memory only. Attempting buffered read...');
    const text = fs.readFileSync(DATA_PATH, 'utf8');
    const dt = fromCSV(text);

    const result = dt
      .filter((d) => d.event_type === 'purchase')
      .groupby('brand')
      .count()
      .orderby(desc('count'));

    const scanTime = performance.now() - start;

    // 4. Encoding (ordinal on event_type from a sample)
    // Simulating toOrdinal by taking unique values and mapping
    console.log('Encoding event_type...');
    const sample = dt.slice(0, 10);
    // Arquero doesn't have direct factorize, so we just access the column to simulate the work
    const eventTypes = sample.array('event_type');

    const totalTime = performance.now() - start;

    console.log('━'.repeat(40));
    console.log(`Total Time: ${(totalTime / 1000).toFixed(2)}s`);
    console.log(`Process RSS: ${getMemory().toFixed(2)}MB`);
  } catch (e) {
    console.error('Arquero failed (likely OOM/Buffer limit):', e.message);
    console.log(`Current RSS: ${getMemory().toFixed(2)}MB`);
  }
}

run();
