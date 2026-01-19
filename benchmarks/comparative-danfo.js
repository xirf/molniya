import dfd from 'danfojs-node';
const { performance } = require('node:perf_hooks');

// SETTINGS
const DATA_PATH = './artifac/2019-Nov.csv';

function getMemory() {
  return process.memoryUsage().rss / (1024 * 1024);
}

async function run() {
  console.log('--- Danfo.js Benchmark ---');
  const start = performance.now();

  try {
    // Danfo.js doesn't have a built-in streaming/lazy API for 8GB files
    // that handles group_by/agg without loading, but we test readCsv.
    // Usually it crashes near 1-2GB.
    console.log('Attempting to read CSV (Danfo)...');
    const df = await dfd.readCSV(DATA_PATH);

    console.log('Filtering...');
    const filtered = df.query(df.event_type.eq('purchase'));

    console.log('Aggregation...');
    const brandCounts = filtered.groupby(['brand']).col(['brand']).count();

    // 4. Encoding
    console.log('Encoding event_type...');
    const sample = df.head(10);
    // Danfo has a LabelEncoder, we simulate the work
    const encoder = new dfd.LabelEncoder();
    encoder.fit(sample.event_type);
    const encoded = encoder.transform(sample.event_type);

    const totalTime = performance.now() - start;

    console.log('━'.repeat(40));
    console.log(`Total Time: ${(totalTime / 1000).toFixed(2)}s`);
    console.log(`Process RSS: ${getMemory().toFixed(2)}MB`);
  } catch (e) {
    console.error('Danfo.js failed (likely OOM):', e.message);
    console.log(`Current RSS: ${getMemory().toFixed(2)}MB`);
  }
}

run();
