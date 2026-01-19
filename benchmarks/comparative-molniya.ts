import { performance } from 'node:perf_hooks';
import { configure, getMemoryStats, scanCsv } from '../src';

// SETTINGS
const DATA_PATH = './artifac/2019-Nov.csv';
const MEMORY_LIMIT = 100 * 1024 * 1024; // 100MB

configure({ globalLimitBytes: MEMORY_LIMIT });

async function run() {
  console.log('--- Molniya Benchmark ---');
  const start = performance.now();

  // 2. Filter & Aggregate (Streaming)
  console.log('Streaming: Filter & Aggregate...');
  const brandCounts = new Uint32Array(10000000);
  const globalBrandDict = new Map<string, number>();
  let nextId = 0;

  // Create 50k chunks (faster)
  const lazy = await scanCsv(DATA_PATH, {
    lazyConfig: {
      chunkSize: 50_000,
      maxCacheMemory: 50 * 1024 * 1024,
      raw: true, // Enable byte-level parsing
      forceGc: true,
    },
  });

  const decoder = new TextDecoder();
  const purchaseBytes = new TextEncoder().encode('purchase');

  for await (const chunk of lazy) {
    // 1. Filter & Aggregate (Zero-Copy)
    const brands = chunk.col('brand');
    const events = chunk.col('event_type');
    const len = chunk.shape[0];

    for (let i = 0; i < len; i++) {
      // Check condition WITHOUT creating a new DataFrame
      // Byte-level comparison
      const eventBytes = events.at(i) as unknown as Uint8Array;

      // Compare 'purchase' (8 bytes)
      let matches = false;
      if (eventBytes && eventBytes.length === 8) {
        matches = true;
        for (let j = 0; j < 8; j++) {
          if (eventBytes[j] !== purchaseBytes[j]) {
            matches = false;
            break;
          }
        }
      }

      if (matches) {
        const brandBytes = brands.at(i) as unknown as Uint8Array;
        // Optimization: Use a quick hash or just decode since we need the key
        // Ideally we'd have a Trie, but for now decode is better than storing all strings in DF
        const brandStr = decoder.decode(brandBytes);

        // 1. Check if we've seen this brand globally
        let id = globalBrandDict.get(brandStr);
        if (id === undefined) {
          id = nextId++;
          globalBrandDict.set(brandStr, id);
        }

        // 2. Count the INTEGER, not the string
        brandCounts[id]!++;
      }
    }

    // 3. Clear memory explicitly for benchmark accuracy
    // @ts-ignore
    if (global.gc) global.gc();
  }

  console.log(`Processed ${lazy.shape[0]} rows.`);
  console.log(`Unique Brands: ${globalBrandDict.size}`);

  const scanTime = performance.now() - start;

  // 4. Encoding (ordinal on event_type from a sample)
  console.log('Encoding event_type...');
  const sample = await lazy.head(10);
  const encoded = sample.toOrdinal('event_type');

  const totalTime = performance.now() - start;
  const stats = getMemoryStats();

  console.log('━'.repeat(40));
  console.log(`Scan Time: ${(scanTime / 1000).toFixed(2)}s`);
  console.log(`Total Time: ${(totalTime / 1000).toFixed(2)}s`);
  console.log(`Peak Tracker Memory: ${(stats.totalUsedBytes / 1024 / 1024).toFixed(2)}MB`);
  console.log(`Process RSS: ${(process.memoryUsage().rss / 1024 / 1024).toFixed(2)}MB`);
}

run().catch(console.error);
