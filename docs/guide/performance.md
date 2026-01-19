# Performance Guide

Molniya is built for speed, but how you use it affects performance. Here is how to keep your pipelines flying.

## 1. Filter Early, Filter Often

The most efficient data to process is data you don't process. Reduce the dataset size as early as possible in your chain.

**Bad:**
```typescript
df
  .sort('date')              // Sorting 1M rows
  .apply(complexMath)        // Calculating on 1M rows
  .filter(r => r.active);    // Oh wait, we only needed 10k rows
```

**Good:**
```typescript
df
  .filter(r => r.active)     // Drop 99% of rows immediately
  .sort('date')              // Sorting only 10k rows (Efficient)
  .apply(complexMath);       // Calculating on 10k rows
```

## 2. Select Columns Early

DataFrames store data in columns. If you have a 100-column CSV but only need 3, drop the rest immediately. This frees up massive amounts of memory.

```typescript
// Reading only what you need (if supported by loader)
// or select immediately after load
const df = await readCsv('huge.csv');
const workingSet = df.select('id', 'value', 'date');
// The other 97 columns are now eligible for garbage collection
```

## 3. Avoid Row Loops

Accessing data row-by-row is slower than column-based operations because of JavaScript object allocation overhead.

**Slow (Row-based):**
```typescript
// Creates 1 million objects only to derive one value
const total = df.rows().reduce((sum, row) => sum + row.price, 0);
```

**Column-based (Recommended):**
```typescript
// Uses a tight loop over a typed array
const total = df.col('price').sum();
```

## 4. Large Datasets? Use `scanCsv`

If your CSV is bigger than your RAM (e.g., 2GB+ file on a laptop), `readCsv` will crash. Use `scanCsv` to create a `LazyFrame`.

```typescript
// Doesn't load anything yet
const lazy = await scanCsv('massive_log.csv');

// Operations are recorded, not executed
const result = lazy
  .filter(row => row.status === 500)
  .select('url', 'latency')
  .head(100)  // We only need top 100
  .collect(); // NOW it reads just enough of the file to answer the query
```

> [!TIP]
> For server deployments, set global memory limits with `configure()`. This prevents any single operation from using all available RAM. See [Memory Limits](/guide/memory-limits).

## 5. Type Selection

- **Strings are expensive**: They take more memory and are slower to compare than numbers. If you have a categorical column like "Status" with values "OPEN", "CLOSED", consider mapping them to integers `0` and `1` if you are memory constraint.
- **Float64 vs Int32**: Molniya defaults to `float64` for numbers for safety. If you have massive arrays of small integers, forcing `int32` can save memory (though JS engines are erratic about this).

## 6. Byte-Level Optimizations (LazyFrame)

For extreme cases (e.g., 10GB+ CSVs on a machine with 4GB RAM), `LazyFrame` offers "raw" mode and aggressive GC.

```typescript
const lazy = await scanCsv('gigantic.csv', {
  lazyConfig: {
    raw: true,      // Keep data as Uint8Array (no string decoding overhead)
    forceGc: true,  // Force garbage collection after each chunk
  }
});
```

- **`raw: true`**: Skips the expensive UTF-8 decoding step. Filter operations run directly on bytes. Ideal if you are just filtering/selecting data to export a smaller subset.
- **`forceGc: true`**: Manually triggers the Garbage Collector after processing every chunk. Slows down execution but ensures memory usage stays absolute minimal.

## Benchmarking

Always measure.

```typescript
console.time('analysis');
// ... your code ...
console.timeEnd('analysis');
```

