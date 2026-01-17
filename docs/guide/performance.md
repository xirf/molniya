# Performance Guide

Molniya is built for speed, but how you use it affects performance. Here is how to keep your pipelines flying.

## 1. Filter Early, Filter Often

The fastest data to process is data you don't process. Reduce the dataset size as early as possible in your chain.

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
  .sort('date')              // Sorting only 10k rows (FAST)
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

**Fast (Column-based):**
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

## 5. Type Selection

- **Strings are expensive**: They take more memory and are slower to compare than numbers. If you have a categorical column like "Status" with values "OPEN", "CLOSED", consider mapping them to integers `0` and `1` if you are memory constraint.
- **Float64 vs Int32**: Molniya defaults to `float64` for numbers for safety. If you have massive arrays of small integers, forcing `int32` can save memory (though JS engines are erratic about this).

## Benchmarking

Always measure.

```typescript
console.time('analysis');
// ... your code ...
console.timeEnd('analysis');
```

