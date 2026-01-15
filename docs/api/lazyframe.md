# LazyFrame API

`LazyFrame` is a memory-efficient alternative to `DataFrame` for working with large CSV files. It loads data on-demand instead of reading the entire file into memory.

## When to Use LazyFrame

| Scenario                           | Recommended                                         |
| ---------------------------------- | --------------------------------------------------- |
| File fits comfortably in RAM       | `readCsv()` → `DataFrame`                           |
| File size approaches available RAM | `scanCsv()` → `LazyFrame`                           |
| Only need a subset of rows         | `scanCsv()` → `LazyFrame`                           |
| Need to process entire file        | Either works, but `LazyFrame` uses less peak memory |

## Creation

```typescript
import { scanCsv } from 'mornye';

// Create a LazyFrame (does not load all data)
const lazy = await scanCsv('./large_file.csv');

// With options
const lazy2 = await scanCsv('./data.csv', {
  delimiter: 44,        // comma (default)
  hasHeader: true,      // default
  sampleRows: 100,      // rows to sample for type inference
  lazyConfig: {
    maxCacheMemory: 100 * 1024 * 1024, // 100MB cache (default)
    chunkSize: 10_000,                  // rows per chunk (default)
  }
});
```

## Properties

| Property | Type           | Description  |
| -------- | -------------- | ------------ |
| `shape`  | `[rows, cols]` | Dimensions   |
| `schema` | `Schema`       | Column types |
| `path`   | `string`       | File path    |

## Methods Overview

All data-returning methods are `async` since they may need to load data from disk.

### head / tail

```typescript
// Get first n rows as DataFrame
const first5 = await lazy.head(5);

// Get last n rows as DataFrame  
const last10 = await lazy.tail(10);
```

### select

Returns a new LazyFrame with only the specified columns. Does not load data.

```typescript
const subset = lazy.select('id', 'name', 'price');
const df = await subset.head(10);
```

### filter

Filters rows by predicate. Processes data in chunks to limit memory usage.

```typescript
const filtered = await lazy.filter(row => row.price > 100);
```

::: info
Unlike `DataFrame.filter()` which returns a `DataFrame`, `LazyFrame.filter()` processes the entire file and returns a `DataFrame` with matching rows.
:::

### collect

Loads all (or limited) data into a regular `DataFrame`.

```typescript
// Load everything (use with caution for large files)
const df = await lazy.collect();

// Load only first n rows
const sample = await lazy.collect(1000);
```

::: warning
Calling `collect()` without a limit on a large file will load all data into memory.
:::

### info

Returns metadata about the LazyFrame.

```typescript
const info = lazy.info();
// {
//   rows: 1000000,
//   columns: 5,
//   dtypes: { id: 'int32', name: 'string', ... },
//   cached: 2  // number of cached chunks
// }
```

### columns

Returns column names.

```typescript
lazy.columns();  // ['id', 'name', 'price', ...]
```

### print

Displays a sample of data to console.

```typescript
await lazy.print();
// LazyFrame [./data.csv]
// ┌────────┬────────┬─────────┐
// │   id   │  name  │  price  │
// ├────────┼────────┼─────────┤
// │      1 │  Alice │   10.50 │
// │    ... │    ... │     ... │
// └────────┴────────┴─────────┘
// ... 999990 more rows
```

### clearCache

Clears the internal row cache to free memory.

```typescript
lazy.clearCache();
```

## Example: Processing a Large File

```typescript
import { scanCsv } from 'mornye';

// 1. Open file lazily
const lazy = await scanCsv('./transactions.csv');
console.log(`File has ${lazy.shape[0]} rows`);

// 2. Preview data
const sample = await lazy.head(10);
sample.print();

// 3. Filter without loading everything
const highValue = await lazy.filter(row => row.amount > 10000);
console.log(`Found ${highValue.shape[0]} high-value transactions`);

// 4. Work with specific columns
const summary = lazy.select('date', 'amount', 'category');
const topRows = await summary.head(100);

// 5. Clear cache when done
lazy.clearCache();
```

## How It Works

1. **Initial scan**: When you call `scanCsv()`, the library scans the file to build an index of row byte positions. This is fast (reading only to find newlines).

2. **On-demand loading**: When you call `head()`, `tail()`, or other methods, only the requested rows are parsed.

3. **Chunk caching**: Parsed rows are stored in an LRU cache. Frequently accessed chunks stay in memory; rarely used chunks are evicted when the memory budget is exceeded.

4. **Streaming filter**: The `filter()` method processes the file in chunks, keeping memory usage bounded regardless of file size.

## Configuration

### Cache Memory

Control how much memory is used for caching parsed rows:

```typescript
const lazy = await scanCsv('./data.csv', {
  lazyConfig: {
    maxCacheMemory: 50 * 1024 * 1024, // 50MB
  }
});
```

### Chunk Size

Control how many rows are loaded per chunk:

```typescript
const lazy = await scanCsv('./data.csv', {
  lazyConfig: {
    chunkSize: 5000, // 5,000 rows per chunk
  }
});
```

Smaller chunks = less memory per load, but more disk reads for sequential access.
