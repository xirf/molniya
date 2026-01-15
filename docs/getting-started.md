# Getting Started

## Installation

```bash
bun add mornye
```

## Quick Start

```typescript
import { DataFrame, readCsv, m } from 'mornye';

// Define a schema
const schema = {
  name: m.string(),
  age: m.int32(),
  score: m.float64(),
} as const;

// Create from data
const df = DataFrame.from(schema, [
  { name: 'Alice', age: 25, score: 95.5 },
  { name: 'Bob', age: 30, score: 87.2 },
  { name: 'Carol', age: 22, score: 91.8 },
]);

// Access columns
const ages = df.col('age');        // Series<'int32'>
console.log(ages.mean());          // 25.67

// Filter rows
const adults = df.where('age', '>=', 25);

// Sort
const ranked = df.sort('score', false);

// Group and aggregate
const grouped = df.groupby('name').agg({ score: 'mean' });

// Display
df.print();
```

## Load CSV Files

```typescript
import { readCsv } from 'mornye';

// Auto-infer types from data
const df = await readCsv('./data.csv');

// Or provide explicit schema
const df2 = await readCsv('./data.csv', {
  schema: {
    id: m.int32(),
    price: m.float64(),
    name: m.string(),
  }
});
```

## Large Files with LazyFrame

For files that don't fit in memory, use `scanCsv()` to create a `LazyFrame`:

```typescript
import { scanCsv } from 'mornye';

// Open file without loading all data
const lazy = await scanCsv('./large_file.csv');

// Only loads first 10 rows
const sample = await lazy.head(10);
sample.print();

// Filter without loading everything into memory
const filtered = await lazy.filter(row => row.price > 100);

// Convert to DataFrame when needed
const df = await lazy.collect(1000); // Load only 1000 rows
```

::: tip
Use `readCsv()` for files that fit in memory, and `scanCsv()` for larger files or when you only need a subset of the data.
:::

## Implementation Notes

Mornye uses several techniques for efficient data processing:

- **TypedArrays** (Float64Array, Int32Array) for numeric data
- **Buffer.indexOf** for line finding
- **Byte-level CSV parsing** (minimal string allocations)
- **Pre-allocated storage** based on row count estimation
- **LRU chunk caching** for LazyFrame

## Next Steps

- [Series API](/api/series) - Statistical operations and transformations
- [DataFrame API](/api/dataframe) - Filtering, grouping, sorting
- [LazyFrame API](/api/lazyframe) - Working with large files
