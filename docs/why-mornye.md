# Why Mornye?

Mornye is designed for developers who need type-safe data manipulation in the JavaScript/TypeScript ecosystem, with support for both small and large datasets.

## Design Goals

### 1. TypedArray Foundation

All numeric data is stored in native TypedArrays (`Float64Array`, `Int32Array`):

- **Contiguous memory layout** for efficient access
- **Zero-copy slicing** via array views
- **JIT-friendly** patterns for JavaScript engines

### 2. Type-Safe API

```typescript
const schema = {
  name: m.string(),
  age: m.int32(),
  score: m.float64(),
} as const;

// TypeScript knows the exact types
const df = DataFrame.from(schema, data);
df.col('age');  // Series<'int32'> - fully typed
```

### 3. Large File Support

`LazyFrame` enables working with files larger than available RAM:

```typescript
// Load data on-demand, not all at once
const lazy = await scanCsv('./very_large_file.csv');
const sample = await lazy.head(100);
```

### 4. Pandas-Like API

Familiar operations for easy transition from Python:

```typescript
df.filter(row => row.age > 25)
  .groupby('department')
  .agg({ salary: 'mean' });
```

## Feature Overview

| Feature                 | Mornye |
| ----------------------- | ------ |
| TypedArray backend      | ✅      |
| Static typing           | ✅      |
| LazyFrame (large files) | ✅      |
| Zero dependencies       | ✅      |
| Bun-native APIs         | ✅      |

## When to Use Mornye

✅ **Good fit:**
- You want type-safe DataFrames
- You prefer a Pandas-like API
- You need to work with large CSV files
- You're using Bun.js

⚠️ **Consider alternatives when:**
- You need GPU acceleration (TensorFlow.js)
- You need full Pandas API compatibility
- You're targeting browsers (consider Arquero)