# What is Molniya?

Molniya is a DataFrame library designed specifically for Bun. It makes working with structured data simple and fun.

## Design Philosophy

**Schema-First**
Define your data types upfront. This enables type checking, optimizations, and makes your code self-documenting.

**Progressive Disclosure**
Start simple with basic operations. Layer in complexity (LazyFrame, string ops, custom aggregations) only when you need it.

**Bun-Optimized**
Built for Bun's runtime from day one. Uses Bun.file() for I/O, SIMD for parsing, and native APIs throughout. No Node.js compatibility layer.

**Explicit Errors**
No surprise exceptions. Operations return Result types - you explicitly handle success and failure.

**Memory Conscious**
Columnar storage and lazy evaluation keep memory usage predictable. You can process datasets larger than RAM.

## Core Concepts

### DataFrame

A table of data with named, typed columns. Immutable - operations return new DataFrames.

```typescript
const df = DataFrame.fromColumns(
  {
    name: ["Alice", "Bob"],
    age: [25, 30],
  },
  {
    name: DType.String,
    age: DType.Int32,
  },
);
```

### LazyFrame

A query builder that creates an execution plan before doing any work. Optimizes operations like filters and column selection.

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .filter("status", "==", "active")
  .select(["id", "name"])
  .collect(); // Execute optimized plan
```

### Series

A single column of typed data. Supports searching, sorting, and string operations.

```typescript
const names = df.get("name"); // Returns Series<string>
const upper = names.str.toUpperCase();
```

### Schema

A definition of column types. Required for CSV loading, optional for in-memory DataFrames.

```typescript
const schema = {
  id: DType.Int32,
  name: DType.String,
  value: DType.Float64,
};
```

## Key Features

**File I/O Built-In**
No need to read files yourself. Molniya uses Bun.file() for optimized I/O:

```typescript
// Automatic file reading
const result = await scanCsv("data.csv", { schema });

// In-memory data
const result = await scanCsvFromString(csvData, { schema });
```

**Query Optimization**
LazyFrame analyzes your query before execution:

- **Predicate pushdown** - Filters during CSV parsing
- **Column pruning** - Only loads requested columns
- **Query fusion** - Combines operations when possible

**Result Types**
All potentially-failing operations return Result:

```typescript
type Result<T, E> = { ok: true; data: T } | { ok: false; error: E };

const result = await scanCsv("data.csv", { schema });
if (result.ok) {
  // Use result.data
} else {
  // Handle result.error
}
```

**Zero Dependencies**
Molniya has no runtime dependencies. What you install is all you get.

**Type Safety**
Full TypeScript support. Schemas enable type checking without runtime overhead.

## When to Use Molniya

**Good for:**

- Loading and cleaning CSV files
- Data pipelines in Bun projects
- Filtering and transforming tabular data
- Type-safe data manipulation
- Memory-efficient processing

**Not (yet) for:**

- Multi-gigabyte datasets (use DuckDB or Polars)
- Complex SQL joins
- Statistical modeling
- Real-time streaming

## Comparison

### vs Pandas (Python)

**Similarities:**

- DataFrame concept
- Filter, select, sort operations
- Column-oriented storage

**Differences:**

- Molniya is schema-first (Pandas infers types)
- Molniya is immutable (Pandas mutates)
- Molniya returns Result types (Pandas throws)
- Molniya requires Bun (Pandas is Python)

### vs Polars

**Similarities:**

- Lazy evaluation with query plans
- Columnar storage
- SIMD optimizations

**Differences:**

- Molniya is JavaScript (Polars is Rust)
- Molniya is simpler (fewer features)
- Polars handles larger data
- Polars has more aggregations

### vs Arquero

**Similarities:**

- JavaScript/TypeScript
- Data transformation DSL

**Differences:**

- Molniya is Bun-specific (Arquero is universal)
- Molniya is schema-first (Arquero infers)
- Molniya has LazyFrame (Arquero is eager)

## Architecture

**Storage**
Data is stored in columnar format using TypedArrays:

- Int32Array for Int32 columns
- Float64Array for Float64 columns
- Regular arrays for String/Boolean

**Memory**
Columns are stored separately. Operations like select() are cheap (just reference different columns).

**Optimization**
LazyFrame builds a query plan as an AST. Before execution, it:

1. Pushes filters into scan operations
2. Prunes unrequested columns
3. Fuses compatible operations
4. Estimates memory usage

**File I/O**
Uses Bun.file() for async, streaming reads. CSV parser uses SIMD when available.

## Roadmap

**Current (v0.x):**

- ✅ CSV loading with automatic file reading
- ✅ Basic operations (filter, select, sort)
- ✅ LazyFrame optimization
- ✅ String operations
- ✅ Result types for errors

**Coming Soon:**

- GroupBy aggregations
- Join operations
- Parquet support
- More string functions
- Window functions

**Future:**

- JSON I/O
- Arrow integration
- Streaming transformations

## Contributing

Molniya is open source and welcomes contributions:

- [GitHub](https://github.com/xirf/molniya)
- [Issues](https://github.com/xirf/molniya/issues)
- [Discussions](https://github.com/xirf/molniya/discussions)

## License

Molniya is open source software licensed under the [MIT License](https://github.com/xirf/molniya/blob/main/LICENSE).

## Next Steps

- [Getting Started](./getting-started.md) - Complete walkthrough
- [Cookbook](./cookbook.md) - Copy-paste examples
- [API Reference](../api/dataframe.md) - Detailed docs
```
