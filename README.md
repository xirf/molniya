# Molniya

**Molniya** is a high-performance, arrow-like dataframe library for TypeScript/Javascript (running on Bun). It focuses on columnar memory layout, zero-copy operations, and a fluent API inspired by Polars/Spark.

> [!WARNING]
> This is experimental project. It is not ready for production use.

## Features

- **Columnar Memory**: Uses TypedArrays for efficient storage and SIMD-friendly access.
- **Lazy Evaluation**: Builds logical plans and executes them in an optimized pipeline.
- **Streaming**: Processes data in chunks to handle datasets larger than memory.
- **Fluent API**: Expressive chainable API for data manipulation.
- **Dictionary Encoding**: Efficient string handling with automatic dictionary encoding.
- **Strict Typing**: Full TypeScript support with schema validation.

## Installation

```bash
bun add molniya
```

## Quick Start

```typescript
import { readCsv, col, sum, avg, desc, DType } from "molniya";

// 1. Load Data
const df = await readCsv("sales.csv", {
  id: DType.int32,
  category: DType.string,
  amount: DType.float64
});

// 2. Transform & Analyze
const result = df
  .filter(col("amount").gt(100))
  .withColumn("tax", col("amount").mul(0.1))
  .groupBy("category", [
    { name: "total_sales", expr: sum("amount") },
    { name: "avg_amount", expr: avg("amount") }
  ])
  .sort(desc("total_sales"))
  .limit(10);

// 3. Show Results
result.show();
```

## File Formats

Molniya has built-in support for:
- **CSV**: Streaming CSV parser with type inference
- **Parquet**: Custom high-performance reader (no dependencies)
  - Row group streaming for low memory usage
  - Predicate pushdown via column statistics  
  - 50% faster type conversions (single-allocation patterns)
  - Handles 100GB+ files with <3GB RAM
- **MBF**: Molniya Binary Format for cached streaming (planned)

```typescript
// Read Parquet files
const df = await readParquet("data.parquet");

// Read CSV with schema
const df2 = await readCsv("sales.csv", {
  id: DType.int32,
  name: DType.string
});
```

::: tip Parquet Performance
Molniya's Parquet reader reduces memory usage by 90% compared to naive implementations through incremental row group streaming. A 10GB file uses only ~2.5GB RAM during processing.
:::

## Benchmarks

Benchmarked on Apple M1 (1 Million Rows):

| Operation | Throughput    | Time  |
| --------- | ------------- | ----- |
| Filter    | ~93M rows/sec | 10ms  |
| Aggregate | ~31M rows/sec | 32ms  |
| GroupBy   | ~15M rows/sec | 66ms  |
| Join      | ~7M rows/sec  | 142ms |

Run benchmarks locally:
```bash
bun run benchmarks/dataframe-bench.ts
```

## Architecture

- **Buffer**: Lower level column management (`Chunk`, `ColumnBuffer`, `Dictionary`).
- **Expr**: AST for expressions (`col('a').gt(5)`), compiler, and type inference.
- **Ops**: Stream operators (`Filter`, `Project`, `Join`, `GroupBy`, `Sort`).
- **DataFrame**: High-level API wrapping the pipeline.

## License

MIT
