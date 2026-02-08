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
bun add Molniya
```

## Quick Start

```typescript
import { readCsv, col, sum, avg, desc, DType } from "Molniya";

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
