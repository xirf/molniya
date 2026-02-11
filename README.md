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
Molniya's Parquet reader reduces memory usage by 90% compared to naive implementations through incremental row group streaming. Successfully processes large files:
- **29M rows** (195MB Parquet) loaded in **5 seconds** at 5.9M rows/sec
- **10M rows** (1.9GB CSV) loaded in **13ms** at 754M rows/sec
- Memory-efficient streaming handles multi-GB datasets
:::

## Benchmarks

Tested on real Kaggle datasets (Apple M1):

**Students Dataset (CSV - 1.9GB, 10M rows)**
| Operation        | Throughput       | Time   |
| ---------------- | ---------------- | ------ |
| Load CSV         | ~754M rows/sec   | 13ms   |
| Filter           | ~365K rows/sec   | 27s    |
| Select (3 cols)  | ~283K rows/sec   | 35s    |
| Complex Filter   | ~295K rows/sec   | 34s    |

**Sales Dataset (Parquet - 195MB, 29M rows)**
| Operation        | Throughput       | Time   |
| ---------------- | ---------------- | ------ |
| Load Parquet     | ~5.9M rows/sec   | 5s     |
| Filter           | ~2.2M rows/sec   | 13s    |
| Select (3 cols)  | ~13.9M rows/sec  | 2s     |
| Limit (100K)     | ~612K rows/sec   | 163ms  |

Run benchmarks locally:
```bash
bun run scripts/benchmark-real-data.ts
```

## Architecture

- **Buffer**: Lower level column management (`Chunk`, `ColumnBuffer`, `Dictionary`).
- **Expr**: AST for expressions (`col('a').gt(5)`), compiler, and type inference.
- **Ops**: Stream operators (`Filter`, `Project`, `Join`, `GroupBy`, `Sort`).
- **DataFrame**: High-level API wrapping the pipeline.

## License

MIT
