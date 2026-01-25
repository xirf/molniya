# Introduction

> Molniya is a data manipulation library for Bun that makes working with structured data simple and fun.

## What is Molniya?

Molniya is a DataFrame library built specifically for Bun. It helps you load, transform, and analyze data without the complexity of heavy frameworks.

Think Pandas for Python, but designed for TypeScript and Bun from the ground up.

## Why Molniya?

**Schema-first approach**
Define your data types once, get type safety and optimizations everywhere. No guessing types at runtime.

**Built for Bun**
Uses Bun's file I/O and SIMD capabilities. No polyfills, no compatibility layers. Just modern JavaScript.

**Zero dependencies**
No npm install anxiety. The entire library has zero runtime dependencies.

**Explicit errors**
No surprise exceptions. All operations return Result types - you handle success and failure explicitly.

**Lazy evaluation**
Build query plans that optimize automatically. Filter a billion rows by only reading what matches.

## Quick Example

```typescript
import { scanCsv, DType } from "molniya";

const schema = {
  name: DType.String,
  age: DType.Int32,
};

const result = await scanCsv("users.csv", { schema });

if (result.ok) {
  const adults = result.data.filter("age", ">=", 18);
  console.log(adults.toString());
}
```

That's it. Load a CSV, filter it, print it. No ceremony.

## Core Concepts

### DataFrame

A table of data with named columns. Each column has a specific type. You can filter, sort, and transform it.

### LazyFrame

A query builder that creates an execution plan before doing any work. Optimizes operations like filters and column selection.

### Series

A single column of typed data. Supports operations like searching, sorting, and transformations.

### Schema

A definition of what types your columns should be. Molniya uses this for type checking and optimization.

## When to use Molniya

**Good for:**

- Loading and cleaning CSV files
- Filtering and transforming tabular data
- Data pipelines in Bun projects
- Memory-efficient data processing
- Type-safe data manipulation

**Not (yet) for:**

- Gigabytes of data (use DuckDB or Polars)
- Complex SQL-like joins
- Statistical modeling
- Real-time streaming data

## Design Philosophy

**Progressive disclosure**
Start simple, layer complexity as needed. Basic operations should be one-liners.

**Type safety without overhead**
Schemas enable type checking without runtime penalty. The types serve you, not the other way around.

**Bun-first**
No compatibility compromises. Uses Bun's APIs directly for maximum performance.

**Explicit over implicit**
No magic. No global state. Result types make errors visible.

**Memory conscious**
Columnar storage and lazy evaluation keep memory usage predictable and low.

## What's Next?

Ready to start? Head to [Getting Started](./getting-started.md) for a complete walkthrough.

Want to see real examples? Check the [Cookbook](./cookbook.md) for copy-pasteable recipes.

Need details? Browse the [API Reference](../api/dataframe.md).
