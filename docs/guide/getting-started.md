<script setup lang="ts">
import Card from '../.vitepress/theme/components/Card.vue'
</script>

# Getting Started

Molniya is a dataframe library designed specifically for the Bun runtime. It provides a familiar, Polars-inspired API for working with structured data.

## What is Molniya?

Molniya is built around a few core principles

<div class="grid grid-cols-2 gap-4">
    <Card title="Columnar Storage" description="Data is stored in column-oriented format using TypedArrays, which provides better cache locality for analytical workloads." />
    <Card title="Streaming Processing" description="Files are processed in chunks, allowing you to work with datasets larger than available memory." />
    <Card title="Lazy Evaluation" description="Operations build a logical plan that is optimized before execution, similar to query planners in databases." />
    <Card title="Type Safety" description="Full TypeScript support with compile-time schema validation." />
</div>

## Installation

```bash
bun add Molniya
```

::: warning Bun Only
Curently Molniya uses Bun-specific APIs like `Bun.file()` for streaming file I/O. It will not work in Node.js or browsers.
:::

## Your First DataFrame

Here's a complete example that demonstrates the basic workflow:

```typescript
import { readCsv, col, sum, avg, desc, DType } from "Molniya";

// 1. Define your schema
const schema = {
  id: DType.int32,
  name: DType.string,
  age: DType.int32,
  salary: DType.float64
};

// 2. Read data (returns immediately - lazy loading)
const df = await readCsv("employees.csv", schema);

// 3. Build your transformation pipeline
const result = df
  .filter(col("age").gte(25))                    // Filter rows
  .withColumn("bonus", col("salary").mul(0.1))   // Add computed column
  .groupBy("department", [                        // Group and aggregate
    { name: "total_salary", expr: sum("salary") },
    { name: "avg_age", expr: avg("age") },
    { name: "count", expr: count() }
  ])
  .sort(desc("total_salary"))                     // Sort descending
  .limit(10);                                     // Take top 10

// 4. Execute and display
await result.show();
```

## Core Concepts

### DataFrames are Lazy

When you call methods like `filter()` or `groupBy()`, you're not immediately processing data. Instead, you're building a logical plan:

```typescript
const df = await readCsv("bigfile.csv", schema);

// These don't process any data yet:
const filtered = df.filter(col("amount").gt(100));
const grouped = filtered.groupBy("category", [...]);

// Execution happens here:
await grouped.show();  // or .collect(), .toArray(), etc.
```

This allows Molniya to optimize the execution plan and stream data efficiently.

### Schemas are Required

Unlike some dataframe libraries, Molniya requires you to specify the schema upfront when reading CSV:

```typescript
const schema = {
  id: DType.int32,
  name: DType.string.nullable,  // Nullable string
  amount: DType.float64
};
```

This will give you:
- Type inference in TypeScript
- A better binary storage
- Early error detection

### Expressions are Composable

The `col()` function creates column references that support method chaining:

```typescript
import { col, lit, and } from "Molniya";

// Comparison operators
col("age").gt(18)           // >
col("age").gte(18)          // >=
col("age").lt(65)           // <
col("age").lte(65)          // <=
col("age").eq(25)           // ===
col("age").neq(25)          // !==

// Arithmetic
col("price").mul(1.1)       // *
col("price").add(col("tax")) // +
col("total").div(col("count")) // /

// Null handling
col("email").isNull()
col("email").isNotNull()

// String operations
col("name").startsWith("A")
col("name").contains("john")

// Combining conditions
and(
  col("age").gte(18),
  col("age").lt(65),
  col("active").eq(true)
)
```

## Next Steps

- Learn about [Data Types](./data-types) and when to use each one
- Explore the [Cookbook](../cookbook/) for common patterns
- Browse the [API Reference](../api/) for detailed documentation
- Check out [Examples](../examples/) for real-world use cases
