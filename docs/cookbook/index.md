# 🍳 Cookbook

Practical recipes for real-world data tasks. Each recipe explains **what** you're doing, **why** it works, and **how** to adapt it to your needs.

Unlike the API reference (which lists every method), the cookbook shows you how to combine operations to solve actual problems.

## What's Cooking?

<div class="cookbook-grid">

### [DataFrame Recipes](./dataframe.md)

Filter, sort, transform, and manipulate your data tables.

### [LazyFrame Recipes](./lazyframe.md)

High-performance queries for large CSV files.

### [Series Recipes](./series.md)

String operations, stats, and column transformations.

### [CSV I/O Recipes](./csv-io.md)

Loading and saving CSV files with all the options.

### [Data Cleaning](./cleaning.md)

Handle nulls, outliers, and messy data like a pro.

</div>

## Quick Start

If you're new, start with these:

**Load a CSV in 3 lines:**

```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("data.csv", {
  name: DType.String,
  age: DType.Int32,
});
```

**Filter and sort:**

```typescript
const top10 = df.filter("age", ">=", 18).sortBy("score", false).head(10);
```

**Get quick stats:**

```typescript
const values = df.get("revenue").toArray();
const total = values.reduce((a, b) => a + b, 0);
console.log(`Total revenue: $${total}`);
```

## Tips

- **Start small** - Copy an example and modify it
- **Check types** - Use the right `DType` for your data
- **LazyFrame for big files** - Optimized for efficient processing
- **Chain operations** - Most methods return a new DataFrame

## See Also

- [Guide](/guide/introduction) - Learn the concepts
- [API Reference](/api/overview) - Full documentation
- [Examples](https://github.com/xirf/molniya/tree/main/examples) - Real projects
