# Getting Started

Welcome to Molniya! This guide will get you up and running with ergonomic data analysis in TypeScript.

## What You'll Learn

- How to install Molniya in your project
- Creating your first DataFrame
- Running a simple analysis pipeline
- Understanding the basic output

## Installation

Molniya is a pure TypeScript library with zero runtime dependencies. It works in Bun, other runtime support is planned.

::: code-group

```bash [bun]
bun add molniya
```

```bash [npm]
npm install molniya
```

```bash [pnpm]
pnpm add molniya
```

```bash [yarn]
yarn add molniya
```

:::

> [!TIP]
> **TypeScript Users**: Molniya is written in TypeScript and bundles its own types. You don't need to install any `@types` packages.

---

## Your First Analysis

Let's jump right in. Instead of a "Hello World", we'll do something useful: analyze value-scores for potential products.

Create a file named `analysis.ts`:

```typescript
import { DataFrame } from "molniya";

// 1. Create a DataFrame (columns are fully typed!)
const df = DataFrame.fromColumns({
  product: ["Laptop", "Mouse", "Monitor", "Keyboard"],
  category: ["Electronics", "Accessories", "Electronics", "Accessories"],
  price: [999.99, 29.99, 199.99, 59.99],
  rating: [4.5, 4.2, 4.8, 3.9],
});

// 2. Perform your analysis
// Let's find high-rated items (rating >= 4.0) and calculate a "value score"
const result = df
  .filter((row) => row.rating >= 4.0)
  .assign("value_score", (row) => row.rating / Math.log10(row.price))
  .sort("value_score", false) // Descending sort
  .select("product", "price", "value_score");

// 3. See the results
result.print();
```

### Run it

```bash
bun run analysis.ts
# or
npx tsx analysis.ts
```

### The Output

You should see a nicely formatted table in your terminal:

```text
┌─────────┬──────────┬─────────────┐
│ product │  price   │ value_score │
├─────────┼──────────┼─────────────┤
│   Mouse │  29.9900 │      2.8436 │
│ Monitor │ 199.9900 │      2.0860 │
│  Laptop │ 999.9900 │      1.5000 │
└─────────┴──────────┴─────────────┘
```

> [!NOTE]
> Did you notice? The `Mouse` actually has the best value score despite being cheap! This is the power of quick data exploration.

---

## What Just Happened?

Let's break down that valid one-liner:

1.  **`DataFrame.fromColumns`**: We created a columnar data structure. Molniya inferred the types automatically (`string`, `float64`, etc.).
2.  **`filter`**: We narrowed down the dataset to only include items with a rating of 4.0 or higher.
3.  **`assign`**: We created a **new column** on the fly. The arrow function receives a fully typed `row` object.
4.  **`sort`**: We ordered the results by our new metric.
5.  **`select`**: We picked only the columns we cared about for the final report.

Crucially, **the original `df` remained unchanged**. Molniya uses an immutable approach, so every operation returns a new DataFrame.

## Next Steps

Now that you've got the basics, where should you go next?

- **[Core Concepts](/guide/concepts)**: Understand how Molniya thinks about data (DataFrames vs Series).
- **[Loading Data](/guide/loading-data)**: Learn how to ingest CSVs, JSON, and more.
- **[Filtering & Sorting](/guide/filtering)**: Dive deeper into data manipulation.

