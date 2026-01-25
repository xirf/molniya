# Getting Started

Welcome to Molniya! This guide will get you up and running in under 5 minutes.

## Installation

Add Molniya to your project:

```bash
bun add molniya
```

That's it. Zero dependencies, ready to use.

## Your First DataFrame

Let's create a DataFrame from some data. Copy and paste this into a file:

```typescript
import { DataFrame, DType } from "molniya";

// Define what your data looks like
const schema = {
  name: DType.String,
  age: DType.Int32,
  salary: DType.Float64,
};

// Create a DataFrame
const df = DataFrame.fromColumns(
  {
    name: ["Alice", "Bob", "Charlie"],
    age: [25, 30, 35],
    salary: [50000, 60000, 70000],
  },
  schema,
);

// See what you got
console.log(df.toString());
```

Run it with `bun run your-file.ts`. You should see a table printed to your console.

### What just happened?

1. **Schema** - You told Molniya what types your data should be
2. **fromColumns** - You created a DataFrame from column data
3. **toString** - You printed it in a readable format

The schema isn't optional noise - it's how Molniya ensures type safety and enables optimizations.

## Loading Real Data

Working with CSV files? Just point Molniya at the file:

```typescript
import { scanCsv, DType } from "molniya";

const schema = {
  name: DType.String,
  age: DType.Int32,
  city: DType.String,
};

const result = await scanCsv("users.csv", { schema });

if (result.ok) {
  const df = result.data;
  console.log(`Loaded ${df.columnOrder.length} columns`);
} else {
  console.error("Oops:", result.error.message);
}
```

No need to read the file yourself - Molniya handles it with Bun's optimized file I/O.

### Already have the data?

If you've got CSV data as a string:

```typescript
import { scanCsvFromString, DType } from "molniya";

const csvData = `name,age,city
Alice,25,NYC
Bob,30,LA`;

const schema = {
  name: DType.String,
  age: DType.Int32,
  city: DType.String,
};

const result = await scanCsvFromString(csvData, { schema });
```

## Common Operations

Once you have a DataFrame, here's what you can do with it:

### Filter rows

```typescript
// Everyone over 25
const adults = df.filter("age", ">", 25);

// Exact match
const alice = df.filter("name", "==", "Alice");
```

### Pick columns

```typescript
// Just names and salaries
const payroll = df.select(["name", "salary"]);
```

### Sort

```typescript
// Highest paid first
const bySalary = df.sortBy("salary", false); // false = descending

// Youngest first
const byAge = df.sortBy("age", true); // true = ascending
```

### Chain it all together

```typescript
const result = df
  .filter("age", ">", 25)
  .select(["name", "salary"])
  .sortBy("salary", false);
```

## Working with Large Files

For big datasets, use LazyFrame. It builds a query plan and optimizes before execution:

```typescript
import { LazyFrame, DType } from "molniya";

const schema = {
  product: DType.String,
  category: DType.String,
  revenue: DType.Float64,
};

const result = await LazyFrame.scanCsv("sales.csv", schema)
  .filter("category", "==", "Electronics") // Will push down to scan
  .filter("revenue", ">", 1000)
  .select(["product", "revenue"]) // Only loads needed columns
  .collect(); // Execute the optimized plan
```

LazyFrame analyzes your query and:

- **Predicate pushdown** - Filters rows during CSV parsing
- **Column pruning** - Only reads the columns you need
- **Query fusion** - Combines operations when possible

For a 1GB CSV file, this can mean reading only 100MB.

## Error Handling

Molniya doesn't throw exceptions for expected errors. It returns Result types:

```typescript
const result = await scanCsv("data.csv", { schema });

if (result.ok) {
  // Happy path - you have data
  const df = result.data;
  console.log(df.toString());
} else {
  // Something went wrong
  console.error("Failed:", result.error.message);
}
```

This forces you to handle errors explicitly. No surprises.

## What's Next?

You now know enough to be productive. Where you go depends on what you need:

**Basic usage** (start here if you're new):

- [Understanding Data Types](./data-types.md) - Available types and when to use them

### Key Features

-   **[Lazy Evaluation](./lazy-evaluation)**: Build query plans and execute them efficiently.
-   **Operations**: Rich set of data manipulation operations.
-   **Filtering**: Powerful filtering capabilities.
-   **Sorting**: Multi-column sorting.
-   **Memory Efficiency**: Optimized memory usage with dictionary encoding.
-   **String Operations**: Vectorized string operations.

Check out the [Examples](../cookbook/index) to see more.

**API Reference** (when you need exact details):

- [DataFrame API](../api/dataframe.md)
- [LazyFrame API](../api/lazyframe.md)
- [Series API](../api/series.md)

## Need Help?

Stuck? Have questions?

- [GitHub Issues](https://github.com/xirf/molniya/issues) - Bug reports and feature requests
- [Discussions](https://github.com/xirf/molniya/discussions) - Ask questions, share ideas
