# Migration Guide

Switching to Molniya from another data library? This guide shows you how common operations translate.

## From Pandas (Python)

### Loading CSV

**Pandas:**
```python
import pandas as pd

df = pd.read_csv("data.csv")
```

**Molniya:**
```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("data.csv", {
  column1: DType.String,
  column2: DType.Int32,
  // ... schema required
});

const df = result.ok ? result.data : null;
```

**Key difference:** Molniya requires explicit schemas. This catches type errors early and enables better performance.

### Filtering

**Pandas:**
```python
filtered = df[df["age"] > 25]
young_adults = df[(df["age"] >= 18) & (df["age"] < 30)]
```

**Molniya:**
```typescript
const filtered = df.filter("age", ">", 25);
const youngAdults = df
  .filter("age", ">=", 18)
  .filter("age", "<", 30);
```

**Key difference:** Molniya uses method chaining instead of boolean indexing. Each filter returns a new DataFrame.

### Selecting columns

**Pandas:**
```python
subset = df[["name", "age", "email"]]
```

**Molniya:**
```typescript
const subset = df.select(["name", "age", "email"]);
```

**Same concept!** Selection works identically.

### Sorting

**Pandas:**
```python
sorted_df = df.sort_values("age", ascending=False)
```

**Molniya:**
```typescript
const sortedDf = df.sortBy("age", false);
```

### Group by and aggregate

**Pandas:**
```python
grouped = df.groupby("category")["revenue"].sum()
```

**Molniya:**
```typescript
const categories = df.get("category").toArray();
const revenues = df.get("revenue").toArray();

const grouped = new Map();
categories.forEach((cat, i) => {
  grouped.set(cat, (grouped.get(cat) || 0) + revenues[i]);
});
```

**Note:** Molniya's groupby is more manual currently. LazyFrame has built-in groupby:

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .groupby(
    ["category"],
    [{ column: "revenue", agg: "sum", alias: "total" }]
  )
  .collect();
```

### Null handling

**Pandas:**
```python
df = df.dropna()  # Remove rows with any null
df["age"] = df["age"].fillna(0)  # Fill nulls with 0
```

**Molniya:**
```typescript
// Handle nulls during load
const result = await readCsv("data.csv", schema, {
  nullValues: ["NA", "NULL", ""],
});

// Remove rows with nulls in specific column
const cleaned = df.filter("age", "!=", null);

// Fill nulls
const ages = df.get("age").toArray();
const filled = ages.map(age => age === null ? 0 : age);
```

## From Polars (Python/Rust)

Molniya is heavily inspired by Polars! The concepts are very similar.

### Lazy evaluation

**Polars:**
```python
import polars as pl

df = (
    pl.scan_csv("data.csv")
    .filter(pl.col("age") > 25)
    .select(["name", "age"])
    .collect()
)
```

**Molniya:**
```typescript
import { LazyFrame, DType } from "molniya";

const result = await LazyFrame.scanCsv("data.csv", {
  name: DType.String,
  age: DType.Int32,
})
  .filter("age", ">", 25)
  .select(["name", "age"])
  .collect();

const df = result.ok ? result.data : null;
```

**Very similar!** Main differences:
- Molniya requires schema upfront
- Molniya uses Result type for errors
- Polars has more expressions (coming to Molniya)

### String operations

**Polars:**
```python
df = df.with_columns(
    pl.col("name").str.to_lowercase().alias("name_lower")
)
```

**Molniya:**
```typescript
const names = df.get("name");
const lower = names.str.toLowerCase();

const withLower = DataFrame.fromColumns(
  {
    ...df,
    name_lower: lower.toArray(),
  },
  {
    ...schema,
    name_lower: DType.String,
  }
);
```

### Type system

**Polars:**
```python
schema = {
    "id": pl.Int32,
    "name": pl.Utf8,
    "price": pl.Float64,
}
```

**Molniya:**
```typescript
const schema = {
  id: DType.Int32,
  name: DType.String,
  price: DType.Float64,
};
```

**Nearly identical!** DType maps directly to Polars types.

## From Arquero (JavaScript)

### Loading and filtering

**Arquero:**
```javascript
import { table, fromCSV } from "arquero";

const dt = await fromCSV("data.csv");
const filtered = dt.filter(d => d.age > 25);
```

**Molniya:**
```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("data.csv", {
  age: DType.Int32,
  // ... schema
});

if (result.ok) {
  const filtered = result.data.filter("age", ">", 25);
}
```

**Key differences:**
- Molniya requires schema (type safety)
- Molniya uses Result type (explicit errors)
- Arquero filters use functions, Molniya uses operators

### Selecting columns

**Arquero:**
```javascript
const subset = dt.select("name", "age", "email");
```

**Molniya:**
```typescript
const subset = df.select(["name", "age", "email"]);
```

**Nearly the same!** Just use an array instead of varargs.

### Aggregation

**Arquero:**
```javascript
const summary = dt.rollup({
  total: d => op.sum(d.revenue),
  avg: d => op.mean(d.price),
});
```

**Molniya:**
```typescript
const revenues = df.get("revenue").toArray();
const prices = df.get("price").toArray();

const summary = {
  total: revenues.reduce((a, b) => a + b, 0),
  avg: prices.reduce((a, b) => a + b, 0) / prices.length,
};
```

**More explicit in Molniya.** You work with arrays directly.

### Derivations

**Arquero:**
```javascript
const derived = dt.derive({
  total: d => d.price * d.quantity,
});
```

**Molniya:**
```typescript
const prices = df.get("price").toArray();
const quantities = df.get("quantity").toArray();
const totals = prices.map((p, i) => p * quantities[i]);

const derived = DataFrame.fromColumns(
  {
    ...df,
    total: totals,
  },
  {
    ...schema,
    total: DType.Float64,
  }
);
```

## From Danfo.js

### Basic operations

**Danfo.js:**
```javascript
import * as dfd from "danfojs-node";

const df = await dfd.readCSV("data.csv");
const filtered = df.query(df["age"].gt(25));
const sorted = df.sortValues("age", { ascending: false });
```

**Molniya:**
```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("data.csv", schema);
if (result.ok) {
  const df = result.data;
  const filtered = df.filter("age", ">", 25);
  const sorted = df.sortBy("age", false);
}
```

**Molniya is more explicit about types and errors.**

### Column operations

**Danfo.js:**
```javascript
df["total"] = df["price"].mul(df["quantity"]);
```

**Molniya:**
```typescript
const prices = df.get("price").toArray();
const quantities = df.get("quantity").toArray();
const totals = prices.map((p, i) => p * quantities[i]);

const withTotal = DataFrame.fromColumns(
  { ...df, total: totals },
  { ...schema, total: DType.Float64 }
);
```

**Note:** Molniya DataFrames are immutable. Operations return new DataFrames.

## Key Concepts in Molniya

### 1. Explicit Schemas

Unlike many JS libraries, Molniya requires schemas:

**Why?**
- Type safety: Catch errors early
- Performance: Enables optimizations
- Clarity: Documents your data structure

```typescript
const schema = {
  id: DType.Int32,
  name: DType.String,
  price: DType.Float64,
};
```

### 2. Result Type

Molniya doesn't throw exceptions. Operations return `Result<T, E>`:

```typescript
const result = await readCsv("data.csv", schema);

if (result.ok) {
  const df = result.data;
  // Use DataFrame
} else {
  console.error(result.error.message);
  // Handle error
}
```

**Why?** Makes errors explicit and forces you to handle them.

### 3. Immutability

DataFrames are never modified in place:

```typescript
const df = await readCsv("data.csv", schema);

// This creates a NEW DataFrame
const filtered = df.filter("age", ">", 25);

// Original is unchanged
console.log(df.count()); // Still all rows
console.log(filtered.count()); // Fewer rows
```

**Why?** Prevents bugs from unexpected mutations.

### 4. Lazy vs Eager

Two ways to load data:

**Eager (`readCsv`):** Load everything immediately
```typescript
const result = await readCsv("small.csv", schema);
```

**Lazy (`LazyFrame`):** Build query plan, execute optimized
```typescript
const result = await LazyFrame.scanCsv("huge.csv", schema)
  .filter("status", "==", "active")
  .collect();
```

**Rule:** File > 10MB? Use LazyFrame.

## Migration Checklist

**Coming from Pandas?**
- ☐ Add type schemas to all CSVs
- ☐ Replace boolean indexing with `.filter()`
- ☐ Handle Result types (no exceptions)
- ☐ Use `.toArray()` for manual aggregations
- ☐ Consider LazyFrame for large files

**Coming from Polars?**
- ☐ Add schemas to `scanCsv`
- ☐ Handle Result types
- ☐ Use simpler filter syntax (operators instead of expressions)
- ☐ Same lazy evaluation concepts!

**Coming from Arquero?**
- ☐ Add schemas everywhere
- ☐ Replace filter functions with operators
- ☐ Handle Result types
- ☐ Use arrays directly for custom operations

**Coming from Danfo.js?**
- ☐ Add schemas
- ☐ Handle Result types
- ☐ Accept immutability (operations return new DataFrames)
- ☐ Use method chaining

## Common Patterns

### Pattern: Pipeline processing

**Other libraries:**
```python
df = (
    pd.read_csv("data.csv")
    .query("age > 25")
    .sort_values("age")
    .head(10)
)
```

**Molniya:**
```typescript
const result = await readCsv("data.csv", schema);

if (result.ok) {
  const df = result.data
    .filter("age", ">", 25)
    .sortBy("age", false)
    .head(10);
}
```

### Pattern: Error handling

**Other libraries:** Try/catch
```python
try:
    df = pd.read_csv("data.csv")
except Exception as e:
    print(f"Error: {e}")
```

**Molniya:** Result type
```typescript
const result = await readCsv("data.csv", schema);

if (!result.ok) {
  console.error("Error:", result.error.message);
  return;
}

const df = result.data;
```

### Pattern: Large file processing

**Other libraries:**
```python
# Pandas (not optimized)
df = pd.read_csv("huge.csv")
filtered = df[df["status"] == "active"]
```

**Molniya:**
```typescript
// Optimized: filter during CSV scan
const result = await LazyFrame.scanCsv("huge.csv", schema)
  .filter("status", "==", "active")
  .collect();
```

## Need Help?

**Missing a feature?**
- Check the [Roadmap](https://github.com/xirf/molniya/blob/main/ROADMAP.md)
- Open an [issue](https://github.com/xirf/molniya/issues)

**Questions?**
- Read the [Guide](/guide/introduction)
- Check the [Cookbook](/cookbook/)
- Browse [API docs](/api/overview)

**Found a bug?**
- [Report it](https://github.com/xirf/molniya/issues/new)
- Include version, code sample, and expected behavior
