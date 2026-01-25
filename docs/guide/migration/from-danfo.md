# From Danfo.js to Molniya

Danfo.js brings Pandas-like operations to JavaScript. This guide shows you how to migrate to Molniya.

## Key Differences

| Aspect             | Danfo.js            | Molniya                   |
| ------------------ | ------------------- | ------------------------- |
| **Type System**    | Runtime inference   | Explicit schemas required |
| **Mutability**     | Can modify in place | Always immutable          |
| **Error Handling** | Exceptions          | Result type               |
| **Large Files**    | Memory limited      | LazyFrame optimization    |
| **Dependencies**   | TensorFlow.js       | Standalone                |
| **Bundle Size**    | ~10MB+              | ~100KB                    |
| **API Style**      | Pandas-like         | Method chaining           |

## Common Operations

### Loading CSV

**Danfo.js:**

```javascript
import * as dfd from "danfojs-node";

const df = await dfd.readCSV("data.csv");
```

**Molniya:**

```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("data.csv", {
  id: DType.Int32,
  name: DType.String,
  value: DType.Float64,
});

const df = result.ok ? result.data : null;
```

**What changed:**

- Must provide schema
- Returns Result type
- Lighter weight (no TensorFlow)

---

### Filtering

**Danfo.js:**

```javascript
// Query method
const filtered = df.query(df["age"].gt(25));

// Boolean indexing
const filtered = df.loc({ rows: df["age"].gt(25) });

// Multiple conditions
const filtered = df.query(df["age"].gt(25).and(df["status"].eq("active")));
```

**Molniya:**

```typescript
// Simple filter
const filtered = df.filter("age", ">", 25);

// Multiple conditions (chained)
const filtered = df.filter("age", ">", 25).filter("status", "==", "active");
```

**What changed:**

- Simpler syntax with operators
- No `.query()` or `.loc()` needed
- Chain for multiple conditions

---

### Selecting Columns

**Danfo.js:**

```javascript
// Single column
const ages = df["age"];

// Multiple columns
const subset = df.loc({ columns: ["name", "age", "email"] });
```

**Molniya:**

```typescript
// Single column
const ages = df.get("age");

// Multiple columns
const subset = df.select(["name", "age", "email"]);
```

**What changed:**

- Use `.get()` instead of brackets
- Use `.select()` for multiple columns

---

### Adding Columns

**Danfo.js:**

```javascript
// Add calculated column
df["total"] = df["price"].mul(df["quantity"]);

// Add conditional column
df["category"] = df["age"].apply((age) => (age >= 18 ? "Adult" : "Minor"));
```

**Molniya:**

```typescript
// DataFrames are immutable - create new one
const prices = df.get("price").toArray();
const quantities = df.get("quantity").toArray();
const totals = prices.map((p, i) => p * quantities[i]);

const ages = df.get("age").toArray();
const categories = ages.map((age) => (age >= 18 ? "Adult" : "Minor"));

const withCols = DataFrame.fromColumns(
  {
    price: prices,
    quantity: quantities,
    total: totals,
    category: categories,
  },
  {
    price: DType.Float64,
    quantity: DType.Int32,
    total: DType.Float64,
    category: DType.String,
  },
);
```

**What changed:**

- Immutable - create new DataFrame
- Use JavaScript map directly
- Must specify schema for new columns

---

### Sorting

**Danfo.js:**

```javascript
// Ascending
const sorted = df.sortValues("age", { ascending: true });

// Descending
const sorted = df.sortValues("age", { ascending: false });
```

**Molniya:**

```typescript
// Ascending
const sorted = df.sortBy("age", true);

// Descending
const sorted = df.sortBy("age", false);
```

**Similar!** Just simpler parameter.

---

### Group By

**Danfo.js:**

```javascript
const grouped = df
  .groupby(["category"])
  .agg({ revenue: "sum", quantity: "mean" });
```

**Molniya (manual):**

```typescript
const categories = df.get("category").toArray();
const revenues = df.get("revenue").toArray();

const grouped = new Map();
categories.forEach((cat, i) => {
  grouped.set(cat, (grouped.get(cat) || 0) + revenues[i]);
});
```

**Molniya (LazyFrame):**

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .groupby(
    ["category"],
    [
      { column: "revenue", agg: "sum", alias: "total" },
      { column: "quantity", agg: "mean", alias: "avg_qty" },
    ],
  )
  .collect();
```

**What changed:**

- Manual aggregation on DataFrame
- Built-in groupby on LazyFrame
- More explicit

---

### Statistics

**Danfo.js:**

```javascript
const mean = df["age"].mean();
const sum = df["revenue"].sum();
const describe = df["age"].describe();
```

**Molniya:**

```typescript
const ages = df.get("age");
const mean = ages.mean();
const sum = df.get("revenue").sum();

// Manual describe
const values = ages.toArray();
const stats = {
  count: values.length,
  mean: values.reduce((a, b) => a + b, 0) / values.length,
  min: Math.min(...values),
  max: Math.max(...values),
};
```

**What changed:**

- Series has stats methods (similar!)
- No built-in `.describe()` yet
- Create stats objects manually

---

### String Operations

**Danfo.js:**

```javascript
df["name"] = df["name"].str.toLowerCase();
df["name"] = df["name"].str.trim();

const contains = df.loc({
  rows: df["email"].str.includes("@company.com"),
});
```

**Molniya:**

```typescript
const names = df.get("name").str.toLowerCase().trim();

const contains = df.filter("email", "contains", "@company.com");
```

**What changed:**

- Series has `.str` accessor (similar!)
- Some operations are filters
- Must create new DataFrame with results

---

### Head/Tail

**Danfo.js:**

```javascript
const first = df.head(10);
const last = df.tail(5);
```

**Molniya:**

```typescript
const first = df.head(10);
const last = df.tail(5);
```

**Identical!**

---

### Null Handling

**Danfo.js:**

```javascript
// Drop nulls
const cleaned = df.dropna();

// Fill nulls
df["age"].fillna(0, { inplace: true });

// Check for nulls
const hasNulls = df["age"].isna().sum() > 0;
```

**Molniya:**

```typescript
// Handle at load
const result = await readCsv("data.csv", schema, {
  nullValues: ["NA", "NULL", ""],
});

// Filter nulls
const cleaned = df.filter("age", "!=", null);

// Fill nulls
const ages = df.get("age").fillNull(0);

// Check for nulls
const hasNulls = ages.toArray().some((v) => v === null);
```

**What changed:**

- Prefer handling at load time
- No `inplace` (always immutable)
- Use filter to remove null rows

---

### Saving CSV

**Danfo.js:**

```javascript
df.toCSV("output.csv");
```

**Molniya:**

```typescript
import { writeCsv } from "molniya";

const result = await writeCsv(df, "output.csv");

if (!result.ok) {
  console.error(result.error.message);
}
```

**What changed:**

- Standalone function
- Returns Result type
- Async operation

## Mutability

**Danfo.js (mutable):**

```javascript
// Modifies original DataFrame
df["new_col"] = df["price"].mul(2);
df.drop({ columns: ["old_col"], inplace: true });
df.sortValues("age", { inplace: true });
```

**Molniya (immutable):**

```typescript
// Always creates new DataFrame
const withCol = DataFrame.fromColumns(
  { ...df, new_col: /* ... */ },
  schema
);
const dropped = df.drop(["old_col"]);
const sorted = df.sortBy("age", true);

// Original df is unchanged!
```

**Why immutability?**

- Prevents bugs from unexpected changes
- Easier to reason about
- Enables optimizations

## Performance: Large Files

**Danfo.js:**

```javascript
// Loads entire file into memory
const df = await dfd.readCSV("huge.csv");
const filtered = df.query(df["status"].eq("active"));
```

**Molniya (LazyFrame):**

```typescript
// Filters during CSV scan - much faster!
const result = await LazyFrame.scanCsv("huge.csv", schema)
  .filter("status", "==", "active")
  .collect();

// 10-100x faster for selective queries
```

**Molniya advantage:** Predicate pushdown for large files.

## Bundle Size

**Danfo.js:**

- Includes TensorFlow.js
- Bundle: ~10-20MB
- Good for ML tasks

**Molniya:**

- Standalone data library
- Bundle: ~100KB
- Good for data wrangling

**Choose based on your needs:**

- Need tensor operations? Use Danfo.js
- Just data wrangling? Molniya is lighter

## Migration Checklist

- ☐ Add schemas to all CSV reads
- ☐ Replace `.query()` with `.filter()`
- ☐ Change `df["col"]` to `.get("col")`
- ☐ Remove all `inplace=true` (operations return new DataFrames)
- ☐ Add Result type checks to I/O operations
- ☐ Replace `.loc()` with `.select()` or `.filter()`
- ☐ Rewrite column additions as DataFrame creation
- ☐ Update `.sortValues()` to `.sortBy()`
- ☐ Use LazyFrame for files > 10MB

## Common Patterns

### Data Cleaning

**Danfo.js:**

```javascript
const df = await dfd.readCSV("data.csv");
df.dropna({ inplace: true });
df["name"] = df["name"].str.toLowerCase();
df.drop({ columns: ["temp"], inplace: true });
```

**Molniya:**

```typescript
const result = await readCsv("data.csv", schema, {
  nullValues: ["NA", "NULL", ""],
});

if (result.ok) {
  let df = result.data.filter("name", "!=", null).drop(["temp"]);

  const names = df.get("name").str.toLowerCase();
  // Create new DataFrame with cleaned names
}
```

### Pipeline Processing

**Danfo.js:**

```javascript
const result = df
  .query(df["age"].gt(25))
  .sortValues("salary", { ascending: false })
  .head(10);
```

**Molniya:**

```typescript
const result = df.filter("age", ">", 25).sortBy("salary", false).head(10);
```

### Aggregation

**Danfo.js:**

```javascript
const total = df["revenue"].sum();
const avg = df["price"].mean();
```

**Molniya:**

```typescript
const total = df.get("revenue").sum();
const avg = df.get("price").mean();
```

## Error Handling

**Danfo.js:**

```javascript
try {
  const df = await dfd.readCSV("data.csv");
  // Use df
} catch (error) {
  console.error(error);
}
```

**Molniya:**

```typescript
const result = await readCsv("data.csv", schema);

if (!result.ok) {
  console.error(result.error.message);
  return;
}

const df = result.data;
// Use df (guaranteed valid)
```

**Molniya makes errors explicit** - you can't forget to check.

## Need Help?

- [Cookbook](/cookbook/) - Common recipes
- [DataFrame API](/api/dataframe) - All methods
- [Migration from Pandas](/guide/migration/from-pandas) - Similar concepts
