# From Polars to Molniya

Molniya is heavily inspired by Polars! If you know Polars, you'll feel right at home.

## Key Differences

| Aspect             | Polars                     | Molniya                     |
| ------------------ | -------------------------- | --------------------------- |
| **Type System**    | Explicit schemas           | Explicit schemas ✓          |
| **Lazy Execution** | `scan_csv()` → `collect()` | `scanCsv()` → `collect()` ✓ |
| **Error Handling** | Exceptions                 | Result type                 |
| **Language**       | Python/Rust                | TypeScript/JavaScript       |
| **Expressions**    | Rich expression API        | Simpler operator-based      |
| **Performance**    | Highly optimized           | Good (similar strategies)   |
| **API Style**      | Method chaining            | Method chaining ✓           |

**Bottom line:** Very similar concepts! Main differences are language and error handling.

## Common Operations

### Lazy CSV Loading

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

**What changed:**

- Schema required upfront in `scanCsv()`
- Simpler filter syntax (no `pl.col()`)
- Result type for errors
- Same lazy execution!

---

### Eager CSV Loading

**Polars:**

```python
df = pl.read_csv("data.csv")
```

**Molniya:**

```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("data.csv", {
  column1: DType.String,
  column2: DType.Int32,
});

const df = result.ok ? result.data : null;
```

**What changed:**

- Schema required
- Result type instead of exception

---

### Filtering

**Polars:**

```python
# Simple filter
df = df.filter(pl.col("age") > 25)

# Multiple conditions
df = df.filter(
    (pl.col("age") >= 18) & (pl.col("age") < 30)
)

# String operations
df = df.filter(pl.col("name").str.starts_with("A"))
```

**Molniya:**

```typescript
// Simple filter
const filtered = df.filter("age", ">", 25);

// Multiple conditions (chained)
const filtered = df.filter("age", ">=", 18).filter("age", "<", 30);

// String operations
const filtered = df.filter("name", "startsWith", "A");
```

**What changed:**

- No `pl.col()` needed - just use column name
- Chain filters for AND logic
- String operators built into filter

---

### Selecting Columns

**Polars:**

```python
# Select specific columns
df = df.select(["name", "age", "email"])

# Get single column
ages = df["age"]
```

**Molniya:**

```typescript
// Select specific columns
const subset = df.select(["name", "age", "email"]);

// Get single column
const ages = df.get("age");
```

**What changed:**

- Use `.get()` instead of bracket notation

---

### Sorting

**Polars:**

```python
df = df.sort("age", descending=True)
```

**Molniya:**

```typescript
const sorted = df.sortBy("age", false); // false = descending
```

**Nearly identical!**

---

### Group By

**Polars:**

```python
df = df.groupby("category").agg([
    pl.col("revenue").sum().alias("total_revenue"),
    pl.col("quantity").mean().alias("avg_quantity")
])
```

**Molniya (LazyFrame):**

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .groupby(
    ["category"],
    [
      { column: "revenue", agg: "sum", alias: "total_revenue" },
      { column: "quantity", agg: "mean", alias: "avg_quantity" },
    ],
  )
  .collect();
```

**What changed:**

- Array of objects instead of expressions
- Available aggregations: sum, mean, min, max, count
- More aggregations coming soon

---

### String Operations

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
    name: df.get("name").toArray(),
    name_lower: lower.toArray(),
  },
  {
    name: DType.String,
    name_lower: DType.String,
  },
);
```

**What changed:**

- Series has `.str` accessor (similar!)
- Manual column addition (more explicit)
- `.with_columns()` coming soon

---

### Type System

**Polars:**

```python
schema = {
    "id": pl.Int32,
    "name": pl.Utf8,
    "price": pl.Float64,
    "active": pl.Boolean,
}

df = pl.read_csv("data.csv", schema=schema)
```

**Molniya:**

```typescript
const schema = {
  id: DType.Int32,
  name: DType.String,
  price: DType.Float64,
  active: DType.Boolean,
};

const result = await readCsv("data.csv", schema);
```

**Nearly identical!** Type names map directly:

- `pl.Int32` → `DType.Int32`
- `pl.Int64` → `DType.Int64`
- `pl.Float64` → `DType.Float64`
- `pl.Utf8` → `DType.String`
- `pl.Boolean` → `DType.Boolean`
- `pl.Datetime` → `DType.Datetime`

---

### Null Handling

**Polars:**

```python
# Drop nulls
df = df.drop_nulls()

# Fill nulls
df = df.with_columns(
    pl.col("age").fill_null(0)
)
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
```

**What changed:**

- Prefer handling at load time
- Use filter to drop nulls
- Series has `.fillNull()` method

---

### Writing CSV

**Polars:**

```python
df.write_csv("output.csv")
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

- Standalone function instead of method
- Result type for errors

## Optimization Comparison

Both Polars and Molniya use similar optimization strategies:

| Optimization            | Polars                     | Molniya                    |
| ----------------------- | -------------------------- | -------------------------- |
| **Predicate Pushdown**  | ✓ Filters during scan      | ✓ Filters during scan      |
| **Projection Pushdown** | ✓ Load only needed columns | ✓ Load only needed columns |
| **Query Fusion**        | ✓ Combine operations       | ✓ Combine operations       |
| **Parallel Processing** | ✓ Multi-threaded           | Future                     |
| **Memory Mapping**      | ✓ For large files          | Future                     |

**Performance:** Molniya is fast (10-100x faster than naive approaches) but Polars is faster still thanks to Rust and parallelization.

## Expression API

**Polars strength:** Rich expression API

```python
df = df.with_columns([
    (pl.col("price") * pl.col("quantity")).alias("total"),
    pl.when(pl.col("age") >= 18)
      .then("Adult")
      .otherwise("Minor")
      .alias("category")
])
```

**Molniya approach:** More manual, but flexible

```typescript
const prices = df.get("price").toArray();
const quantities = df.get("quantity").toArray();
const ages = df.get("age").toArray();

const totals = prices.map((p, i) => p * quantities[i]);
const categories = ages.map((age) => (age >= 18 ? "Adult" : "Minor"));

const result = DataFrame.fromColumns(
  {
    price: prices,
    quantity: quantities,
    age: ages,
    total: totals,
    category: categories,
  },
  schema,
);
```

**Trade-off:** Polars is more concise, Molniya gives you full JavaScript power.

## Migration Checklist

- ☐ Add schemas to all `scanCsv()` calls (already have them? Great!)
- ☐ Replace `pl.col()` expressions with simple column names
- ☐ Add Result type checks to all operations
- ☐ Rewrite complex expressions as JavaScript map/filter
- ☐ Update `with_columns()` to manual DataFrame creation
- ☐ Same lazy execution patterns work!

## Common Patterns

### Lazy Pipeline

**Polars:**

```python
result = (
    pl.scan_csv("data.csv")
    .filter(pl.col("status") == "active")
    .select(["id", "name", "value"])
    .sort("value", descending=True)
    .collect()
)
```

**Molniya:**

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .filter("status", "==", "active")
  .select(["id", "name", "value"])
  .collect();

if (result.ok) {
  const sorted = result.data.sortBy("value", false);
}
```

**Note:** Sorting happens after collect in Molniya (can't optimize sorting during scan).

### Chained Transformations

**Polars:**

```python
df = (
    df
    .filter(pl.col("age") > 18)
    .with_columns(
        pl.col("name").str.to_lowercase().alias("name_lower")
    )
    .select(["name_lower", "age"])
)
```

**Molniya:**

```typescript
const filtered = df.filter("age", ">", 18);
const names = filtered.get("name").str.toLowerCase();

const result = DataFrame.fromColumns(
  {
    name_lower: names.toArray(),
    age: filtered.get("age").toArray(),
  },
  {
    name_lower: DType.String,
    age: DType.Int32,
  },
);
```

## Why Choose Molniya?

**Choose Polars if:**

- ✓ Raw performance is critical
- ✓ You're in Python ecosystem
- ✓ Need rich expression API
- ✓ Working with very large datasets (100GB+)

**Choose Molniya if:**

- ✓ You're in JavaScript/TypeScript ecosystem
- ✓ Want similar concepts in JS
- ✓ Need Node.js integration
- ✓ Prefer explicit over magic
- ✓ Working with medium datasets (< 10GB)

## Need Help?

- [LazyFrame Guide](/guide/lazy-evaluation) - Optimization details
- [Cookbook](/cookbook/lazyframe) - LazyFrame recipes
- [API Reference](/api/lazyframe) - All LazyFrame methods
