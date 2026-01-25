# From Pandas to Molniya

Pandas is the most popular Python data library. This guide shows you how Pandas operations translate to Molniya.

## Key Differences

| Aspect             | Pandas                    | Molniya                      |
| ------------------ | ------------------------- | ---------------------------- |
| **Type System**    | Inferred from data        | Explicit schemas required    |
| **Error Handling** | Exceptions (try/catch)    | Result type (if/else)        |
| **Mutability**     | Can modify in place       | Always immutable             |
| **Language**       | Python                    | TypeScript/JavaScript        |
| **Null Handling**  | Built-in (dropna, fillna) | Manual or at load time       |
| **Performance**    | Single-threaded           | Optimized with LazyFrame     |
| **Large Files**    | Memory limited            | Chunked processing available |

## Common Operations

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
  column3: DType.Float64,
});

if (result.ok) {
  const df = result.data;
}
```

**What changed:**

- Must specify schema explicitly
- Returns Result type (check `.ok`)
- Use `await` (async operation)

---

### Filtering Rows

**Pandas:**

```python
# Single condition
filtered = df[df["age"] > 25]

# Multiple conditions
young_adults = df[(df["age"] >= 18) & (df["age"] < 30)]

# String matching
starts_a = df[df["name"].str.startswith("A")]
```

**Molniya:**

```typescript
// Single condition
const filtered = df.filter("age", ">", 25);

// Multiple conditions (chained)
const youngAdults = df.filter("age", ">=", 18).filter("age", "<", 30);

// String matching
const startsA = df.filter("name", "startsWith", "A");
```

**What changed:**

- Method chaining instead of boolean indexing
- Operators as strings
- Each filter returns new DataFrame

---

### Selecting Columns

**Pandas:**

```python
# Single column
ages = df["age"]

# Multiple columns
subset = df[["name", "age", "email"]]
```

**Molniya:**

```typescript
// Single column (returns Series)
const ages = df.get("age");

// Multiple columns
const subset = df.select(["name", "age", "email"]);
```

**What changed:**

- Use `.get()` for single column
- Use `.select()` with array for multiple

---

### Sorting

**Pandas:**

```python
# Ascending
sorted_df = df.sort_values("age")

# Descending
sorted_df = df.sort_values("age", ascending=False)

# Multiple columns
sorted_df = df.sort_values(["dept", "age"], ascending=[True, False])
```

**Molniya:**

```typescript
// Ascending
const sortedDf = df.sortBy("age", true);

// Descending
const sortedDf = df.sortBy("age", false);

// Multiple columns (chain them)
const sortedDf = df.sortBy("dept", true).sortBy("age", false);
```

**What changed:**

- `sortBy()` instead of `sort_values()`
- Boolean parameter for direction
- Chain for multi-column sorts

---

### Head/Tail

**Pandas:**

```python
first_10 = df.head(10)
last_5 = df.tail(5)
```

**Molniya:**

```typescript
const first10 = df.head(10);
const last5 = df.tail(5);
```

**Same!** This one is identical.

---

### Group By & Aggregate

**Pandas:**

```python
# Group by and sum
grouped = df.groupby("category")["revenue"].sum()

# Multiple aggregations
grouped = df.groupby("category").agg({
    "revenue": "sum",
    "quantity": "mean"
})
```

**Molniya (manual):**

```typescript
const categories = df.get("category").toArray();
const revenues = df.get("revenue").toArray();

const grouped = new Map();
categories.forEach((cat, i) => {
  grouped.set(cat, (grouped.get(cat) || 0) + revenues[i]);
});

console.log(Object.fromEntries(grouped));
```

**Molniya (LazyFrame):**

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .groupby(
    ["category"],
    [
      { column: "revenue", agg: "sum", alias: "total_revenue" },
      { column: "quantity", agg: "mean", alias: "avg_qty" },
    ],
  )
  .collect();
```

**What changed:**

- Manual aggregation with Maps/arrays on DataFrame
- Built-in groupby on LazyFrame
- More explicit but flexible

---

### Null Handling

**Pandas:**

```python
# Drop rows with any null
df_clean = df.dropna()

# Drop rows with null in specific column
df_clean = df.dropna(subset=["age"])

# Fill nulls
df["age"] = df["age"].fillna(0)

# Check for nulls
has_nulls = df["age"].isnull().any()
```

**Molniya:**

```typescript
// Handle at load time
const result = await readCsv("data.csv", schema, {
  nullValues: ["NA", "NULL", ""],
});

// Filter out nulls
const cleaned = df.filter("age", "!=", null);

// Fill nulls
const ages = df.get("age").fillNull(0);

// Check for nulls
const hasNulls = df
  .get("age")
  .toArray()
  .some((v) => v === null);
```

**What changed:**

- Prefer handling nulls at load time
- Use `.filter()` to remove null rows
- Series has `.fillNull()` method

---

### Adding Columns

**Pandas:**

```python
# Calculated column
df["total"] = df["price"] * df["quantity"]

# Conditional column
df["category"] = df["age"].apply(
    lambda x: "Minor" if x < 18 else "Adult"
)
```

**Molniya:**

```typescript
// Calculated column
const prices = df.get("price").toArray();
const quantities = df.get("quantity").toArray();
const totals = prices.map((p, i) => p * quantities[i]);

const withTotal = DataFrame.fromColumns(
  {
    price: prices,
    quantity: quantities,
    total: totals,
  },
  {
    price: DType.Float64,
    quantity: DType.Int32,
    total: DType.Float64,
  },
);

// Conditional column
const ages = df.get("age").toArray();
const categories = ages.map((age) => (age < 18 ? "Minor" : "Adult"));
```

**What changed:**

- DataFrames are immutable - create new one
- Use JavaScript map/array methods
- Must specify schema for new columns

---

### String Operations

**Pandas:**

```python
# Lowercase
df["name"] = df["name"].str.lower()

# Contains
matches = df[df["email"].str.contains("@company.com")]

# Replace
df["text"] = df["text"].str.replace("old", "new")
```

**Molniya:**

```typescript
// Lowercase
const names = df.get("name").str.toLowerCase();

// Contains
const matches = df.filter("email", "contains", "@company.com");

// Replace
const text = df.get("text").str.replace("old", "new");
```

**What changed:**

- Series has `.str` accessor (similar!)
- Some operations are filters instead
- Must create new DataFrame with results

---

### Statistics

**Pandas:**

```python
mean = df["age"].mean()
total = df["revenue"].sum()
min_val = df["price"].min()
max_val = df["price"].max()

# Describe (all stats)
df["age"].describe()
```

**Molniya:**

```typescript
const ages = df.get("age");
const mean = ages.mean();
const total = df.get("revenue").sum();
const minVal = df.get("price").min();
const maxVal = df.get("price").max();

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

- Series has stats methods (same as Pandas!)
- No built-in `.describe()` yet
- Create stats objects manually

---

### Saving CSV

**Pandas:**

```python
df.to_csv("output.csv", index=False)
```

**Molniya:**

```typescript
import { writeCsv } from "molniya";

const result = await writeCsv(df, "output.csv");

if (!result.ok) {
  console.error("Save failed:", result.error.message);
}
```

**What changed:**

- Use `writeCsv()` function
- Returns Result type
- No row index to worry about

## Large Files

**Pandas approach:**

```python
# Pandas loads everything into memory
df = pd.read_csv("huge.csv")
filtered = df[df["status"] == "active"]
```

**Molniya approach (much faster):**

```typescript
// LazyFrame filters during CSV scan
const result = await LazyFrame.scanCsv("huge.csv", schema)
  .filter("status", "==", "active")
  .collect();

// 10-100x faster for large files!
```

**Why it matters:**

- Pandas: Loads full file, then filters (slow, memory-heavy)
- Molniya: Filters while reading (fast, memory-efficient)

## Migration Checklist

- ☐ Convert all DataFrames to have explicit schemas
- ☐ Replace `df[df["col"] > val]` with `.filter("col", ">", val)`
- ☐ Change `df["col"]` to `.get("col")`
- ☐ Replace try/catch with Result type checks
- ☐ Update `sort_values()` to `sortBy()`
- ☐ Rewrite groupby operations (use LazyFrame if possible)
- ☐ Handle nulls at load time with `nullValues` option
- ☐ Replace in-place operations with new DataFrame creation
- ☐ Use LazyFrame for files > 10MB

## Common Patterns

### Pipeline Processing

**Pandas:**

```python
result = (
    pd.read_csv("data.csv")
    .query("age > 25")
    .sort_values("salary", ascending=False)
    .head(10)
)
```

**Molniya:**

```typescript
const result = await readCsv("data.csv", schema);

if (result.ok) {
  const top10 = result.data
    .filter("age", ">", 25)
    .sortBy("salary", false)
    .head(10);
}
```

### Data Cleaning

**Pandas:**

```python
df = df.dropna()
df["name"] = df["name"].str.lower().str.strip()
df = df[df["age"] > 0]
```

**Molniya:**

```typescript
let df = result.data;

// Remove nulls
df = df.filter("name", "!=", null);
df = df.filter("age", "!=", null);

// Clean strings
const names = df.get("name").str.toLowerCase().trim();

// Filter invalid ages
df = df.filter("age", ">", 0);
```

## Need Help?

- [Cookbook](/cookbook/) - Common recipes
- [API Reference](/api/dataframe) - All DataFrame methods
- [LazyFrame Guide](/guide/lazy-evaluation) - Performance tips
