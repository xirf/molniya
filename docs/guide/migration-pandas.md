# Migrating from Pandas

This guide helps you transition from Python's pandas library to Molniya. While both libraries share similar concepts, there are important differences in API design, execution model, and type system.

## Key Differences

| Aspect | Pandas | Molniya |
|--------|--------|---------|
| **Runtime** | Python | Bun (JavaScript/TypeScript) |
| **Execution** | Eager (immediate) | Lazy (deferred) |
| **Schema** | Inferred | Required |
| **Memory Model** | Row-oriented | Columnar (Arrow-like) |
| **Streaming** | Limited (chunks) | Native |
| **Type Safety** | Runtime | Compile-time (TypeScript) |

## Reading Data

### CSV Files

::: code-group

```python [Pandas]
import pandas as pd

# Schema is inferred
df = pd.read_csv("data.csv")

# With explicit types
df = pd.read_csv("data.csv", dtype={
    "id": "int32",
    "name": "string",
    "amount": "float64"
})
```

```typescript [Molniya]
import { readCsv, DType } from "molniya";

// Schema is required
const df = await readCsv("data.csv", {
  id: DType.int32,
  name: DType.string,
  amount: DType.float64
});
```

:::

### From Records/Arrays

::: code-group

```python [Pandas]
import pandas as pd

data = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25}
]
df = pd.DataFrame(data)
```

```typescript [Molniya]
import { fromRecords, DType } from "molniya";

const data = [
  { id: 1, name: "Alice", age: 30 },
  { id: 2, name: "Bob", age: 25 }
];

const df = fromRecords(data, {
  id: DType.int32,
  name: DType.string,
  age: DType.int32
});
```

:::

## Selecting Columns

::: code-group

```python [Pandas]
# Single column (returns Series)
df["name"]
df.name

# Multiple columns
df[["name", "age"]]

# Drop columns
df.drop(columns=["temp_col"])
```

```typescript [Molniya]
// Select columns
const result = df.select("name", "age");

// Drop columns
const result = df.drop("temp_col");
```

:::

::: warning No Single Column Access
Molniya doesn't have a Series type. Selecting a single column still returns a DataFrame with one column.
:::

## Filtering Rows

::: code-group

```python [Pandas]
# Boolean indexing
df[df["age"] > 25]

# Multiple conditions
df[(df["age"] > 25) & (df["dept"] == "Engineering")]

# Query method
df.query("age > 25 and dept == 'Engineering'")
```

```typescript [Molniya]
import { col, and } from "molniya";

// Single condition
const result = df.filter(col("age").gt(25));

// Multiple conditions
const result = df.filter(
  and(
    col("age").gt(25),
    col("dept").eq("Engineering")
  )
);

// Chained filters
const result = df
  .filter(col("age").gt(25))
  .filter(col("dept").eq("Engineering"));
```

:::

## Adding/Modifying Columns

::: code-group

```python [Pandas]
# Add single column
df["bonus"] = df["salary"] * 0.1

# Multiple columns
df = df.assign(
    bonus=df["salary"] * 0.1,
    full_name=df["first"] + " " + df["last"]
)
```

```typescript [Molniya]
import { col, add } from "molniya";

// Add single column
const result = df.withColumn("bonus", col("salary").mul(0.1));

// Multiple columns
const result = df.withColumns({
  bonus: col("salary").mul(0.1),
  total: add(col("salary"), col("bonus"))
});
```

:::

## Aggregation

::: code-group

```python [Pandas]
# Simple aggregation
df["amount"].sum()
df["amount"].mean()
df["amount"].count()

# GroupBy
df.groupby("dept").agg({
    "salary": ["sum", "mean"],
    "id": "count"
})
```

```typescript [Molniya]
import { sum, avg, count, col } from "molniya";

// GroupBy returns a RelationalGroupedDataset
const result = df.groupBy("dept", [
  { name: "total_salary", expr: sum("salary") },
  { name: "avg_salary", expr: avg("salary") },
  { name: "count", expr: count() }
]);

// Or use the count() shortcut
const result = df.groupBy("dept").count();
```

:::

## Sorting

::: code-group

```python [Pandas]
# Single column
df.sort_values("age")
df.sort_values("age", ascending=False)

# Multiple columns
df.sort_values(["dept", "salary"], ascending=[True, False])
```

```typescript [Molniya]
import { asc, desc } from "molniya";

// Single column
const result = df.sort(asc("age"));
const result = df.sort(desc("age"));

// Multiple columns
const result = df.sort([asc("dept"), desc("salary")]);
```

:::

## Joining

::: code-group

```python [Pandas]
# Inner join
pd.merge(df1, df2, on="id")

# Left join
pd.merge(df1, df2, on="id", how="left")

# Different keys
pd.merge(df1, df2, left_on="user_id", right_on="id")
```

```typescript [Molniya]
// Inner join
const result = await df1.innerJoin(df2, "id");

// Left join
const result = await df1.leftJoin(df2, "id");

// Different keys
const result = await df1.innerJoin(df2, "user_id", "id");
```

:::

## Handling Missing Values

::: code-group

```python [Pandas]
# Drop nulls
df.dropna()
df.dropna(subset=["name"])

# Fill nulls
df.fillna(0)
df["age"].fillna(df["age"].mean())
```

```typescript [Molniya]
import { dropNulls, fillNull, col, avg } from "molniya";

// Drop nulls
const result = df.dropNulls();
const result = df.dropNulls("name");

// Fill nulls (simple value)
const result = df.fillNull("age", 0);

// Note: Molniya doesn't support expression-based fill yet
```

:::

## Execution Model

The biggest difference is lazy vs eager execution:

::: code-group

```python [Pandas]
# Everything executes immediately
filtered = df[df["age"] > 25]  # Data processed now
grouped = filtered.groupby("dept").sum()  # Data processed now
result = grouped.head(10)  # Data processed now
```

```typescript [Molniya]
// Nothing executes until you call a terminal method
const filtered = df.filter(col("age").gt(25));  // Just builds plan
const grouped = filtered.groupBy("dept", [...]);  // Just builds plan
const result = grouped.limit(10);  // Just builds plan

// Execution happens here:
await result.show();  // Terminal method
data = await result.toArray();  // Terminal method
```

:::

### Terminal Methods in Molniya

These methods trigger execution:

- `await df.show()` - Print to console
- `await df.toArray()` - Convert to array of objects
- `await df.collect()` - Materialize to in-memory DataFrame
- `await df.count()` - Count rows
- `await df.innerJoin(other, key)` - Joins materialize

## Common Patterns

### Top N by Group

::: code-group

```python [Pandas]
# Get top 2 salaries per department
df.sort_values("salary", ascending=False)
  .groupby("dept")
  .head(2)
```

```typescript [Molniya]
// Molniya doesn't have group head
// Use window functions (when available) or collect and process
const result = await df
  .sort(desc("salary"))
  .toArray();
// Then process in JS
```

:::

### Pivot Table

::: code-group

```python [Pandas]
pd.pivot_table(
    df,
    values="sales",
    index="region",
    columns="quarter",
    aggfunc="sum"
)
```

```typescript [Molniya]
// Molniya doesn't have pivot yet
// Use groupBy with multiple keys
const result = df.groupBy(["region", "quarter"], [
  { name: "sales", expr: sum("sales") }
]);
```

:::

## Type System

Pandas uses numpy dtypes with some inference:

```python
# Pandas
import pandas as pd
from pandas import Int64, Float64, string

df = pd.DataFrame({
    "id": pd.array([1, 2, 3], dtype=Int64),
    "name": pd.array(["A", "B", "C"], dtype=string),
    "score": pd.array([1.5, 2.5, 3.5], dtype=Float64)
})
```

Molniya requires explicit schema with TypeScript types:

```typescript
// Molniya
import { DType } from "molniya";

const schema = {
  id: DType.int32,
  name: DType.string.nullable,  // Nullable
  score: DType.float64
};

// TypeScript infers: { id: number, name: string | null, score: number }
```

## Quick Reference Table

| Pandas | Molniya |
|--------|---------|
| `df.head(n)` | `df.limit(n)` |
| `df.tail(n)` | Not available (streaming) |
| `df.shape` | `{ rows: await df.count(), columns: df.columnNames.length }` |
| `df.columns` | `df.columnNames` |
| `df.dtypes` | `df.schema` |
| `df.info()` | `df.printSchema()` |
| `df.describe()` | Not available |
| `df.copy()` | Not needed (immutable) |
| `df.iterrows()` | `await df.toArray()` |
| `len(df)` | `await df.count()` |

## Best Practices

1. **Always filter before sorting/joining** - Molniya streams data, so filter early to reduce memory usage
2. **Specify schemas explicitly** - Enables type inference and better performance
3. **Chain operations** - Build the full pipeline before executing
4. **Use `limit()` for exploration** - Limit data before calling `toArray()` or `show()`
5. **Remember async/await** - Terminal methods return Promises
