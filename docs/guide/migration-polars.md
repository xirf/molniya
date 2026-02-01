# Migrating from Polars

This guide helps you transition from Polars (Rust-based DataFrame library) to Molniya. Both libraries share similar design philosophies, making migration relatively straightforward.

## Key Similarities

- **Lazy evaluation** - Both use deferred execution
- **Expression-based API** - Both use expressions for transformations
- **Columnar storage** - Both use Arrow-like memory layout
- **Streaming** - Both support chunked processing
- **Type safety** - Both emphasize explicit types

## Key Differences

| Aspect | Polars | Molniya |
|--------|--------|---------|
| **Language** | Python (Rust backend) | TypeScript/JavaScript (Bun) |
| **API Style** | `pl.col("name")` | `col("name")` |
| **Schema** | Optional inference | Required |
| **String Type** | `pl.Utf8` | `DType.string` |
| **DateTime** | `pl.Datetime` | `DType.timestamp` |
| **Null Handling** | `fill_null()` | `fillNull()` (camelCase) |

## Reading Data

### CSV Files

::: code-group

```python [Polars]
import polars as pl

# Schema inferred
df = pl.read_csv("data.csv")

# With schema
df = pl.read_csv(
    "data.csv",
    schema={
        "id": pl.Int32,
        "name": pl.Utf8,
        "amount": pl.Float64
    }
)

# Lazy CSV scan
df = pl.scan_csv("data.csv")
```

```typescript [Molniya]
import { readCsv, DType } from "Molniya";

// Schema required
const df = await readCsv("data.csv", {
  id: DType.int32,
  name: DType.string,
  amount: DType.float64
});

// Note: readCsv is always lazy/streaming in Molniya
```

:::

### Parquet Files

::: code-group

```python [Polars]
# Eager
df = pl.read_parquet("data.parquet")

# Lazy
df = pl.scan_parquet("data.parquet")
```

```typescript [Molniya]
// Always reads with streaming support
const df = await readParquet("data.parquet");
```

:::

## Column Expressions

The expression API is very similar between Polars and Molniya:

::: code-group

```python [Polars]
import polars as pl

pl.col("name")
pl.col("age").alias("years")
pl.col("price") * 1.1
pl.col("amount").sum()
```

```typescript [Molniya]
import { col, sum } from "Molniya";

col("name")
col("age").alias("years")
col("price").mul(1.1)
sum("amount")
```

:::

### Comparison Operators

| Polars | Molniya |
|--------|---------|
| `pl.col("a") == 5` | `col("a").eq(5)` |
| `pl.col("a") != 5` | `col("a").neq(5)` |
| `pl.col("a") > 5` | `col("a").gt(5)` |
| `pl.col("a") >= 5` | `col("a").gte(5)` |
| `pl.col("a") < 5` | `col("a").lt(5)` |
| `pl.col("a") <= 5` | `col("a").lte(5)` |

### Arithmetic

::: code-group

```python [Polars]
pl.col("a") + pl.col("b")
pl.col("a") - pl.col("b")
pl.col("a") * pl.col("b")
pl.col("a") / pl.col("b")
pl.col("a") % pl.col("b")
```

```typescript [Molniya]
import { add, sub, mul, div, mod, col } from "Molniya";

add(col("a"), col("b"))
sub(col("a"), col("b"))
mul(col("a"), col("b"))
div(col("a"), col("b"))
mod(col("a"), col("b"))

// Or using method chaining:
col("a").add(col("b"))
col("a").mul(2)
```

:::

## Filtering

::: code-group

```python [Polars]
df.filter(pl.col("age") > 25)
df.filter((pl.col("age") > 25) & (pl.col("dept") == "Engineering"))
df.filter(pl.col("name").is_null())
```

```typescript [Molniya]
import { col, and, or, not } from "Molniya";

df.filter(col("age").gt(25))
df.filter(and(col("age").gt(25), col("dept").eq("Engineering")))
df.filter(col("name").isNull())
```

:::

## Adding Columns

::: code-group

```python [Polars]
df.with_columns(
    (pl.col("salary") * 0.1).alias("bonus"),
    (pl.col("salary") + pl.col("bonus")).alias("total")
)

# Single column
df.with_columns(
    pl.col("name").str.to_uppercase().alias("name_upper")
)
```

```typescript [Molniya]
import { col } from "Molniya";

df.withColumns({
  bonus: col("salary").mul(0.1),
  total: col("salary").add(col("bonus"))
})

// Single column
df.withColumn("name_upper", col("name").toUpperCase())
```

:::

## Selecting Columns

::: code-group

```python [Polars]
df.select("name", "age")
df.select(pl.col("name"), pl.col("age"))
df.drop("temp_col")
```

```typescript [Molniya]
df.select("name", "age")
df.drop("temp_col")
```

:::

## Aggregation

::: code-group

```python [Polars]
df.group_by("dept").agg(
    pl.col("salary").sum().alias("total"),
    pl.col("salary").mean().alias("avg"),
    pl.col("id").count().alias("count")
)

# Multiple group keys
df.group_by(["dept", "role"]).agg(...)
```

```typescript [Molniya]
import { sum, avg, count, col } from "Molniya";

df.groupBy("dept", [
  { name: "total", expr: sum("salary") },
  { name: "avg", expr: avg("salary") },
  { name: "count", expr: count() }
])

// Multiple group keys
df.groupBy(["dept", "role"], [...])
```

:::

## Sorting

::: code-group

```python [Polars]
df.sort("age")
df.sort("age", descending=True)
df.sort(["dept", "salary"], descending=[False, True])
```

```typescript [Molniya]
import { asc, desc } from "Molniya";

df.sort(asc("age"))
df.sort(desc("age"))
df.sort([asc("dept"), desc("salary")])
```

:::

## Joins

::: code-group

```python [Polars]
# Inner join
df1.join(df2, on="id")

# Left join
df1.join(df2, on="id", how="left")

# Different keys
df1.join(df2, left_on="user_id", right_on="id")

# Semi/anti joins
df1.join(df2, on="id", how="semi")
df1.join(df2, on="id", how="anti")
```

```typescript [Molniya]
// Inner join
await df1.innerJoin(df2, "id")

// Left join
await df1.leftJoin(df2, "id")

// Different keys
await df1.innerJoin(df2, "user_id", "id")

// Semi/anti joins
await df1.semiJoin(df2, "id")
await df1.antiJoin(df2, "id")
```

:::

## Null Handling

::: code-group

```python [Polars]
df.drop_nulls()
df.drop_nulls(subset=["name"])
df.fill_null(0)
df.fill_null(strategy="forward")
df.with_columns(pl.col("age").fill_null(pl.col("age").mean()))
```

```typescript [Molniya]
df.dropNulls()
df.dropNulls("name")
df.fillNull("column", 0)
// Note: forward fill and expression-based fill not yet available
```

:::

## String Operations

::: code-group

```python [Polars]
pl.col("name").str.contains("Alice")
pl.col("name").str.starts_with("A")
pl.col("name").str.ends_with("e")
pl.col("name").str.to_uppercase()
pl.col("name").str.lengths()
```

```typescript [Molniya]
col("name").contains("Alice")
col("name").startsWith("A")
col("name").endsWith("e")
// Note: toUpperCase, lengths not yet implemented
```

:::

## Type Casting

::: code-group

```python [Polars]
pl.col("id").cast(pl.Int64)
pl.col("amount").cast(pl.Float32)
```

```typescript [Molniya]
import { DTypeKind } from "Molniya";

col("id").cast(DTypeKind.Int64)
col("amount").cast(DTypeKind.Float32)
```

:::

## Data Types Mapping

| Polars | Molniya |
|--------|---------|
| `pl.Int8` | `DType.int8` |
| `pl.Int16` | `DType.int16` |
| `pl.Int32` | `DType.int32` |
| `pl.Int64` | `DType.int64` |
| `pl.UInt8` | `DType.uint8` |
| `pl.UInt16` | `DType.uint16` |
| `pl.UInt32` | `DType.uint32` |
| `pl.UInt64` | `DType.uint64` |
| `pl.Float32` | `DType.float32` |
| `pl.Float64` | `DType.float64` |
| `pl.Boolean` | `DType.boolean` |
| `pl.Utf8` | `DType.string` |
| `pl.Date` | `DType.date` |
| `pl.Datetime` | `DType.timestamp` |

## Lazy vs Eager

Both libraries support lazy evaluation, but with differences:

::: code-group

```python [Polars]
# Explicit lazy API with scan_*/lazy()
lf = pl.scan_csv("data.csv")
result = lf.filter(pl.col("age") > 25).collect()

# Or eager API
lf = pl.read_csv("data.csv")
```

```typescript [Molniya]
// Molniya is always lazy for CSV
const df = await readCsv("data.csv", schema);

// Build plan (no execution)
const filtered = df.filter(col("age").gt(25));

// Execute
await filtered.show();
```

:::

## Collecting Results

::: code-group

```python [Polars]
# Collect to DataFrame
df.collect()

# Collect to list of dictionaries
df.to_dicts()

# Get single value
df.select(pl.col("value")).to_series()[0]
```

```typescript [Molniya]
// Collect to in-memory DataFrame
await df.collect()

// Collect to array of objects
await df.toArray()

// Get single value (not directly supported)
const rows = await df.limit(1).toArray();
const value = rows[0]?.columnName;
```

:::

## Window Functions

::: code-group

```python [Polars]
df.with_columns(
    pl.col("salary").rank().over("dept").alias("rank_in_dept")
)
```

```typescript [Molniya]
// Window functions not yet implemented in Molniya
// Use groupBy and join as workaround
```

:::

## Quick Reference

| Polars | Molniya |
|--------|---------|
| `df.head(n)` | `df.limit(n)` |
| `df.tail(n)` | Not available |
| `df.shape` | `{ rows: await df.count(), columns: df.columnNames.length }` |
| `df.columns` | `df.columnNames` |
| `df.schema` | `df.schema` |
| `df.dtypes` | `df.schema.columns.map(c => c.dtype)` |
| `df.glimpse()` | `df.printSchema()` |
| `df.describe()` | Not available |
| `df.n_unique()` | Not available |
| `df.is_duplicated()` | Not available |
| `df.unique()` | `df.distinct()` |
| `df.sort_by()` | `df.sort()` |
| `df.filter()` | `df.filter()` |
| `df.with_columns()` | `df.withColumns()` |
| `df.select()` | `df.select()` |
| `df.drop()` | `df.drop()` |
| `df.rename()` | `df.rename()` |
| `df.join()` | `df.innerJoin()`, `df.leftJoin()`, etc. |
| `df.group_by()` | `df.groupBy()` |
| `df.pivot()` | Not available |
| `df.melt()` | Not available |
| `df.explode()` | `df.explode()` (limited) |
| `df.slice()` | Not available |
| `df.limit()` | `df.limit()` |
| `df.collect()` | `await df.collect()` |
| `df.fetch(n)` | `await df.limit(n).collect()` |

## Best Practices

1. **Leverage your Polars knowledge** - The APIs are very similar
2. **Remember async/await** - All terminal operations are async in Molniya
3. **Use camelCase** - Molniya uses JavaScript naming conventions
4. **Check for missing features** - Some advanced features from Polars may not be available yet
5. **Schema is always required** - Unlike Polars, Molniya requires explicit schema for CSV
