# Aggregation Functions API

API reference for aggregation functions used in groupBy and agg operations.

## Overview

Aggregation functions collapse multiple rows into a single value. They are typically used within `groupBy()` or `agg()` calls.

## Basic Aggregations

### sum()

Calculate the sum of a numeric column.

```typescript
sum(column: string | ColumnRef): AggregationExpr
```

**Parameters:**
- `column` - Column name or reference to sum

**Returns:** Aggregation expression

**Example:**

```typescript
import { sum, col } from "molniya";

// Sum of column
sum("amount")

// Sum of expression
sum(col("price").mul(col("quantity")))

// In groupBy
df.groupBy("category", [
  { name: "total", expr: sum("amount") }
])
```

**Null Handling:** Null values are ignored in the sum.

### avg()

Calculate the average (arithmetic mean) of a numeric column.

```typescript
avg(column: string | ColumnRef): AggregationExpr
```

**Alias:** `mean()`

**Example:**

```typescript
import { avg } from "molniya";

avg("score")
avg(col("total"))
```

**Null Handling:** Null values are ignored in the average.

### min()

Find the minimum value in a column.

```typescript
min(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { min } from "molniya";

min("price")
min("date")  // Works with dates too
```

**Null Handling:** Null values are ignored.

### max()

Find the maximum value in a column.

```typescript
max(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { max } from "molniya";

max("price")
max("date")
```

**Null Handling:** Null values are ignored.

### count()

Count rows or non-null values.

```typescript
count(): AggregationExpr                    // Count all rows
count(column: string | ColumnRef): AggregationExpr  // Count non-null values
```

**Example:**

```typescript
import { count } from "molniya";

// Count all rows
count()

// Count non-null values in column
count("email")

// In groupBy
df.groupBy("category", [
  { name: "total_rows", expr: count() },
  { name: "with_email", expr: count("email") }
])
```

**Null Handling:** 
- `count()` counts all rows including nulls
- `count(column)` counts only non-null values

## Statistical Aggregations

### std()

Calculate the standard deviation.

```typescript
std(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { std } from "molniya";

std("value")
```

**Null Handling:** Null values are ignored.

### var()

Calculate the variance.

```typescript
var(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { var as variance } from "molniya";

variance("value")
```

**Null Handling:** Null values are ignored.

### median()

Calculate the median value.

```typescript
median(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { median } from "molniya";

median("income")
```

**Null Handling:** Null values are ignored.

## Position Aggregations

### first()

Get the first value in the group.

```typescript
first(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { first } from "molniya";

first("name")
first(col("created_at"))
```

::: tip Ordering
The "first" value depends on the current row ordering. Use `sort()` before `groupBy()` for predictable results.
:::

### last()

Get the last value in the group.

```typescript
last(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { last } from "molniya";

last("status")
last(col("updated_at"))
```

## Distinct Aggregations

### countDistinct()

Count distinct/unique values.

```typescript
countDistinct(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { countDistinct } from "molniya";

df.groupBy("region", [
  { name: "unique_customers", expr: countDistinct("customer_id") },
  { name: "total_orders", expr: count() }
])
```

## Aggregation with DataFrame.agg()

Apply aggregations to the entire DataFrame:

```typescript
import { sum, avg, min, max, count } from "molniya";

// Single aggregation
const total = await df.agg(sum("amount"));

// Multiple aggregations
const stats = await df.agg([
  sum("amount"),
  avg("amount"),
  min("amount"),
  max("amount"),
  count()
]);
```

## Conditional Aggregations

Filter aggregations to specific rows:

```typescript
import { sum, count, col } from "molniya";

df.groupBy("category", [
  { 
    name: "high_value_total", 
    expr: sum("amount").filter(col("amount").gt(1000)) 
  },
  { 
    name: "high_value_count", 
    expr: count().filter(col("amount").gt(1000)) 
  }
])
```

## Aggregating Expressions

Use expressions within aggregations:

```typescript
import { sum, avg, col } from "molniya";

df.groupBy("category", [
  { name: "total_revenue", expr: sum(col("price").mul(col("quantity"))) },
  { name: "avg_discount_pct", expr: avg(col("discount").mul(100)) }
])
```

## Combining Aggregations

Multiple aggregations in a single groupBy:

```typescript
import { sum, avg, min, max, count, first, last } from "molniya";

df.groupBy("department", [
  { name: "total_salary", expr: sum("salary") },
  { name: "avg_salary", expr: avg("salary") },
  { name: "min_salary", expr: min("salary") },
  { name: "max_salary", expr: max("salary") },
  { name: "employee_count", expr: count() },
  { name: "first_hire", expr: first("hire_date") },
  { name: "latest_hire", expr: last("hire_date") }
])
```

## Performance Notes

- Aggregations are computed in a single pass when possible
- Null handling adds minimal overhead
- Statistical functions (std, var, median) require more computation
- Count distinct uses a hash set and may use significant memory for high cardinality

## Type Safety

Aggregation functions preserve type information:

```typescript
// sum of int32 returns int64 (to prevent overflow)
// avg of numeric returns float64
// min/max preserve input type
// count always returns int64
```
