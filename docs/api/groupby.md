# GroupBy API

API reference for grouping and aggregating data.

## groupBy()

Group DataFrame by one or more columns for aggregation.

```typescript
groupBy(column: string): GroupedDataFrame<T>
groupBy(columns: string[]): GroupedDataFrame<T>
groupBy(column: string, aggregations: AggregationSpec[]): DataFrame<T>
groupBy(columns: string[], aggregations: AggregationSpec[]): DataFrame<T>
```

**Parameters:**
- `column` / `columns` - Column(s) to group by
- `aggregations` - Array of aggregation specifications (optional, for immediate aggregation)

**Returns:** GroupedDataFrame for further operations, or DataFrame if aggregations provided

**Example:**

```typescript
import { col, sum, count } from "Molniya";

// Group by single column
df.groupBy("category")

// Group by multiple columns
df.groupBy(["year", "month"])

// Group with immediate aggregation
df.groupBy("category", [
  { name: "total", expr: sum("amount") },
  { name: "count", expr: count() }
])
```

## AggregationSpec

Specification for an aggregation within a groupBy:

```typescript
interface AggregationSpec {
  name: string;      // Name of the output column
  expr: Expr;        // Aggregation expression
}
```

**Example:**

```typescript
{ name: "total_sales", expr: sum("amount") }
{ name: "avg_price", expr: avg("price") }
{ name: "record_count", expr: count() }
```

## GroupedDataFrame Methods

When you call `groupBy()` without aggregations, you get a `GroupedDataFrame` with these methods:

### agg()

Apply aggregations to grouped data.

```typescript
agg(aggregations: AggregationSpec[]): DataFrame<T>
```

**Example:**

```typescript
import { col, sum, avg, count } from "Molniya";

df.groupBy("category").agg([
  { name: "total", expr: sum("amount") },
  { name: "average", expr: avg("price") },
  { name: "count", expr: count() }
])
```

### count()

Count rows per group (convenience method).

```typescript
count(): DataFrame<T>
```

**Example:**

```typescript
df.groupBy("category").count()
// Equivalent to: groupBy("category").agg([{ name: "count", expr: count() }])
```

### pivot()

Pivot groups into columns (cross-tabulation).

```typescript
pivot(pivotColumn: string, values?: unknown[]): GroupedDataFrame<T>
```

**Example:**

```typescript
// Pivot sales by year
// Input:  year | category | amount
// Output: category | 2023 | 2024
df.groupBy("category")
  .pivot("year")
  .agg([{ name: "total", expr: sum("amount") }])
```

## Aggregation Functions

### sum()

Sum of values.

```typescript
sum(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { sum, col } from "Molniya";

sum("amount")
sum(col("price").mul(col("quantity")))
```

### avg() / mean()

Average (arithmetic mean).

```typescript
avg(column: string | ColumnRef): AggregationExpr
mean(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { avg } from "Molniya";

avg("score")
avg(col("total"))
```

### min()

Minimum value.

```typescript
min(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { min } from "Molniya";

min("price")
min(col("date"))
```

### max()

Maximum value.

```typescript
max(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { max } from "Molniya";

max("price")
max(col("date"))
```

### count()

Count of rows or non-null values.

```typescript
count(): AggregationExpr                    // Count all rows
count(column: string | ColumnRef): AggregationExpr  // Count non-null values
```

**Example:**

```typescript
import { count } from "Molniya";

count()           // Total rows per group
count("email")    // Rows with non-null email per group
```

### first()

First value in group.

```typescript
first(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { first } from "Molniya";

first("name")
first(col("created_at"))
```

::: tip Ordering
Use `sort()` before `groupBy()` to control which value is "first".
:::

### last()

Last value in group.

```typescript
last(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { last } from "Molniya";

last("status")
last(col("updated_at"))
```

### std()

Standard deviation.

```typescript
std(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { std } from "Molniya";

std("value")
```

### var()

Variance.

```typescript
var(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { var as variance } from "Molniya";

variance("value")
```

### median()

Median value.

```typescript
median(column: string | ColumnRef): AggregationExpr
```

**Example:**

```typescript
import { median } from "Molniya";

median("income")
```

## Conditional Aggregations

Filter aggregations within groups:

```typescript
import { sum, count, col } from "Molniya";

df.groupBy("region", [
  { 
    name: "high_value_total", 
    expr: sum("amount").filter(col("amount").gt(1000)) 
  },
  { 
    name: "high_value_count", 
    expr: count().filter(col("amount").gt(1000)) 
  },
  { 
    name: "total_count", 
    expr: count() 
  }
])
```

## Multiple GroupBy Columns

Group by computed or multiple columns:

```typescript
import { col, year, month, sum } from "Molniya";

// Group by year and month
df.withColumn("year", year(col("date")))
  .withColumn("month", month(col("date")))
  .groupBy(["year", "month"], [
    { name: "revenue", expr: sum("amount") }
  ])

// Group by age bracket
df.withColumn("age_group",
  when(col("age").lt(25), "18-24")
    .when(col("age").lt(35), "25-34")
    .otherwise("35+")
)
.groupBy("age_group", [
  { name: "count", expr: count() },
  { name: "avg_income", expr: avg("income") }
])
```

## Common Patterns

### Summary Statistics

```typescript
import { sum, avg, min, max, count, std } from "Molniya";

df.groupBy("category", [
  { name: "total", expr: sum("amount") },
  { name: "average", expr: avg("amount") },
  { name: "min", expr: min("amount") },
  { name: "max", expr: max("amount") },
  { name: "count", expr: count() },
  { name: "std_dev", expr: std("amount") }
])
```

### Count Distinct

```typescript
import { countDistinct } from "Molniya";

df.groupBy("region", [
  { name: "unique_customers", expr: countDistinct("customer_id") },
  { name: "total_orders", expr: count() }
])
```

### First and Last

```typescript
import { first, last } from "Molniya";

df.sort(asc("date"))
  .groupBy("customer_id", [
    { name: "first_order", expr: first("order_date") },
    { name: "last_order", expr: last("order_date") },
    { name: "latest_status", expr: last("status") }
  ])
```

## Performance Notes

- GroupBy operations require materializing data
- Pre-filter data before grouping when possible
- Too many unique group keys can cause memory issues
- Aggregations are computed in a single pass when possible
