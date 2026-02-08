# Aggregations

Aggregations summarize data by collapsing multiple rows into single values. Molniya provides a rich set of aggregation functions for statistical analysis and data summarization.

## Basic Aggregation

### Simple Aggregations

Apply a single aggregation to a DataFrame:

```typescript
import { col, sum, avg, count } from "molniya";

// Sum of a column
const total = await df.agg(sum(col("amount")));

// Average value
const average = await df.agg(avg(col("score")));

// Count rows
const rowCount = await df.agg(count());
```

### Multiple Aggregations

Compute multiple aggregations at once:

```typescript
import { col, sum, avg, min, max, count } from "molniya";

const stats = await df.agg([
  sum(col("amount")),
  avg(col("amount")),
  min(col("amount")),
  max(col("amount")),
  count()
]);
```

## GroupBy Aggregations

Group data by one or more columns before aggregating:

### Single Column GroupBy

```typescript
import { col, sum, count } from "molniya";

// Group by category and sum amounts
const result = df.groupBy("category", [
  { name: "total_sales", expr: sum("amount") },
  { name: "transaction_count", expr: count() }
]);

await result.show();
// Output: category, total_sales, transaction_count
```

### Multiple Column GroupBy

```typescript
// Group by year and month
const monthly = df
  .withColumn("year", col("date").year())
  .withColumn("month", col("date").month())
  .groupBy(["year", "month"], [
    { name: "revenue", expr: sum("amount") },
    { name: "orders", expr: count() },
    { name: "avg_order", expr: avg("amount") }
  ]);
```

## Aggregation Functions

### Statistical Functions

| Function | Description | Example |
|----------|-------------|---------|
| `sum()` | Sum of all values | `sum("amount")` |
| `avg()` / `mean()` | Average (arithmetic mean) | `avg("score")` |
| `min()` | Minimum value | `min("price")` |
| `max()` | Maximum value | `max("price")` |
| `count()` | Count of rows | `count()` |
| `count(col)` | Count of non-null values | `count("name")` |
| `std()` | Standard deviation | `std("value")` |
| `var()` | Variance | `var("value")` |
| `median()` | Median value | `median("income")` |

```typescript
import { sum, avg, min, max, count, std, median } from "molniya";

const stats = df.groupBy("department", [
  { name: "total_salary", expr: sum("salary") },
  { name: "avg_salary", expr: avg("salary") },
  { name: "min_salary", expr: min("salary") },
  { name: "max_salary", expr: max("salary") },
  { name: "employee_count", expr: count() },
  { name: "salary_std", expr: std("salary") },
  { name: "salary_median", expr: median("salary") }
]);
```

### First/Last Values

```typescript
import { first, last } from "molniya";

// Get first and last values in each group
const result = df.groupBy("category", [
  { name: "first_order", expr: first("order_date") },
  { name: "last_order", expr: last("order_date") },
  { name: "latest_status", expr: last("status") }
]);
```

::: tip Ordering Matters
`first()` and `last()` respect the current row ordering. Use `sort()` before groupBy for predictable results.
:::

## Conditional Aggregations

Aggregate only rows matching a condition:

```typescript
import { sum, count, col } from "molniya";

// Sum only high-value orders
const result = df.groupBy("region", [
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
]);
```

## Distinct Aggregations

Count distinct values:

```typescript
import { countDistinct } from "molniya";

// Count unique customers per region
const result = df.groupBy("region", [
  { name: "unique_customers", expr: countDistinct("customer_id") },
  { name: "total_orders", expr: count() }
]);
```

## Aggregating Expressions

Use expressions within aggregations:

```typescript
import { sum, avg, col } from "molniya";

// Sum of calculated values
const result = df.groupBy("category", [
  { name: "total_revenue", expr: sum(col("price").mul(col("quantity"))) },
  { name: "avg_discount", expr: avg(col("discount_rate").mul(100)) }
]);
```

## Window Functions (Coming Soon)

Window functions perform calculations across sets of rows related to the current row:

```typescript
// Rank rows within groups
// Moving averages
// Running totals
// Lead/Lag values
```

## Aggregation with Multiple Groups

Complex grouping scenarios:

```typescript
// Group by computed columns
const byAgeGroup = df
  .withColumn("age_group",
    when(col("age").lt(25), "18-24")
      .when(col("age").lt(35), "25-34")
      .when(col("age").lt(50), "35-49")
      .otherwise("50+")
  )
  .groupBy("age_group", [
    { name: "count", expr: count() },
    { name: "avg_income", expr: avg("income") }
  ]);

// Nested grouping (group then subgroup)
const nested = df
  .groupBy(["region", "product_category"], [
    { name: "sales", expr: sum("amount") },
    { name: "units", expr: sum("quantity") }
  ]);
```

## Common Aggregation Patterns

### Summary Statistics

```typescript
// Get comprehensive stats for a column
const summary = await df.agg([
  count(),
  count("value"),
  sum("value"),
  avg("value"),
  min("value"),
  max("value"),
  std("value"),
  median("value")
]);
```

### Percentage of Total

```typescript
// Calculate percentage of total
const total = await df.agg(sum("amount"));

const withPercent = df
  .withColumn("percent_of_total", 
    col("amount").div(total).mul(100)
  );
```

### Running Totals

```typescript
// Cumulative sum within groups
const running = df
  .sort(asc("date"))
  .groupBy("category", [
    { name: "running_total", expr: sum("amount") }
  ]);
```

### Top N per Group

```typescript
// Get top 3 products per category
const top3 = df
  .sort(desc("sales"))
  .groupBy("category")
  .head(3);
```

## Null Handling in Aggregations

Aggregations handle null values automatically:

| Function | Null Behavior |
|----------|---------------|
| `sum()` | Ignores nulls (sum of non-null values) |
| `avg()` | Ignores nulls (average of non-null values) |
| `count()` | Counts all rows including nulls |
| `count(col)` | Counts only non-null values |
| `min()` / `max()` | Ignores nulls |

```typescript
// Explicit null handling
const result = df.groupBy("category", [
  { name: "total", expr: sum(col("amount").fillNull(0)) },
  { name: "non_null_count", expr: count("amount") },
  { name: "total_count", expr: count() }
]);
```

## Performance Considerations

1. **Pre-filter when possible**: Filter before grouping to reduce data volume
2. **Use appropriate types**: Smaller numeric types improve aggregation speed
3. **Limit groups**: Too many unique group keys can cause memory issues
4. **Sort for first/last**: Ensure data is sorted before using `first()`/`last()`

```typescript
// Good: Filter first
const result = df
  .filter(col("year").eq(2024))
  .groupBy("month", [{ name: "total", expr: sum("amount") }]);

// Less efficient: Group then filter
const result = df
  .groupBy(["year", "month"], [{ name: "total", expr: sum("amount") }])
  .filter(col("year").eq(2024));
```

## Best Practices

1. **Name aggregation outputs**: Always provide clear names for aggregated columns
2. **Handle nulls explicitly**: Use `fillNull()` when nulls should be treated as 0
3. **Filter before aggregate**: Reduces memory usage and improves performance
4. **Use appropriate functions**: Choose the right aggregation for your analysis
5. **Consider data types**: Use `int64` or `float64` for sums of large values to prevent overflow
