# Grouping & Aggregating

Summarize data by categories using `groupBy()` and aggregation functions.

## Basic GroupBy

### Simple Count

```typescript
import { col, count } from "molniya";

// Count rows per category
const result = df
  .groupBy("category")
  .count();

await result.show();
// Output: category, count
```

### Multiple Aggregations

```typescript
import { col, sum, avg, min, max, count } from "molniya";

const result = df
  .groupBy("department", [
    { name: "total_salary", expr: sum("salary") },
    { name: "avg_salary", expr: avg("salary") },
    { name: "min_salary", expr: min("salary") },
    { name: "max_salary", expr: max("salary") },
    { name: "employee_count", expr: count() }
  ]);

await result.show();
```

## Aggregation Functions

### sum()

Calculate the total of a numeric column.

```typescript
import { sum, col } from "molniya";

// Sum of a column
df.groupBy("category", [
  { name: "total", expr: sum("amount") }
])

// Sum of an expression
df.groupBy("category", [
  { name: "total_with_tax", expr: sum(col("amount").mul(1.1)) }
])
```

### avg()

Calculate the average (mean) of a numeric column.

```typescript
import { avg } from "molniya";

df.groupBy("category", [
  { name: "average_price", expr: avg("price") }
])
```

::: info Null Handling
`avg()` automatically excludes null values from the calculation. If all values are null, the result is null.
:::

### min() / max()

Find the minimum or maximum value.

```typescript
import { min, max } from "molniya";

df.groupBy("category", [
  { name: "lowest_price", expr: min("price") },
  { name: "highest_price", expr: max("price") }
])
```

### count()

Count rows or non-null values.

```typescript
import { count } from "molniya";

// Count all rows
df.groupBy("category", [
  { name: "row_count", expr: count() }
])

// Count non-null values in a column
df.groupBy("category", [
  { name: "valid_emails", expr: count("email") }
])
```

### first() / last()

Get the first or last value in each group.

```typescript
import { first, last } from "molniya";

df.groupBy("category", [
  { name: "first_order", expr: first("order_date") },
  { name: "last_order", expr: last("order_date") }
])
```

::: warning Ordering
`first()` and `last()` return values based on the physical order of data in the file. Use `sort()` before grouping if you need specific ordering.
:::

## Multi-Column Grouping

Group by multiple columns for more granular summaries:

```typescript
df.groupBy(["year", "month", "category"], [
  { name: "total_sales", expr: sum("amount") },
  { name: "order_count", expr: count() }
])
```

## Shortcut Methods

The `RelationalGroupedDataset` returned by `groupBy()` has shortcut methods:

```typescript
const grouped = df.groupBy("category");

// Instead of: grouped.agg([{ name: "sum_amount", expr: sum("amount") }])
grouped.sum("amount");  // Creates column "sum(amount)"

// Similarly:
grouped.avg("price");   // Column: "mean(price)"
grouped.min("price");   // Column: "min(price)"
grouped.max("price");   // Column: "max(price)"
grouped.count();        // Column: "count"
```

## Practical Examples

### Sales Report by Category

```typescript
const report = await df
  .groupBy("category", [
    { name: "total_revenue", expr: sum(col("price").mul(col("quantity"))) },
    { name: "total_orders", expr: count() },
    { name: "avg_order_value", expr: avg(col("price").mul(col("quantity"))) },
    { name: "unique_customers", expr: count("customer_id") }
  ])
  .sort(desc("total_revenue"))
  .limit(10)
  .toArray();
```

### Daily Active Users

```typescript
const dau = await df
  .filter(col("event_type").eq("login"))
  .groupBy("date", [
    { name: "active_users", expr: count("user_id") }
  ])
  .sort("date")
  .toArray();
```

### Cohort Analysis

```typescript
const cohorts = await df
  .groupBy(["signup_month", "activity_month"], [
    { name: "retained_users", expr: count("user_id") }
  ])
  .toArray();
```

### Pareto Analysis (80/20 Rule)

```typescript
// Find top 20% of customers generating 80% of revenue
const customerTotals = await df
  .groupBy("customer_id", [
    { name: "total_spent", expr: sum("amount") }
  ])
  .sort(desc("total_spent"))
  .toArray();

// Calculate cumulative percentage
// (This would need to be done in JavaScript after collection)
```

## Combining with Other Operations

### Filter Before Grouping

```typescript
// Only include completed orders
const result = await df
  .filter(col("status").eq("completed"))
  .groupBy("category", [
    { name: "total", expr: sum("amount") }
  ])
  .collect();
```

### Transform After Grouping

```typescript
const result = await df
  .groupBy("category", [
    { name: "total", expr: sum("amount") }
  ])
  .withColumn("percentage", 
    col("total").div(col("total").sum())
  )
  .collect();
```

::: warning Post-GroupBy Operations
Currently, operations after `groupBy()` are limited. Complex transformations may need to be done after `.collect()`.
:::

## Common Patterns

### Top N per Group

Molniya doesn't have a built-in window function for "top N per group" yet. Workaround:

```typescript
// Get all data, then process in JavaScript
const allData = await df
  .groupBy(["category", "product"], [
    { name: "sales", expr: sum("amount") }
  ])
  .toArray();

// Process in JS to get top 3 per category
const topPerCategory = allData.reduce((acc, row) => {
  const cat = row.category;
  if (!acc[cat]) acc[cat] = [];
  acc[cat].push(row);
  acc[cat].sort((a, b) => b.sales - a.sales);
  acc[cat] = acc[cat].slice(0, 3);
  return acc;
}, {});
```

### Running Totals

Running totals also require post-processing:

```typescript
const daily = await df
  .groupBy("date", [
    { name: "sales", expr: sum("amount") }
  ])
  .sort("date")
  .toArray();

// Calculate running total in JS
let runningTotal = 0;
const withRunningTotal = daily.map(row => ({
  ...row,
  running_total: runningTotal += row.sales
}));
```

## Performance Considerations

1. **GroupBy materializes data**: The entire dataset is loaded into memory for grouping
2. **Filter first**: Reduce data volume before grouping
3. **Use appropriate types**: Integer grouping keys are faster than strings
4. **Limit groups**: Extremely high cardinality grouping (millions of unique keys) may cause memory issues

```typescript
// Good: Filter first
const result = await df
  .filter(col("year").eq(2024))
  .groupBy("category", [...])
  .collect();

// Less efficient: Groups all years
const result = await df
  .groupBy(["year", "category"], [...])
  .filter(col("year").eq(2024))
  .collect();
```
