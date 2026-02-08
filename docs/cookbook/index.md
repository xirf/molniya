# Cookbook

Practical recipes for common data manipulation tasks with Molniya.

This cookbook contains patterns and solutions for everyday data processing needs. Each recipe is self-contained and can be adapted to your specific use case.

## Recipe Categories

### Data Selection
- [Filtering Data](./filtering) - Select rows based on conditions
- [Sorting & Limiting](./sorting-limiting) - Order and paginate results

### Data Transformation
- [Transforming Columns](./transforming) - Add and modify columns
- [Type Casting](./type-casting) - Convert between data types
- [Handling Nulls](./handling-nulls) - Deal with missing data

### Data Aggregation
- [Grouping & Aggregating](./grouping) - Summarize data by categories

### Data Combination
- [Joining DataFrames](./joining) - Combine data from multiple sources

### String Processing
- [String Operations](./string-ops) - Work with text data

## Quick Reference

### Common Patterns

```typescript
// Filter and select
df.filter(col("status").eq("active")).select("id", "name")

// Add computed column
df.withColumn("total", col("price").add(col("tax")))

// Group and aggregate
df.groupBy("category", [
  { name: "sum", expr: sum("amount") },
  { name: "count", expr: count() }
])

// Sort and limit
df.sort(desc("amount")).limit(10)

// Chain everything
const result = await df
  .filter(col("year").eq(2024))
  .withColumn("discounted", col("price").mul(0.9))
  .groupBy("category", [
    { name: "total", expr: sum("discounted") }
  ])
  .sort(desc("total"))
  .limit(5)
  .toArray();
```

## Before You Start

Most recipes assume you have:

```typescript
import { 
  readCsv, fromRecords, col, lit, and, or, not,
  sum, avg, min, max, count, asc, desc, DType 
} from "molniya";
```

## Need More Help?

- Check the [API Reference](../api/) for detailed method documentation
- See [Examples](../examples/) for complete real-world scenarios
- Review [Core Concepts](../guide/core-concepts) for deeper understanding
