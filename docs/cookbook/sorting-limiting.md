# Sorting & Limiting

Recipes for ordering DataFrame rows and limiting result sets.

## Basic Sorting

### Sort by Single Column

```typescript
import { col, asc, desc } from "Molniya";

// Ascending order (default)
df.sort(asc("name"))

// Descending order
df.sort(desc("price"))
```

### Sort by Multiple Columns

```typescript
// Sort by category, then by price within each category
df.sort(asc("category"), desc("price"))

// Equivalent using array syntax
df.sort([asc("category"), desc("price")])
```

## Common Sorting Patterns

### Top N Items

```typescript
import { desc } from "Molniya";

// Top 10 most expensive products
const top10 = df.sort(desc("price")).limit(10);

// Top 5 best-selling items
const top5 = df.sort(desc("sales_count")).limit(5);
```

### Bottom N Items

```typescript
import { asc } from "Molniya";

// 10 cheapest products
const cheapest = df.sort(asc("price")).limit(10);

// 5 lowest scores
const lowest = df.sort(asc("score")).limit(5);
```

### Sort by Computed Column

```typescript
import { col, desc } from "Molniya";

// Sort by total (price * quantity)
df.withColumn("total", col("price").mul(col("quantity")))
  .sort(desc("total"))
```

## Advanced Sorting

### Sort with Nulls

```typescript
import { asc, desc } from "Molniya";

// Nulls first (default)
df.sort(asc("middle_name"))  // nulls appear first

// Nulls last
df.sort(asc("middle_name").nullsLast())

// Descending with nulls last
df.sort(desc("rating").nullsLast())
```

### Sort by Multiple Criteria

```typescript
import { asc, desc } from "Molniya";

// Complex sort: active first, then by score descending, then by name
df.sort(
  desc("is_active"),    // true (1) before false (0)
  desc("score"),        // Highest scores first
  asc("name")           // Alphabetical within same score
)
```

### Sort by String Length

```typescript
import { col, length, desc } from "Molniya";

// Sort by name length (longest first)
df.sort(desc(length(col("name"))))
```

## Limiting Results

### Basic Limit

```typescript
// Get first 100 rows
const limited = df.limit(100);

// Get first 10 rows
const sample = df.limit(10);
```

### Pagination

```typescript
// Page 1: rows 0-9
const page1 = df.limit(10);

// Page 2: rows 10-19
const page2 = df.offset(10).limit(10);

// Page 3: rows 20-29
const page3 = df.offset(20).limit(10);
```

### Head and Tail

```typescript
// First 5 rows (convenience method)
const first5 = df.head(5);

// Last 5 rows
const last5 = df.tail(5);
```

## Combining Sort and Limit

### Top Performers

```typescript
import { desc } from "Molniya";

// Top 20 employees by sales
const topPerformers = df
  .sort(desc("total_sales"))
  .limit(20);
```

### Recent Items

```typescript
import { desc } from "Molniya";

// 50 most recent orders
const recent = df
  .sort(desc("order_date"))
  .limit(50);
```

### Worst Performers

```typescript
import { asc } from "Molniya";

// 10 products with lowest ratings
const worstRated = df
  .filter(col("rating").isNotNull())
  .sort(asc("rating"))
  .limit(10);
```

## Sorting After Aggregation

### Top Categories

```typescript
import { sum, desc } from "Molniya";

// Top 5 categories by revenue
const topCategories = df
  .groupBy("category", [
    { name: "revenue", expr: sum("amount") }
  ])
  .sort(desc("revenue"))
  .limit(5);
```

### Bottom Groups

```typescript
import { count, asc } from "Molniya";

// Categories with fewest products
const smallestCategories = df
  .groupBy("category", [
    { name: "product_count", expr: count() }
  ])
  .sort(asc("product_count"))
  .limit(10);
```

## Random Sampling

### Sample Rows

```typescript
// Get random 100 rows (shuffle then limit)
const sample = df.shuffle().limit(100);

// 10% sample
const tenPercent = df.sample(0.1);
```

## Sorting with Null Handling

### Prioritize Non-Null Values

```typescript
import { col, desc } from "Molniya";

// Sort by priority (non-null first), then by date
df.sort(
  col("priority").isNull(),  // false (0) before true (1)
  desc("created_at")
)
```

### Fill Then Sort

```typescript
import { col, asc } from "Molniya";

// Replace nulls with 0, then sort
df.withColumn("score_filled", col("score").fillNull(0))
  .sort(desc("score_filled"))
```

## Performance Tips

### Sort Early

```typescript
// Good: Sort before limiting
df.sort(desc("date")).limit(100);

// Less efficient: Limit would need to scan all anyway
// (but sorting is the expensive part)
```

### Use Indexes

When reading from files, filter first if possible:

```typescript
// Filter before sort for better performance
df.filter(col("year").eq(2024))
  .sort(desc("amount"))
  .limit(10);
```

## Common Recipes

### Leaderboard

```typescript
import { desc } from "Molniya";

// Top 100 players by score
const leaderboard = df
  .select("player_name", "score", "level")
  .sort(desc("score"), desc("level"))
  .limit(100)
  .withColumn("rank", range(1, 101));
```

### Recent Activity Feed

```typescript
import { desc } from "Molniya";

// Latest 20 activities
const feed = df
  .filter(col("is_public").eq(true))
  .sort(desc("created_at"))
  .limit(20);
```

### Price Range Display

```typescript
import { asc, desc } from "Molniya";

// Show price range: 5 cheapest and 5 most expensive
const cheapest = df.sort(asc("price")).limit(5);
const mostExpensive = df.sort(desc("price")).limit(5);
const priceRange = cheapest.union(mostExpensive);
```

### Alphabetical Listing

```typescript
import { asc } from "Molniya";

// A-Z listing with pagination
const page = df
  .sort(asc("last_name"), asc("first_name"))
  .offset(pageNum * pageSize)
  .limit(pageSize);
```

## Error Handling

### Empty Results

```typescript
// Handle empty DataFrames gracefully
const result = df.sort(desc("score")).limit(10);
const count = await result.count();

if (count === 0) {
  console.log("No results found");
}
```

### Invalid Sort Column

```typescript
// Check column exists before sorting
if (df.columnNames.includes("score")) {
  const sorted = df.sort(desc("score"));
}
```
