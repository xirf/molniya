# Filtering Data

Select rows based on conditions using the `filter()` method.

## Basic Filtering

### Single Condition

```typescript
import { col } from "Molniya";

// Greater than
df.filter(col("age").gt(18))

// Greater than or equal
df.filter(col("score").gte(60))

// Less than
df.filter(col("price").lt(100))

// Less than or equal
df.filter(col("quantity").lte(10))

// Equal
df.filter(col("status").eq("active"))

// Not equal
df.filter(col("type").neq("deleted"))
```

### Null Checks

```typescript
// Is null
df.filter(col("email").isNull())

// Is not null
df.filter(col("email").isNotNull())
```

## Combining Conditions

### AND (All conditions must match)

```typescript
import { and, col } from "Molniya";

// Method 1: Using and()
df.filter(and(
  col("age").gte(18),
  col("age").lt(65),
  col("active").eq(true)
))

// Method 2: Chaining filters (more efficient)
df
  .filter(col("age").gte(18))
  .filter(col("age").lt(65))
  .filter(col("active").eq(true))
```

::: tip Chaining vs AND
Chaining `filter()` calls is often more readable and can be more efficient as it allows the optimizer to process conditions incrementally.
:::

### OR (Any condition can match)

```typescript
import { or, col } from "Molniya";

df.filter(or(
  col("status").eq("pending"),
  col("status").eq("processing")
))
```

### NOT (Negate a condition)

```typescript
import { not, col } from "Molniya";

// Exclude specific status
df.filter(not(col("status").eq("deleted")))

// Equivalent to:
df.filter(col("status").neq("deleted"))
```

### Complex Combinations

```typescript
import { and, or, col } from "Molniya";

// (age >= 18 AND age < 65) OR (type === 'senior' AND age >= 65)
df.filter(or(
  and(
    col("age").gte(18),
    col("age").lt(65)
  ),
  and(
    col("type").eq("senior"),
    col("age").gte(65)
  )
))
```

## Range Filtering

### Between (Inclusive)

```typescript
// Value between low and high (inclusive)
df.filter(col("age").between(18, 65))

// Equivalent to:
df.filter(and(
  col("age").gte(18),
  col("age").lte(65)
))
```

## String Filtering

### Contains

```typescript
// String contains substring
df.filter(col("email").contains("@company.com"))
```

### Starts With / Ends With

```typescript
// Starts with prefix
df.filter(col("name").startsWith("John"))

// Ends with suffix
df.filter(col("email").endsWith("@gmail.com"))
```

::: warning String Performance
String operations are case-sensitive and work on dictionary-encoded strings. For large datasets, consider normalizing case before filtering.
:::

## Practical Examples

### Filter by Date Range

```typescript
// Assuming timestamps as milliseconds
df.filter(and(
  col("created_at").gte(1704067200000),  // Jan 1, 2024
  col("created_at").lt(1735689600000)     // Jan 1, 2025
))
```

### Filter by Multiple Categories

```typescript
const allowedCategories = ["electronics", "books", "clothing"];

// Build OR condition dynamically
const condition = allowedCategories
  .map(cat => col("category").eq(cat))
  .reduce((acc, curr) => or(acc, curr));

df.filter(condition)
```

### Filter with Calculated Values

```typescript
import { col, lit } from "Molniya";

// Filter where price * quantity > 1000
df
  .withColumn("total", col("price").mul(col("quantity")))
  .filter(col("total").gt(1000))
```

### Excluding Specific Values

```typescript
const excludedStatuses = ["deleted", "archived", "banned"];

// Build NOT(OR) condition
const excludeCondition = excludedStatuses
  .map(status => col("status").eq(status))
  .reduce((acc, curr) => or(acc, curr));

df.filter(not(excludeCondition))

// Or chain neq filters
df
  .filter(col("status").neq("deleted"))
  .filter(col("status").neq("archived"))
  .filter(col("status").neq("banned"))
```

## Common Patterns

### Active Records Only

```typescript
const active = df.filter(and(
  col("deleted_at").isNull(),
  col("status").eq("active")
))
```

### Recent Records

```typescript
const oneWeekAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;
const recent = df.filter(col("created_at").gte(oneWeekAgo))
```

### Non-Empty Strings

```typescript
const valid = df.filter(and(
  col("name").isNotNull(),
  col("name").neq("")
))
```

## Performance Tips

1. **Filter early**: Apply filters as soon as possible to reduce data volume
2. **Use simple comparisons**: `eq()`, `gt()`, etc. are faster than string operations
3. **Chain filters**: Multiple chained filters can be more efficient than complex AND expressions
4. **Avoid redundant filters**: Don't filter the same condition twice

```typescript
// Good: Filter early, then process
const result = await df
  .filter(col("year").eq(2024))      // Filter first
  .withColumn("bonus", ...)           // Then transform
  .sort("amount")
  .limit(100)
  .collect();

// Less efficient: Transform everything, then filter
const result = await df
  .withColumn("bonus", ...)           // Processes all rows
  .filter(col("year").eq(2024))      // Then filters
  .collect();
```
