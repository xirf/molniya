# Column References

Column references are expressions that point to existing columns in a DataFrame. They provide a fluent API for building transformations.

## Creating Column References

Use the `col()` function to create column references:

```typescript
import { col } from "molniya";

// Basic column reference
col("id")
col("name")
col("price")
```

Column references can be used anywhere an expression is expected:

```typescript
// In filters
df.filter(col("age").gte(18))

// In projections
df.select(col("id"), col("name"))

// In calculations
df.withColumn("total", col("price").mul(col("quantity")))
```

## Column Reference Methods

Column references provide chainable methods for building expressions:

### Arithmetic Methods

```typescript
import { col } from "molniya";

// Addition
col("a").add(col("b"))      // a + b
col("value").add(10)        // value + 10

// Subtraction
col("total").sub(col("tax"))  // total - tax

// Multiplication
col("price").mul(col("qty"))  // price * qty
col("price").mul(0.9)         // price * 0.9

// Division
col("sum").div(col("count"))  // sum / count

// Modulo
col("value").mod(10)          // value % 10

// Negation
col("value").neg()            // -value
```

### Comparison Methods

```typescript
import { col } from "molniya";

// Equal
col("status").eq("active")
col("id").eq(col("ref_id"))

// Not equal
col("type").neq("deleted")

// Greater than
col("age").gt(18)

// Greater than or equal
col("score").gte(60)

// Less than
col("price").lt(100)

// Less than or equal
col("quantity").lte(10)
```

### Null Handling Methods

```typescript
import { col } from "molniya";

// Check for null
col("email").isNull()

// Check for non-null
col("email").isNotNull()

// Replace null with value
col("discount").fillNull(0)

// Replace null with another column
col("nickname").fillNull(col("name"))
```

### String Methods

```typescript
import { col } from "molniya";

// String length
col("name").length()

// Substring (start, length)
col("phone").substring(0, 3)

// Contains substring
col("email").contains("@")

// Upper case
col("code").upper()

// Lower case
col("email").lower()

// Starts with
col("phone").startsWith("+1")

// Ends with
col("file").endsWith(".csv")
```

### Date/Time Methods

```typescript
import { col } from "molniya";

// Extract year
col("date").year()

// Extract month
col("date").month()

// Extract day
col("date").day()

// Extract day of week
col("date").dayOfWeek()

// Extract quarter
col("date").quarter()

// Add days
col("date").addDays(7)

// Subtract days
col("date").subDays(30)

// Difference in days
col("end").diffDays(col("start"))
```

### Type Casting

```typescript
import { col, DType } from "molniya";

// Cast to integer
col("price").cast(DType.int32)

// Cast to float
col("quantity").cast(DType.float64)

// Cast to string
col("id").cast(DType.string)
```

### Aggregation Context Methods

These methods work within groupBy operations:

```typescript
import { col, sum, avg, min, max, count } from "molniya";

// Sum of column values
sum(col("amount"))

// Average
avg(col("score"))

// Minimum
min(col("price"))

// Maximum
max(col("price"))

// Count non-null values
count(col("name"))

// Count all rows
count()

// First value in group
first(col("name"))

// Last value in group
last(col("name"))
```

## Chaining Methods

Column reference methods can be chained to build complex expressions:

```typescript
import { col, lit } from "molniya";

// Complex calculation
df.withColumn("final_price",
  col("base_price")
    .mul(lit(1).sub(col("discount")))  // Apply discount
    .add(col("tax"))                    // Add tax
    .round(2)                           // Round to 2 decimals
)

// Complex filter
df.filter(
  col("name")
    .lower()
    .contains("search term")
)
```

## Multiple Column References

Reference multiple columns in a single operation:

```typescript
import { col } from "molniya";

// Select multiple columns
df.select(col("id"), col("name"), col("email"))

// Multiple columns in calculation
df.withColumn("total",
  col("price").mul(col("quantity")).add(col("shipping"))
)

// Multiple columns in filter
df.filter(
  col("min_price").lte(col("target_price"))
    .and(col("max_price").gte(col("target_price")))
)
```

## Column Reference vs String

Molniya accepts both column references and strings in most methods:

```typescript
// These are equivalent
df.select("id", "name")
df.select(col("id"), col("name"))

// Column reference is required for expressions
df.withColumn("total", col("price").mul(col("quantity")))

// String works for simple references
df.filter(col("status").eq("active"))
df.drop("temp_column")
```

::: tip Prefer `col()` for expressions
Using `col()` makes your intent clear and enables better type inference and autocompletion.
:::

## Dynamic Column References

When column names are determined at runtime:

```typescript
import { col } from "molniya";

function filterByColumn(df: DataFrame, columnName: string, value: unknown) {
  return df.filter(col(columnName).eq(value));
}

// Usage
const filtered = filterByColumn(df, "status", "active");
```

## Column Reference in Aggregations

Column references work differently in aggregation contexts:

```typescript
import { col, sum, avg } from "molniya";

// Regular context - per-row operation
df.withColumn("doubled", col("value").mul(2))

// Aggregation context - across rows
df.groupBy("category", [
  { name: "total", expr: sum(col("amount")) },
  { name: "average", expr: avg(col("score")) }
])
```

## Best Practices

1. **Use `col()` consistently**: Improves readability and type safety
2. **Chain method calls**: Build complex expressions fluently
3. **Handle nulls explicitly**: Use `isNull()`, `isNotNull()`, `fillNull()`
4. **Cast when needed**: Use `cast()` to ensure correct types
5. **Break complex chains**: Use intermediate variables for very complex expressions

## Common Patterns

### Calculated Columns

```typescript
// Price with tax
df.withColumn("price_with_tax",
  col("price").mul(1.08).round(2)
)

// Full name
df.withColumn("full_name",
  col("first_name").add(" ").add(col("last_name"))
)

// Age group
df.withColumn("age_group",
  when(col("age").lt(18), "Minor")
    .when(col("age").lt(65), "Adult")
    .otherwise("Senior")
)
```

### Conditional Logic

```typescript
// Status based on multiple conditions
df.withColumn("status",
  when(
    col("paid").eq(true).and(col("shipped").eq(true)),
    "Complete"
  )
  .when(col("paid").eq(true), "Paid")
  .otherwise("Pending")
)
```

### Date Calculations

```typescript
// Days since registration
df.withColumn("days_active",
  lit(new Date()).diffDays(col("registered_date"))
)

// Is recent order
df.withColumn("is_recent",
  col("order_date").diffDays(lit(new Date())).lte(30)
)
```
