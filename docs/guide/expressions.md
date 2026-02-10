# Building Expressions

Expressions are the building blocks of data transformations in Molniya. They represent computations that are evaluated lazily when you execute a DataFrame action.

## What is an Expression?

An expression (`Expr`) is a description of a computation, not the result itself. Expressions are combined to build transformation pipelines:

```typescript
import { col, lit } from "molniya";

// These are expressions - descriptions of computations
const priceExpr = col("price");           // Reference to "price" column
const taxExpr = lit(0.1);                  // Literal value 0.1
const totalExpr = priceExpr.mul(1.1);      // Arithmetic expression

// Nothing is computed yet - we're just building the plan
```

## Expression Types

### Column References

Reference existing columns using [`col()`](./column-refs):

```typescript
import { col } from "molniya";

col("id")           // Simple reference
col("price")        // Numeric column
col("name")         // String column
```

### Literals

Create constant values using `lit()`:

```typescript
import { lit } from "molniya";

lit(100)            // Number literal
lit("active")       // String literal
lit(true)           // Boolean literal
lit(null)           // Null literal
lit(123456789n)     // BigInt literal
```

Literals are useful for comparisons and calculations:

```typescript
df.filter(col("status").eq(lit("active")))
  .withColumn("tax_rate", lit(0.08))
```

## Arithmetic Expressions

### Basic Operations

```typescript
import { col, add, sub, mul, div, mod, neg } from "molniya";

// Using method chaining
df.withColumn("total", col("price").mul(col("quantity")))
  .withColumn("discount", col("total").mul(0.1))
  .withColumn("final", col("total").sub(col("discount")))

// Using operator functions
df.withColumn("avg", div(col("sum"), col("count")))
  .withColumn("remainder", mod(col("value"), lit(10)))
  .withColumn("negative", neg(col("value")))
```

### Operator Reference

| Operator | Method | Description |
|----------|--------|-------------|
| `+` | `add()` | Addition |
| `-` | `sub()` | Subtraction |
| `*` | `mul()` | Multiplication |
| `/` | `div()` | Division |
| `%` | `mod()` | Modulo |
| `-` | `neg()` | Negation |

## Comparison Expressions

### Comparison Operators

```typescript
import { col } from "molniya";

// Equal
col("status").eq("active")

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

### Null Checks

```typescript
// Is null
col("email").isNull()

// Is not null
col("email").isNotNull()
```

## Logical Expressions

Combine conditions with logical operators:

```typescript
import { and, or, not } from "molniya";

// AND - all conditions must be true
and(
  col("age").gte(18),
  col("status").eq("active")
)

// OR - any condition can be true
or(
  col("category").eq("electronics"),
  col("category").eq("computers")
)

// NOT - negate a condition
not(col("deleted").eq(true))

// Combining logical operators
and(
  col("age").gte(18),
  or(
    col("country").eq("US"),
    col("country").eq("CA")
  )
)
```

## String Expressions

```typescript
import { col, length, substring, contains, upper, lower } from "molniya";

// String length
col("name").length()

// Substring
col("phone").substring(0, 3)

// Contains
col("email").contains("@")

// Case conversion
col("name").upper()
col("email").lower()

// Concatenation
col("first").add(" ").add(col("last"))
```

## Date/Time Expressions

```typescript
import { col, year, month, day, addDays, diffDays } from "molniya";

// Extract components
year(col("order_date"))
month(col("order_date"))
day(col("order_date"))

// Date arithmetic
addDays(col("date"), 7)
subDays(col("date"), 30)
diffDays(col("end"), col("start"))
```

## Conditional Expressions

### When/Then/Otherwise

Create conditional logic:

```typescript
import { when, col } from "molniya";

// Simple if-then-else
when(col("score").gte(90), "A")
  .when(col("score").gte(80), "B")
  .when(col("score").gte(70), "C")
  .otherwise("F")

// Used in withColumn
df.withColumn("grade", 
  when(col("score").gte(90), "A")
    .when(col("score").gte(80), "B")
    .otherwise("C")
)
```

## Expression Composition

Expressions can be arbitrarily complex:

```typescript
import { col, lit, and, or } from "molniya";

// Complex calculation
df.withColumn("discounted_price",
  col("price")
    .mul(lit(1).sub(col("discount_rate")))
    .add(col("tax"))
)

// Complex filter
df.filter(and(
  or(
    col("category").eq("electronics"),
    col("category").eq("computers")
  ),
  col("price").gte(100),
  col("in_stock").eq(true)
))
```

## Type Casting in Expressions

Convert between types within expressions:

```typescript
import { col, cast, DType } from "molniya";

// Cast to different type
df.withColumn("price_int", cast(col("price"), DType.int32))
  .withColumn("id_string", cast(col("id"), DType.string))
```

## Expression Evaluation Context

Expressions are evaluated in different contexts:

### Row Context

Most expressions are evaluated once per row:

```typescript
// Evaluated for each row
df.withColumn("doubled", col("value").mul(2))
```

### Aggregation Context

Aggregation expressions collapse multiple rows into one value:

```typescript
import { sum, avg, count } from "molniya";

// Evaluated across groups
df.groupBy("category", [
  { name: "total", expr: sum("amount") },
  { name: "average", expr: avg("price") },
  { name: "count", expr: count() }
])
```

## Expression Optimization

Molniya optimizes expressions before execution:

1. **Constant folding**: `lit(2).add(lit(3))` → `lit(5)`
2. **Predicate pushdown**: Filters applied as early as possible
3. **Common subexpression elimination**: Duplicate expressions computed once

## Best Practices

1. **Use `col()` for columns**: It's type-safe and enables autocompletion
2. **Use `lit()` for constants**: Makes intent clear
3. **Break complex expressions**: Use multiple `withColumn` calls for readability
4. **Leverage method chaining**: Build expressions fluently
5. **Consider null handling**: Use `isNull()`/`isNotNull()` explicitly

## Common Expression Patterns

### Percentage Calculation

```typescript
df.withColumn("percentage", 
  col("part").div(col("whole")).mul(100)
)
```

### Running Total (within groups)

```typescript
df.groupBy("category", [
  { name: "running_total", expr: sum("amount") }
])
```

### Conditional Aggregation

```typescript
df.groupBy("region", [
  { name: "high_value_count", expr: count().filter(col("amount").gt(1000)) }
])
```

### Date Bucketing

```typescript
df.withColumn("month_bucket", 
  year(col("date")).mul(100).add(month(col("date")))
)
```
