# ColumnRef

The `ColumnRef` class provides a fluent API for building column expressions.

## Overview

`ColumnRef` is returned by the `col()` function and provides chainable methods for creating expressions.

```typescript
import { col } from "molniya";

// Create a column reference
const priceCol = col("price");

// Chain methods to build expressions
priceCol.mul(1.1).add(col("tax"));
```

## Creating Column References

### col()

```typescript
col(name: string): ColumnRef
```

**Example:**

```typescript
import { col } from "molniya";

col("id")
col("customer_name")
col("total_amount")
```

## Comparison Methods

### eq()

Equal comparison.

```typescript
eq(other: Expr | number | string | boolean): ComparisonExpr
```

**Example:**

```typescript
col("status").eq("active")
col("id").eq(123)
col("verified").eq(true)
```

### neq()

Not equal comparison.

```typescript
neq(other: Expr | number | string | boolean): ComparisonExpr
```

**Example:**

```typescript
col("status").neq("deleted")
col("id").neq(0)
```

### gt()

Greater than.

```typescript
gt(other: Expr | number): ComparisonExpr
```

**Example:**

```typescript
col("age").gt(18)
col("price").gt(100)
```

### gte()

Greater than or equal.

```typescript
gte(other: Expr | number): ComparisonExpr
```

**Example:**

```typescript
col("score").gte(60)  // Passing grade
col("balance").gte(0)  // Non-negative
```

### lt()

Less than.

```typescript
lt(other: Expr | number): ComparisonExpr
```

**Example:**

```typescript
col("age").lt(65)
col("temperature").lt(100)
```

### lte()

Less than or equal.

```typescript
lte(other: Expr | number): ComparisonExpr
```

**Example:**

```typescript
col("price").lte(50)  // Budget items
```

### between()

Check if value is within a range (inclusive).

```typescript
between(low: Expr | number, high: Expr | number): BetweenExpr
```

**Example:**

```typescript
col("age").between(18, 65)
col("score").between(0, 100)
```

## Null Check Methods

### isNull()

Check if value is null.

```typescript
isNull(): NullCheckExpr
```

**Example:**

```typescript
col("email").isNull()
```

### isNotNull()

Check if value is not null.

```typescript
isNotNull(): NullCheckExpr
```

**Example:**

```typescript
col("email").isNotNull()
```

## Arithmetic Methods

### add()

Add to another expression.

```typescript
add(other: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
col("price").add(col("tax"))
col("score").add(10)  // Bonus points
```

### sub()

Subtract another expression.

```typescript
sub(other: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
col("total").sub(col("discount"))
```

### mul()

Multiply by another expression.

```typescript
mul(other: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
col("price").mul(0.9)  // 10% discount
col("price").mul(col("quantity"))
```

### div()

Divide by another expression.

```typescript
div(other: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
col("total").div(col("count"))  // Average
```

### mod()

Modulo (remainder) operation.

```typescript
mod(other: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
col("n").mod(2)  // Check even/odd
```

### neg()

Negate the value.

```typescript
neg(): NegExpr
```

**Example:**

```typescript
col("debt").neg()  // Negative debt = credit
```

## String Methods

### contains()

Check if string contains a pattern.

```typescript
contains(pattern: string): StringOpExpr
```

**Example:**

```typescript
col("email").contains("@gmail.com")
col("description").contains("urgent")
```

### startsWith()

Check if string starts with a pattern.

```typescript
startsWith(pattern: string): StringOpExpr
```

**Example:**

```typescript
col("name").startsWith("Dr.")
col("phone").startsWith("+1")
```

### endsWith()

Check if string ends with a pattern.

```typescript
endsWith(pattern: string): StringOpExpr
```

**Example:**

```typescript
col("email").endsWith("@company.com")
col("file").endsWith(".pdf")
```

## Utility Methods

### alias()

Create an aliased expression (for renaming in aggregations).

```typescript
alias(name: string): AliasExpr
```

**Example:**

```typescript
// In aggregation context
df.groupBy("category", [
  { name: "total", expr: col("amount").sum().alias("total_amount") }
])
```

### cast()

Cast to a different type.

```typescript
cast(targetDType: DTypeKind): CastExpr
```

**Example:**

```typescript
import { DTypeKind } from "molniya";

col("id").cast(DTypeKind.Int64)
col("price").cast(DTypeKind.Float64)
```

### toExpr()

Get the underlying expression (rarely needed directly).

```typescript
toExpr(): ColumnExpr
```

**Example:**

```typescript
const expr = col("id").toExpr();
```

## Chaining Examples

### Complex Filter

```typescript
df.filter(
  col("age")
    .gte(18)
    .and(col("age").lt(65))
    .and(col("active").eq(true))
)
```

### Calculated Column

```typescript
df.withColumn(
  "discounted_total",
  col("price")
    .mul(col("quantity"))
    .mul(col("discount"))
    .add(col("tax"))
)
```

### String Matching

```typescript
df.filter(
  col("email")
    .contains("@")
    .and(col("email").endsWith(".com"))
)
```

## Method Equivalents

ColumnRef methods have equivalent standalone functions:

```typescript
// These are equivalent:
col("a").add(col("b"))
add(col("a"), col("b"))

// These are equivalent:
col("a").gt(5)
gt(col("a"), 5)
```

Use whichever style you prefer. The ColumnRef methods are often more readable for chaining.
