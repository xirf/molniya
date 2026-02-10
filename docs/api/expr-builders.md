# Expression Builders

Expression builders create `Expr` objects used for filtering, calculations, and aggregations.

## Overview

```typescript
import { 
  col, lit,                    // References and literals
  add, sub, mul, div, mod, neg, // Arithmetic
  and, or, not,                // Logical
  sum, avg, min, max, count,   // Aggregations
  first, last                  // Window-like
} from "molniya";
```

## Column References

### col()

Create a column reference with fluent API.

```typescript
col(name: string): ColumnRef
```

**Example:**

```typescript
import { col } from "molniya";

// Simple reference
col("price")

// Method chaining
col("price").mul(1.1)
col("age").gte(18)
```

See [ColumnRef](./column-ref) for all available methods.

## Literals

### lit()

Create a literal value expression.

```typescript
lit(value: number | bigint | string | boolean | null): LiteralExpr
```

**Example:**

```typescript
import { lit, col } from "molniya";

// In arithmetic
df.withColumn("with_tax", col("price").mul(lit(1.1)))

// In comparisons
df.filter(col("status").eq(lit("active")))

// Note: Most operators accept raw values directly:
df.filter(col("status").eq("active"))  // Same as above
```

### typedLit()

Create a literal with explicit type hint.

```typescript
typedLit(value: number | bigint | string | boolean | null, dtype: DTypeKind): LiteralExpr
```

**Example:**

```typescript
import { typedLit, DTypeKind } from "molniya";

typedLit(42, DTypeKind.Int32)
typedLit("hello", DTypeKind.String)
```

## Arithmetic Functions

### add()

Add two expressions.

```typescript
add(left: Expr | ColumnRef | number, right: Expr | ColumnRef | number): ArithmeticExpr
```

**Example:**

```typescript
import { add, col } from "molniya";

add(col("a"), col("b"))
add(col("price"), col("tax"))
add(10, col("value"))  // Can mix with numbers

// Equivalent to:
col("a").add(col("b"))
```

### sub()

Subtract two expressions.

```typescript
sub(left: Expr | ColumnRef | number, right: Expr | ColumnRef | number): ArithmeticExpr
```

**Example:**

```typescript
import { sub, col } from "molniya";

sub(col("total"), col("discount"))

// Equivalent to:
col("total").sub(col("discount"))
```

### mul()

Multiply two expressions.

```typescript
mul(left: Expr | ColumnRef | number, right: Expr | ColumnRef | number): ArithmeticExpr
```

**Example:**

```typescript
import { mul, col } from "molniya";

mul(col("price"), col("quantity"))
mul(col("amount"), 0.1)  // Calculate 10%

// Equivalent to:
col("price").mul(col("quantity"))
```

### div()

Divide two expressions.

```typescript
div(left: Expr | ColumnRef | number, right: Expr | ColumnRef | number): ArithmeticExpr
```

**Example:**

```typescript
import { div, col } from "molniya";

div(col("total"), col("count"))

// Equivalent to:
col("total").div(col("count"))
```

::: warning Division by Zero
Dividing by zero will produce `Infinity` or `NaN` following JavaScript semantics.
:::

### mod()

Modulo (remainder) operation.

```typescript
mod(left: Expr | ColumnRef | number, right: Expr | ColumnRef | number): ArithmeticExpr
```

**Example:**

```typescript
import { mod, col } from "molniya";

mod(col("n"), 2)  // Check even/odd

// Equivalent to:
col("n").mod(2)
```

### neg()

Negate an expression.

```typescript
neg(expr: Expr | ColumnRef | number): NegExpr
```

**Example:**

```typescript
import { neg, col } from "molniya";

neg(col("debt"))

// Equivalent to:
col("debt").neg()
```

## Logical Functions

### and()

Logical AND of multiple expressions.

```typescript
and(...exprs: (Expr | ColumnRef)[]): LogicalExpr
```

**Example:**

```typescript
import { and, col } from "molniya";

and(
  col("age").gte(18),
  col("age").lt(65),
  col("active").eq(true)
)
```

### or()

Logical OR of multiple expressions.

```typescript
or(...exprs: (Expr | ColumnRef)[]): LogicalExpr
```

**Example:**

```typescript
import { or, col } from "molniya";

or(
  col("status").eq("pending"),
  col("status").eq("processing")
)
```

### not()

Logical NOT of an expression.

```typescript
not(expr: Expr | ColumnRef): NotExpr
```

**Example:**

```typescript
import { not, col } from "molniya";

not(col("deleted"))
```

## Aggregation Functions

These functions create aggregation expressions valid only in `groupBy()` contexts.

### sum()

Sum of values.

```typescript
sum(column: string | Expr | ColumnRef): AggExpr
```

**Example:**

```typescript
import { sum, col } from "molniya";

// Sum a column
sum("amount")

// Sum an expression
sum(col("price").mul(col("quantity")))
```

### avg()

Average (mean) of values.

```typescript
avg(column: string | Expr | ColumnRef): AggExpr
```

**Example:**

```typescript
import { avg } from "molniya";

avg("salary")
avg(col("score"))
```

### min()

Minimum value.

```typescript
min(column: string | Expr | ColumnRef): AggExpr
```

**Example:**

```typescript
import { min } from "molniya";

min("price")
min("created_at")
```

### max()

Maximum value.

```typescript
max(column: string | Expr | ColumnRef): AggExpr
```

**Example:**

```typescript
import { max } from "molniya";

max("price")
max("created_at")
```

### count()

Count rows or non-null values.

```typescript
count(column?: string | Expr | ColumnRef): CountExpr
```

**Example:**

```typescript
import { count } from "molniya";

count()           // Count all rows
count("email")    // Count non-null emails
count(col("id"))  // Count non-null ids
```

### first()

First value in group.

```typescript
first(column: string | Expr | ColumnRef): AggExpr
```

**Example:**

```typescript
import { first } from "molniya";

first("order_date")
```

### last()

Last value in group.

```typescript
last(column: string | Expr | ColumnRef): AggExpr
```

**Example:**

```typescript
import { last } from "molniya";

last("order_date")
```

## Sort Helpers

### asc()

Specify ascending sort order.

```typescript
asc(column: string): SortKey
```

**Example:**

```typescript
import { asc } from "molniya";

df.sort(asc("name"))  // Same as df.sort("name")
```

### desc()

Specify descending sort order.

```typescript
desc(column: string): SortKey
```

**Example:**

```typescript
import { desc } from "molniya";

df.sort(desc("amount"))  // Highest amounts first
```

## Null Handling

### coalesce()

Returns the first non-null value.

```typescript
coalesce(...exprs: (Expr | ColumnRef | number | string | boolean | null)[]): CoalesceExpr
```

**Example:**

```typescript
import { coalesce, col, lit } from "molniya";

// Use backup email if primary is null
coalesce(col("primary_email"), col("backup_email"), lit("no-email"))

// Fill null with default
df.withColumn("display_name", 
  coalesce(col("nickname"), col("full_name"), lit("Anonymous"))
)
```

## Expression Types

The builders return different expression types:

| Function | Return Type | Usage Context |
|----------|-------------|---------------|
| `col()` | `ColumnRef` | Any |
| `lit()` | `LiteralExpr` | Any |
| `add()`, `sub()`, etc. | `ArithmeticExpr` | Any |
| `and()`, `or()`, `not()` | `LogicalExpr` | Filtering |
| `sum()`, `avg()`, etc. | `AggExpr` | GroupBy only |
| `count()` | `CountExpr` | GroupBy only |
| `asc()`, `desc()` | `SortKey` | Sorting |

All expression types extend the base `Expr` type and can be used wherever an expression is expected.
