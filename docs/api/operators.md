# Operators API

API reference for low-level operators and expression builders.

## Expression Builders

### col()

Create a column reference.

```typescript
col(name: string): ColumnRef
```

**Parameters:**
- `name` - Column name

**Returns:** ColumnRef for building expressions

**Example:**

```typescript
import { col } from "molniya";

col("price")
col("customer.name")  // Nested column reference
```

### lit()

Create a literal value expression.

```typescript
lit(value: number | bigint | string | boolean | null | Date): LiteralExpr
```

**Parameters:**
- `value` - Literal value

**Returns:** Literal expression

**Example:**

```typescript
import { lit } from "molniya";

lit(100)
lit("active")
lit(true)
lit(null)
lit(123456789n)
lit(new Date("2024-01-01"))
```

## Arithmetic Operators

### add()

Addition operator (function form).

```typescript
add(left: Expr, right: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
import { add, col } from "molniya";

add(col("a"), col("b"))
add(col("value"), 10)
```

### sub()

Subtraction operator (function form).

```typescript
sub(left: Expr, right: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
import { sub, col } from "molniya";

sub(col("total"), col("tax"))
```

### mul()

Multiplication operator (function form).

```typescript
mul(left: Expr, right: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
import { mul, col } from "molniya";

mul(col("price"), col("quantity"))
```

### div()

Division operator (function form).

```typescript
div(left: Expr, right: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
import { div, col } from "molniya";

div(col("sum"), col("count"))
```

### mod()

Modulo operator (function form).

```typescript
mod(left: Expr, right: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
import { mod, col } from "molniya";

mod(col("value"), 10)
```

### neg()

Negation operator (function form).

```typescript
neg(expr: Expr): ArithmeticExpr
```

**Example:**

```typescript
import { neg, col } from "molniya";

neg(col("value"))
```

## Logical Operators

### and()

Logical AND.

```typescript
and(...exprs: Expr[]): LogicalExpr
```

**Example:**

```typescript
import { and, col } from "molniya";

and(
  col("age").gte(18),
  col("status").eq("active")
)
```

### or()

Logical OR.

```typescript
or(...exprs: Expr[]): LogicalExpr
```

**Example:**

```typescript
import { or, col } from "molniya";

or(
  col("category").eq("electronics"),
  col("category").eq("computers")
)
```

### not()

Logical NOT.

```typescript
not(expr: Expr): LogicalExpr
```

**Example:**

```typescript
import { not, col } from "molniya";

not(col("deleted").eq(true))
```

## Comparison Operators

### eq()

Equal comparison (function form).

```typescript
eq(left: Expr, right: Expr | unknown): ComparisonExpr
```

**Example:**

```typescript
import { eq, col } from "molniya";

eq(col("status"), "active")
```

### neq()

Not equal comparison (function form).

```typescript
neq(left: Expr, right: Expr | unknown): ComparisonExpr
```

**Example:**

```typescript
import { neq, col } from "molniya";

neq(col("type"), "deleted")
```

### gt()

Greater than (function form).

```typescript
gt(left: Expr, right: Expr | number): ComparisonExpr
```

**Example:**

```typescript
import { gt, col } from "molniya";

gt(col("age"), 18)
```

### gte()

Greater than or equal (function form).

```typescript
gte(left: Expr, right: Expr | number): ComparisonExpr
```

**Example:**

```typescript
import { gte, col } from "molniya";

gte(col("score"), 60)
```

### lt()

Less than (function form).

```typescript
lt(left: Expr, right: Expr | number): ComparisonExpr
```

**Example:**

```typescript
import { lt, col } from "molniya";

lt(col("price"), 100)
```

### lte()

Less than or equal (function form).

```typescript
lte(left: Expr, right: Expr | number): ComparisonExpr
```

**Example:**

```typescript
import { lte, col } from "molniya";

lte(col("quantity"), 10)
```

## String Operators

### concat()

Concatenate strings.

```typescript
concat(...exprs: Expr[]): StringExpr
```

**Example:**

```typescript
import { concat, col, lit } from "molniya";

concat(col("first"), lit(" "), col("last"))
```

### length()

String length.

```typescript
length(expr: Expr): Expr
```

**Example:**

```typescript
import { length, col } from "molniya";

length(col("name"))
```

### substring()

Extract substring (function form).

```typescript
substring(expr: Expr, start: number, len: number): StringExpr
```

**Example:**

```typescript
import { substring, col } from "molniya";

substring(col("phone"), 0, 3)
```

### upper()

Convert to uppercase (function form).

```typescript
upper(expr: Expr): StringExpr
```

**Example:**

```typescript
import { upper, col } from "molniya";

upper(col("code"))
```

### lower()

Convert to lowercase (function form).

```typescript
lower(expr: Expr): StringExpr
```

**Example:**

```typescript
import { lower, col } from "molniya";

lower(col("email"))
```

## Date Operators

### year()

Extract year from date.

```typescript
year(expr: Expr): Expr
```

**Example:**

```typescript
import { year, col } from "molniya";

year(col("date"))
```

### month()

Extract month from date.

```typescript
month(expr: Expr): Expr
```

**Example:**

```typescript
import { month, col } from "molniya";

month(col("date"))
```

### day()

Extract day from date.

```typescript
day(expr: Expr): Expr
```

**Example:**

```typescript
import { day, col } from "molniya";

day(col("date"))
```

### toDate()

Parse string to date.

```typescript
toDate(expr: Expr, format: string): Expr
```

**Example:**

```typescript
import { toDate, col } from "molniya";

toDate(col("date_str"), "YYYY-MM-DD")
```

### toTimestamp()

Parse string to timestamp.

```typescript
toTimestamp(expr: Expr, format: string): Expr
```

**Example:**

```typescript
import { toTimestamp, col } from "molniya";

toTimestamp(col("ts_str"), "YYYY-MM-DD HH:mm:ss")
```

## Conditional Operators

### when()

Conditional expression builder.

```typescript
when(condition: Expr, value: Expr | unknown): WhenExpr
```

**Example:**

```typescript
import { when, col, lit } from "molniya";

when(col("score").gte(90), "A")
  .when(col("score").gte(80), "B")
  .otherwise("F")
```

### coalesce()

Return first non-null value.

```typescript
coalesce(...exprs: Expr[]): Expr
```

**Example:**

```typescript
import { coalesce, col } from "molniya";

coalesce(col("email"), col("phone"), col("address"))
```

## Type Casting

### cast()

Cast expression to type (function form).

```typescript
cast(expr: Expr, dtype: DType): Expr
```

**Example:**

```typescript
import { cast, col, DType } from "molniya";

cast(col("id"), DType.int32)
```

## Operator Precedence

When mixing operators, Molniya follows standard precedence:

1. Parentheses `()`
2. Function calls
3. Multiplication, Division, Modulo `* / %`
4. Addition, Subtraction `+ -`
5. Comparison `== != < > <= >=`
6. Logical NOT `not`
7. Logical AND `and`
8. Logical OR `or`

Use parentheses to override precedence:

```typescript
// Without parentheses: a + (b * c)
col("a").add(col("b").mul(col("c")))

// With parentheses: (a + b) * c
col("a").add(col("b")).mul(col("c"))
```
