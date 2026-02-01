# Column Operations API

API reference for adding, modifying, and transforming columns.

## withColumn()

Add a new column or replace an existing column.

```typescript
withColumn(name: string, expr: Expr): DataFrame<T & Record<K, V>>
```

**Parameters:**
- `name` - Name of the new or existing column
- `expr` - Expression defining the column values

**Returns:** DataFrame with the new/modified column

**Example:**

```typescript
import { col } from "Molniya";

// Add new column
df.withColumn("total", col("price").mul(col("quantity")))

// Replace existing column
df.withColumn("price", col("price").mul(1.1))  // 10% increase
```

## withColumns()

Add or replace multiple columns at once.

```typescript
withColumns(columns: Record<string, Expr> | Array<{name: string, expr: Expr}>): DataFrame<T>
```

**Parameters:**
- `columns` - Object or array of column definitions

**Example:**

```typescript
import { col } from "Molniya";

// Object syntax
df.withColumns({
  tax: col("amount").mul(0.08),
  total: col("amount").mul(1.08),
  discount: col("amount").mul(0.1)
})

// Array syntax
df.withColumns([
  { name: "tax", expr: col("amount").mul(0.08) },
  { name: "total", expr: col("amount").mul(1.08) },
  { name: "discount", expr: col("amount").mul(0.1) }
])
```

## cast()

Cast columns to different data types.

```typescript
cast(schema: Partial<Record<keyof T, DType>>): DataFrame<T>
```

**Parameters:**
- `schema` - Object mapping column names to data types

**Example:**

```typescript
import { DType } from "Molniya";

df.cast({
  id: DType.int32,
  amount: DType.float64,
  count: DType.int32
})
```

## fillNull()

Fill null values with a specified value.

```typescript
fillNull(value: Expr | unknown): ColumnRef
```

**Parameters:**
- `value` - Value to replace nulls with

**Example:**

```typescript
import { col } from "Molniya";

// Fill with constant
df.withColumn("discount", col("discount").fillNull(0))

// Fill with another column
df.withColumn("nickname", col("nickname").fillNull(col("name")))
```

## fillNulls()

Fill null values in multiple columns.

```typescript
fillNulls(values: Record<string, Expr | unknown>): DataFrame<T>
```

**Parameters:**
- `values` - Object mapping column names to fill values

**Example:**

```typescript
df.fillNulls({
  discount: 0,
  tax_rate: 0.08,
  status: "pending"
})
```

## dropNulls()

Remove rows with null values.

```typescript
dropNulls(how: "any" | "all" = "any", subset?: string[]): DataFrame<T>
```

**Parameters:**
- `how` - "any" (drop if any null) or "all" (drop only if all null)
- `subset` - Optional columns to check (default: all columns)

**Example:**

```typescript
// Drop rows with any null
df.dropNulls()

// Drop rows with all nulls
df.dropNulls("all")

// Drop only if specific columns are null
df.dropNulls("any", ["id", "name", "email"])
```

## Arithmetic Operations

### add()

Addition.

```typescript
add(other: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
col("a").add(col("b"))
col("value").add(10)
```

### sub()

Subtraction.

```typescript
sub(other: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
col("total").sub(col("tax"))
```

### mul()

Multiplication.

```typescript
mul(other: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
col("price").mul(col("quantity"))
```

### div()

Division.

```typescript
div(other: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
col("sum").div(col("count"))
```

### mod()

Modulo (remainder).

```typescript
mod(other: Expr | number): ArithmeticExpr
```

**Example:**

```typescript
col("value").mod(10)
```

### neg()

Negation.

```typescript
neg(): ArithmeticExpr
```

**Example:**

```typescript
col("value").neg()
```

## Math Functions

### round()

Round to specified decimal places.

```typescript
round(decimals: number): ArithmeticExpr
```

**Example:**

```typescript
col("price").round(2)  // 2 decimal places
```

### floor()

Round down to nearest integer.

```typescript
floor(): ArithmeticExpr
```

**Example:**

```typescript
col("price").floor()
```

### ceil()

Round up to nearest integer.

```typescript
ceil(): ArithmeticExpr
```

**Example:**

```typescript
col("price").ceil()
```

### abs()

Absolute value.

```typescript
abs(): ArithmeticExpr
```

**Example:**

```typescript
col("change").abs()
```

### sqrt()

Square root.

```typescript
sqrt(): ArithmeticExpr
```

**Example:**

```typescript
col("value").sqrt()
```

### pow()

Power/exponentiation.

```typescript
pow(exp: number): ArithmeticExpr
```

**Example:**

```typescript
col("value").pow(2)  // Square
col("value").pow(0.5)  // Square root
```

## Conditional Operations

### when()

Conditional expression builder.

```typescript
when(condition: Expr, value: Expr | unknown): WhenExpr
```

**Example:**

```typescript
import { when, col, lit } from "Molniya";

df.withColumn("grade",
  when(col("score").gte(90), "A")
    .when(col("score").gte(80), "B")
    .when(col("score").gte(70), "C")
    .otherwise("F")
)
```

### coalesce()

Return first non-null value.

```typescript
coalesce(...exprs: Expr[]): Expr
```

**Example:**

```typescript
import { coalesce, col } from "Molniya";

df.withColumn("contact",
  coalesce(col("email"), col("phone"), col("address"))
)
```

## String Operations

### concat()

Concatenate strings.

```typescript
concat(...exprs: Expr[]): StringExpr
```

**Example:**

```typescript
import { concat, col, lit } from "Molniya";

df.withColumn("full_name",
  concat(col("first"), lit(" "), col("last"))
)
```

### length()

String length.

```typescript
length(col: ColumnRef): Expr
```

**Example:**

```typescript
import { length, col } from "Molniya";

df.withColumn("name_length", length(col("name")))
```

### substring()

Extract substring.

```typescript
substring(start: number, length: number): StringExpr
```

**Example:**

```typescript
col("phone").substring(0, 3)  // Area code
```

### upper() / lower()

Case conversion.

```typescript
upper(): StringExpr
lower(): StringExpr
```

**Example:**

```typescript
col("code").upper()
col("email").lower()
```

## Date Operations

### year() / month() / day()

Extract date components.

```typescript
year(col: ColumnRef): Expr
month(col: ColumnRef): Expr
day(col: ColumnRef): Expr
```

**Example:**

```typescript
import { year, month, day, col } from "Molniya";

df.withColumn("year", year(col("date")))
  .withColumn("month", month(col("date")))
  .withColumn("day", day(col("date")))
```

### addDays() / subDays()

Date arithmetic.

```typescript
addDays(days: number): DateExpr
subDays(days: number): DateExpr
```

**Example:**

```typescript
col("date").addDays(7)
col("date").subDays(30)
```

### diffDays()

Difference between dates.

```typescript
diffDays(other: ColumnRef): Expr
```

**Example:**

```typescript
col("end").diffDays(col("start"))
```

## Type Casting

### cast()

Cast to different type.

```typescript
cast(dtype: DType): Expr
```

**Example:**

```typescript
import { DType } from "Molniya";

col("id").cast(DType.int32)
col("price").cast(DType.float64)
```

## Chaining Operations

Operations can be chained for complex transformations:

```typescript
df.withColumn("total",
  col("price")
    .mul(col("quantity"))     // price * quantity
    .mul(lit(1).sub(col("discount")))  // apply discount
    .mul(1.08)                // add tax
    .round(2)                 // round to 2 decimals
)
```
