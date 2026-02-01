# Filtering API

API reference for filtering DataFrame rows based on conditions.

## filter()

Filter rows based on a boolean expression.

```typescript
filter(expr: Expr): DataFrame<T>
```

**Parameters:**
- `expr` - Boolean expression determining which rows to keep

**Returns:** DataFrame containing only rows where expression is true

**Example:**

```typescript
import { col } from "Molniya";

df.filter(col("age").gte(18))
df.filter(col("status").eq("active"))
```

## where()

Alias for `filter()`.

```typescript
where(expr: Expr): DataFrame<T>
```

**Example:**

```typescript
df.where(col("score").gt(90))
```

## Comparison Operators

### eq()

Equal comparison.

```typescript
eq(other: Expr | number | string | boolean | null): ComparisonExpr
```

**Example:**

```typescript
col("status").eq("active")
col("id").eq(col("ref_id"))
```

### neq()

Not equal comparison.

```typescript
neq(other: Expr | number | string | boolean | null): ComparisonExpr
```

**Example:**

```typescript
col("type").neq("deleted")
```

### gt()

Greater than.

```typescript
gt(other: Expr | number): ComparisonExpr
```

**Example:**

```typescript
col("age").gt(18)
col("price").gt(col("cost"))
```

### gte()

Greater than or equal.

```typescript
gte(other: Expr | number): ComparisonExpr
```

**Example:**

```typescript
col("score").gte(60)
```

### lt()

Less than.

```typescript
lt(other: Expr | number): ComparisonExpr
```

**Example:**

```typescript
col("price").lt(100)
```

### lte()

Less than or equal.

```typescript
lte(other: Expr | number): ComparisonExpr
```

**Example:**

```typescript
col("quantity").lte(10)
```

## Logical Operators

### and()

Logical AND - all conditions must be true.

```typescript
and(...exprs: Expr[]): LogicalExpr
```

**Example:**

```typescript
import { and, col } from "Molniya";

and(
  col("age").gte(18),
  col("status").eq("active")
)
```

### or()

Logical OR - any condition can be true.

```typescript
or(...exprs: Expr[]): LogicalExpr
```

**Example:**

```typescript
import { or, col } from "Molniya";

or(
  col("category").eq("electronics"),
  col("category").eq("computers")
)
```

### not()

Logical NOT - negate a condition.

```typescript
not(expr: Expr): LogicalExpr
```

**Example:**

```typescript
import { not, col } from "Molniya";

not(col("deleted").eq(true))
```

## Null Checks

### isNull()

Check if value is null.

```typescript
isNull(): ComparisonExpr
```

**Example:**

```typescript
col("email").isNull()
```

### isNotNull()

Check if value is not null.

```typescript
isNotNull(): ComparisonExpr
```

**Example:**

```typescript
col("email").isNotNull()
```

## String Filters

### contains()

Check if string contains substring.

```typescript
contains(substring: string): ComparisonExpr
```

**Example:**

```typescript
col("email").contains("@company.com")
```

### startsWith()

Check if string starts with prefix.

```typescript
startsWith(prefix: string): ComparisonExpr
```

**Example:**

```typescript
col("phone").startsWith("+1")
```

### endsWith()

Check if string ends with suffix.

```typescript
endsWith(suffix: string): ComparisonExpr
```

**Example:**

```typescript
col("file").endsWith(".csv")
```

### like()

SQL-style pattern matching.

```typescript
like(pattern: string): ComparisonExpr
```

**Patterns:**
- `%` - matches any sequence of characters
- `_` - matches any single character

**Example:**

```typescript
col("name").like("John%")     // Starts with John
col("code").like("US-___")    // US- followed by 3 chars
```

### regexpMatch()

Regular expression matching.

```typescript
regexpMatch(pattern: string): ComparisonExpr
```

**Example:**

```typescript
col("email").regexpMatch("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
```

## Date/Time Filters

### Date Comparisons

```typescript
import { col } from "Molniya";

// Compare dates
col("order_date").gt(new Date("2024-01-01"))
col("created_at").gte(Date.now() - 86400000)  // Last 24 hours
```

### Year/Month/Day Extraction

```typescript
import { col, year, month, day } from "Molniya";

// Filter by date component
year(col("date")).eq(2024)
month(col("date")).eq(1)     // January
day(col("date")).eq(15)      // 15th of month
```

## Between Filters

### between()

Check if value is within range (inclusive).

```typescript
between(lower: Expr | number, upper: Expr | number): ComparisonExpr
```

**Example:**

```typescript
col("age").between(18, 65)
col("price").between(col("min_price"), col("max_price"))
```

## In Filters

### isIn()

Check if value is in a list.

```typescript
isIn(values: (string | number | boolean)[]): ComparisonExpr
```

**Example:**

```typescript
col("status").isIn(["active", "pending", "verified"])
col("category").isIn(["electronics", "computers", "phones"])
```

## Filter Chaining

Multiple filters can be chained:

```typescript
df.filter(col("age").gte(18))
  .filter(col("status").eq("active"))
  .filter(col("country").eq("US"))
```

Equivalent to:

```typescript
import { and, col } from "Molniya";

df.filter(and(
  col("age").gte(18),
  col("status").eq("active"),
  col("country").eq("US")
))
```

## Complex Filter Examples

### Multi-Condition Filter

```typescript
import { and, or, col } from "Molniya";

df.filter(and(
  or(
    col("category").eq("electronics"),
    col("category").eq("computers")
  ),
  col("price").gte(100),
  col("in_stock").eq(true),
  col("deleted_at").isNull()
))
```

### Date Range Filter

```typescript
import { and, col } from "Molniya";

const startDate = new Date("2024-01-01");
const endDate = new Date("2024-12-31");

df.filter(and(
  col("order_date").gte(startDate),
  col("order_date").lte(endDate)
))
```

### Search Filter

```typescript
import { or, col } from "Molniya";

const searchTerm = "laptop";

df.filter(or(
  col("name").lower().contains(searchTerm),
  col("description").lower().contains(searchTerm),
  col("tags").lower().contains(searchTerm)
))
```

## Performance Notes

- Filters are pushed down to data sources when possible
- Multiple filters are combined with AND automatically
- Filter before expensive operations like joins
- String comparisons use dictionary encoding for efficiency
