# Sorting & Limiting API

API reference for ordering and limiting DataFrame rows.

## sort()

Sort DataFrame by one or more columns.

```typescript
sort(...exprs: SortExpr[]): DataFrame<T>
sort(exprs: SortExpr[]): DataFrame<T>
```

**Parameters:**
- `exprs` - Sort expressions (created with `asc()` or `desc()`)

**Returns:** Sorted DataFrame

**Example:**

```typescript
import { asc, desc } from "molniya";

// Single column
df.sort(asc("name"))
df.sort(desc("price"))

// Multiple columns
df.sort(asc("category"), desc("price"))

// Array syntax
df.sort([asc("year"), asc("month"), desc("amount")])
```

## asc()

Create ascending sort expression.

```typescript
asc(column: string | ColumnRef): SortExpr
```

**Parameters:**
- `column` - Column name or reference to sort by

**Returns:** Sort expression for ascending order

**Example:**

```typescript
import { asc, col } from "molniya";

asc("name")
asc(col("created_at"))
```

### Null Handling

```typescript
// Nulls first (default)
asc("middle_name")

// Nulls last
asc("middle_name").nullsLast()
```

## desc()

Create descending sort expression.

```typescript
desc(column: string | ColumnRef): SortExpr
```

**Parameters:**
- `column` - Column name or reference to sort by

**Returns:** Sort expression for descending order

**Example:**

```typescript
import { desc, col } from "molniya";

desc("price")
desc(col("score"))
```

### Null Handling

```typescript
// Nulls first (default)
desc("rating")

// Nulls last
desc("rating").nullsLast()
```

## limit()

Limit the number of rows returned.

```typescript
limit(n: number): DataFrame<T>
```

**Parameters:**
- `n` - Maximum number of rows to return

**Returns:** DataFrame with at most n rows

**Example:**

```typescript
df.limit(100)   // First 100 rows
df.limit(10)    // First 10 rows
df.limit(1)     // First row only
```

## offset()

Skip a number of rows before returning results.

```typescript
offset(n: number): DataFrame<T>
```

**Parameters:**
- `n` - Number of rows to skip

**Returns:** DataFrame with first n rows skipped

**Example:**

```typescript
df.offset(10).limit(10)   // Rows 10-19 (page 2)
df.offset(20).limit(10)   // Rows 20-29 (page 3)
```

## head()

Return first n rows (convenience method).

```typescript
head(n: number = 5): DataFrame<T>
```

**Parameters:**
- `n` - Number of rows to return (default: 5)

**Returns:** DataFrame with first n rows

**Example:**

```typescript
df.head()       // First 5 rows
df.head(10)     // First 10 rows
df.head(1)      // First row
```

## tail()

Return last n rows.

```typescript
tail(n: number = 5): DataFrame<T>
```

**Parameters:**
- `n` - Number of rows to return (default: 5)

**Returns:** DataFrame with last n rows

**Example:**

```typescript
df.tail()       // Last 5 rows
df.tail(10)     // Last 10 rows
```

## distinct()

Return distinct/unique rows.

```typescript
distinct(): DataFrame<T>
distinct(...columns: string[]): DataFrame<T>
```

**Parameters:**
- `columns` - Optional columns to consider for uniqueness

**Returns:** DataFrame with duplicate rows removed

**Example:**

```typescript
// All distinct rows
df.distinct()

// Distinct by specific columns
df.distinct("user_id", "email")
```

## dropDuplicates()

Alias for `distinct()`.

```typescript
dropDuplicates(): DataFrame<T>
dropDuplicates(...columns: string[]): DataFrame<T>
```

## Common Patterns

### Top N

```typescript
import { desc } from "molniya";

// Top 10 by score
const top10 = df.sort(desc("score")).limit(10);

// Top 5 most expensive
const mostExpensive = df.sort(desc("price")).limit(5);
```

### Bottom N

```typescript
import { asc } from "molniya";

// Bottom 10 by score
const bottom10 = df.sort(asc("score")).limit(10);

// 5 cheapest
const cheapest = df.sort(asc("price")).limit(5);
```

### Pagination

```typescript
const pageSize = 20;
const pageNum = 2;  // 0-indexed

const page = df
  .sort(asc("name"))
  .offset(pageNum * pageSize)
  .limit(pageSize);
```

### Multi-Level Sort

```typescript
import { asc, desc } from "molniya";

// Sort by category, then by price descending within category
df.sort(asc("category"), desc("price"));

// Sort by year descending, then month ascending
df.sort(desc("year"), asc("month"));
```

### Sort with Computed Column

```typescript
import { col, desc } from "molniya";

// Sort by total (price * quantity)
df.withColumn("total", col("price").mul(col("quantity")))
  .sort(desc("total"))
```

## Performance Notes

- Sorting requires materializing data (breaks streaming)
- `limit()` without `sort()` returns arbitrary rows
- Use `distinct()` early to reduce data volume
- Sorting is stable (preserves original order for equal keys)

## Type Safety

Sorting preserves DataFrame type:

```typescript
interface Product {
  id: number;
  name: string;
  price: number;
}

const df: DataFrame<Product> = ...;
const sorted = df.sort(asc("price"));
// sorted is still DataFrame<Product>
```
