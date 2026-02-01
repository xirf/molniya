# Transforming Columns

Add, modify, and compute new columns from existing data.

## Adding Columns

### withColumn()

Add a single computed column.

```typescript
import { col } from "Molniya";

// Simple calculation
df.withColumn("total", col("price").mul(col("quantity")))

// String concatenation
df.withColumn("full_name", col("first").add(" ").add(col("last")))

// Conditional value
df.withColumn("is_premium", col("price").gt(1000))
```

### withColumns()

Add multiple columns at once.

```typescript
// Array syntax
df.withColumns([
  { name: "tax", expr: col("amount").mul(0.1) },
  { name: "total", expr: col("amount").add(col("tax")) }
])

// Object syntax (same result)
df.withColumns({
  tax: col("amount").mul(0.1),
  total: col("amount").add(col("tax"))
})
```

## Arithmetic Transformations

### Basic Math

```typescript
import { col, add, sub, mul, div } from "Molniya";

// Addition
df.withColumn("total", col("a").add(col("b")))
df.withColumn("total", add(col("a"), col("b")))  // Equivalent

// Subtraction
df.withColumn("profit", col("revenue").sub(col("cost")))

// Multiplication
df.withColumn("discounted", col("price").mul(0.9))

// Division
df.withColumn("avg_price", col("total").div(col("count")))

// Modulo
df.withColumn("remainder", col("n").mod(2))

// Negation
df.withColumn("negative", col("value").neg())
```

### Chained Calculations

```typescript
// Complex formula
df.withColumn(
  "final_price",
  col("base_price")
    .mul(col("quantity"))
    .mul(col("discount").sub(1).neg())  // (1 - discount)
    .add(col("tax"))
)
```

## Conditional Columns

### Using Filter + Union (Workaround)

Molniya doesn't have a direct `when/then/otherwise`, but you can achieve similar results:

```typescript
import { col, lit } from "Molniya";

// Create separate DataFrames for each condition
const premium = df
  .filter(col("amount").gte(1000))
  .withColumn("tier", lit("premium"));

const standard = df
  .filter(col("amount").lt(1000))
  .filter(col("amount").gte(100))
  .withColumn("tier", lit("standard"));

const basic = df
  .filter(col("amount").lt(100))
  .withColumn("tier", lit("basic"));

// Concatenate results
const withTier = await premium.concat(standard).concat(basic);
```

### Boolean Columns

```typescript
// Create boolean flag columns
df.withColumn("is_adult", col("age").gte(18))
df.withColumn("is_active", col("status").eq("active"))
df.withColumn("has_email", col("email").isNotNull())
```

## String Transformations

### Concatenation

```typescript
import { col, lit } from "Molniya";

// Simple concatenation
df.withColumn("display", col("first").add(" ").add(col("last")))

// With literal
df.withColumn("greeting", lit("Hello, ").add(col("name")))
```

::: warning String Operations
Currently, string manipulation is limited. Advanced string operations like `substring`, `uppercase`, `replace` are available as DataFrame methods rather than expression methods.
:::

### Using String Methods

```typescript
// These are DataFrame methods, not expression methods
await df.trim("name");      // Trim whitespace
await df.replace("status", "old", "new");  // Replace text
```

## Date/Time Transformations

### Working with Timestamps

```typescript
import { col, lit } from "Molniya";

// Timestamps are milliseconds since epoch (bigint)
const oneDay = 24n * 60n * 60n * 1000n;

// Add a day
df.withColumn("tomorrow", col("date").add(lit(oneDay)))

// Calculate difference (returns bigint milliseconds)
df.withColumn("age_ms", col("now").sub(col("created_at")))
```

### Extracting Date Parts

Currently requires post-processing in JavaScript:

```typescript
const rows = await df.select("timestamp").toArray();

const withDateParts = rows.map(row => ({
  ...row,
  date: new Date(Number(row.timestamp)),
  year: new Date(Number(row.timestamp)).getFullYear(),
  month: new Date(Number(row.timestamp)).getMonth() + 1
}));
```

## Type Conversions

### Casting

```typescript
import { DType, col } from "Molniya";

// Cast to float for division
df.withColumn("ratio", col("part").cast(DType.float64).div(col("total")))

// Cast to int (truncates decimals)
df.withColumn("whole", col("amount").cast(DType.int32))

// Cast to string
df.withColumn("id_str", col("id").cast(DType.string))
```

::: warning Data Loss
Casting from larger to smaller types may overflow. Casting float to int truncates (rounds toward zero).
:::

## Null Handling in Transformations

### Coalesce

Use the first non-null value:

```typescript
import { coalesce, col, lit } from "Molniya";

// Use backup if primary is null
df.withColumn(
  "display_name",
  coalesce(col("nickname"), col("full_name"), lit("Anonymous"))
)
```

### Null Checks

```typescript
// Create flag for null values
df.withColumn("missing_email", col("email").isNull())

// Filter out nulls before calculation
const validOnly = df.filter(col("value").isNotNull());
```

## Practical Examples

### E-commerce Calculations

```typescript
const enriched = df
  .withColumn("subtotal", col("price").mul(col("quantity")))
  .withColumn("tax", col("subtotal").mul(0.08))
  .withColumn("shipping", 
    col("subtotal").gte(100).mul(0).add(
      col("subtotal").lt(100).mul(9.99)
    )
  )
  .withColumn("total", 
    col("subtotal").add(col("tax")).add(col("shipping"))
  );
```

### Normalization

```typescript
// Calculate min/max first
const minVal = await df.min("value");
const maxVal = await df.max("value");

// Then normalize (requires collecting and recreating)
const normalized = await df.withColumn(
  "normalized",
  col("value").sub(lit(minVal)).div(lit(maxVal - minVal))
).toArray();
```

### Bucketing

```typescript
// Create price buckets using filter approach
const low = df
  .filter(col("price").lt(50))
  .withColumn("price_tier", lit("low"));

const medium = df
  .filter(col("price").gte(50))
  .filter(col("price").lt(200))
  .withColumn("price_tier", lit("medium"));

const high = df
  .filter(col("price").gte(200))
  .withColumn("price_tier", lit("high"));

const withTier = await low.concat(medium).concat(high);
```

## Performance Tips

1. **Combine operations** - Use `withColumns()` instead of multiple `withColumn()` calls
2. **Filter first** - Apply filters before adding computed columns when possible
3. **Avoid redundant calculations** - Store intermediate results if used multiple times

```typescript
// Good: Single pass with multiple columns
df.withColumns({
  tax: col("amount").mul(0.1),
  discount: col("amount").mul(0.05),
  total: col("amount").mul(1.05)  // Could reference tax + discount
})

// Less efficient: Multiple passes
df
  .withColumn("tax", col("amount").mul(0.1))
  .withColumn("discount", col("amount").mul(0.05))
  .withColumn("total", col("amount").mul(1.05))
```
