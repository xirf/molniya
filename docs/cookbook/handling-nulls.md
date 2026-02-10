# Handling Nulls

Recipes for detecting, filtering, and replacing null (missing) values in your data.

## Detecting Nulls

### Find Null Values

```typescript
import { col } from "molniya";

// Rows where email is null
const missingEmail = df.filter(col("email").isNull());

// Rows where any of several columns are null
import { or } from "molniya";
const missingData = df.filter(or(
  col("email").isNull(),
  col("phone").isNull()
));
```

### Find Non-Null Values

```typescript
import { col } from "molniya";

// Rows where email is not null
const hasEmail = df.filter(col("email").isNotNull());

// All required fields present
import { and } from "molniya";
const completeRecords = df.filter(and(
  col("name").isNotNull(),
  col("email").isNotNull(),
  col("phone").isNotNull()
));
```

### Count Nulls

```typescript
import { col, sum, count, when, lit } from "molniya";

// Count nulls in a column
const nullCount = await df.agg(
  sum(when(col("email").isNull(), lit(1)).otherwise(lit(0)))
);

// Count nulls per column
const nullCounts = await df.agg([
  sum(when(col("name").isNull(), lit(1)).otherwise(lit(0))).as("name_nulls"),
  sum(when(col("email").isNull(), lit(1)).otherwise(lit(0))).as("email_nulls"),
  sum(when(col("phone").isNull(), lit(1)).otherwise(lit(0))).as("phone_nulls")
]);
```

## Filtering Nulls

### Remove Rows with Nulls

```typescript
import { col } from "molniya";

// Remove rows where specific column is null
df.filter(col("email").isNotNull())

// Remove rows where any column is null
df.dropNulls()

// Remove rows where all columns are null
df.dropNulls("all")
```

### Keep Only Complete Records

```typescript
import { col, and } from "molniya";

// Keep rows with all required fields
df.filter(and(
  col("id").isNotNull(),
  col("name").isNotNull(),
  col("amount").isNotNull()
));
```

## Filling Nulls

### Fill with a Constant

```typescript
import { col } from "molniya";

// Replace nulls with 0
df.withColumn("discount", col("discount").fillNull(0))

// Replace nulls with empty string
df.withColumn("middle_name", col("middle_name").fillNull(""))

// Replace nulls with default value
df.withColumn("status", col("status").fillNull("pending"))
```

### Fill with Another Column

```typescript
import { col } from "molniya";

// Use nickname if available, otherwise use name
df.withColumn("display_name", 
  col("nickname").fillNull(col("name"))
)

// Use shipping address if billing is null
df.withColumn("billing_address",
  col("billing_address").fillNull(col("shipping_address"))
)
```

### Fill Multiple Columns

```typescript
import { col } from "molniya";

// Fill multiple columns at once
df.withColumns({
  discount: col("discount").fillNull(0),
  tax_rate: col("tax_rate").fillNull(0.08),
  status: col("status").fillNull("active")
})
```

## Conditional Filling

### Fill Based on Condition

```typescript
import { col, when, lit } from "molniya";

// Fill null discount with 0 for active orders only
df.withColumn("discount",
  when(
    col("status").eq("active").and(col("discount").isNull()),
    lit(0)
  ).otherwise(col("discount"))
)
```

### Fill with Calculated Value

```typescript
import { col, avg } from "molniya";

// Calculate average, then fill nulls with it
const avgValue = await df.agg(avg("score"));

df.withColumn("score_filled", 
  col("score").fillNull(avgValue)
)
```

## Forward and Backward Fill

### Forward Fill (LOCF)

Last Observation Carried Forward - fill nulls with previous non-null value:

```typescript
import { col } from "molniya";

// Fill nulls with previous value
df.withColumn("temperature", col("temperature").forwardFill())
```

### Backward Fill

Fill nulls with next non-null value:

```typescript
import { col } from "molniya";

// Fill nulls with next value
df.withColumn("temperature", col("temperature").backwardFill())
```

### Fill with Group Context

```typescript
import { col } from "molniya";

// Forward fill within each group
df.sort(asc("date"))
  .groupBy("sensor_id")
  .withColumn("reading", col("reading").forwardFill())
```

## Null Handling in Aggregations

### Ignore Nulls

Most aggregation functions ignore nulls by default:

```typescript
import { sum, avg, count } from "molniya";

// Sum ignores nulls automatically
df.groupBy("category", [
  { name: "total", expr: sum("amount") },      // nulls ignored
  { name: "average", expr: avg("score") },     // nulls ignored
  { name: "count", expr: count() }             // counts all rows
])
```

### Count Non-Nulls

```typescript
import { count } from "molniya";

// Count only non-null values
df.groupBy("category", [
  { name: "total_rows", expr: count() },
  { name: "with_email", expr: count("email") }  // only non-null emails
])
```

### Treat Null as Zero

```typescript
import { col, sum } from "molniya";

// Fill nulls with 0 before summing
df.groupBy("category", [
  { name: "total", expr: sum(col("amount").fillNull(0)) }
])
```

## Schema Design for Nulls

### Define Nullable Columns

```typescript
import { DType } from "molniya";

const schema = {
  // Required columns (cannot be null)
  id: DType.int32,
  name: DType.string,
  
  // Optional columns (can be null)
  middle_name: DType.nullable.string,
  email: DType.nullable.string,
  phone: DType.nullable.string,
  deleted_at: DType.nullable.timestamp
};
```

### Convert Nullable to Non-Nullable

```typescript
import { col, DType } from "molniya";

// Fill nulls, then cast to non-nullable
df.withColumn("email", col("email").fillNull("unknown"))
  .cast({ email: DType.string })  // Now non-nullable
```

## Common Null Handling Patterns

### Data Cleaning Pipeline

```typescript
import { col, and } from "molniya";

const cleaned = df
  // Fill defaults
  .withColumn("discount", col("discount").fillNull(0))
  .withColumn("tax_rate", col("tax_rate").fillNull(0.08))
  
  // Remove incomplete records
  .filter(and(
    col("id").isNotNull(),
    col("name").isNotNull()
  ))
  
  // Use fallback values
  .withColumn("display_name", 
    col("nickname").fillNull(col("name"))
  );
```

### Coalesce Multiple Columns

```typescript
import { col, coalesce } from "molniya";

// Use first non-null value from multiple columns
df.withColumn("contact",
  coalesce(col("email"), col("phone"), col("address"))
)
```

### Null Indicator Column

```typescript
import { col, when, lit } from "molniya";

// Add flag column for missing data
df.withColumn("has_email", 
  when(col("email").isNull(), lit(false)).otherwise(lit(true))
)
```

## Best Practices

1. **Define nullable types explicitly**: Use `DType.nullable.*` for optional columns
2. **Handle nulls early**: Clean data at ingestion to avoid null checks later
3. **Document null semantics**: Be clear about what null means in your data
4. **Use appropriate defaults**: Choose meaningful default values
5. **Consider business logic**: Sometimes null has meaning (e.g., never contacted)

## Complete Example

```typescript
import { 
  fromRecords, 
  DType, 
  col, 
  and, 
  when, 
  lit,
  sum,
  avg 
} from "molniya";

const data = [
  { id: 1, name: "Alice", email: "alice@example.com", score: 95 },
  { id: 2, name: "Bob", email: null, score: 87 },
  { id: 3, name: "Charlie", email: "charlie@example.com", score: null },
  { id: 4, name: null, email: "dave@example.com", score: 78 }
];

const schema = {
  id: DType.int32,
  name: DType.nullable.string,
  email: DType.nullable.string,
  score: DType.nullable.int32
};

const df = fromRecords(data, schema);

// Clean the data
const cleaned = df
  // Fill missing names with "Unknown"
  .withColumn("name", col("name").fillNull("Unknown"))
  
  // Fill missing scores with average
  .withColumn("score", col("score").fillNull(
    df.agg(avg("score"))
  ))
  
  // Add email status flag
  .withColumn("has_email", 
    when(col("email").isNull(), lit(false)).otherwise(lit(true))
  )
  
  // Keep only records with valid IDs
  .filter(col("id").isNotNull());

await cleaned.show();
```
