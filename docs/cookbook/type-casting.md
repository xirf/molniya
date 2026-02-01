# Type Casting

Recipes for converting data between different types in Molniya.

## Basic Casting

### Cast to Different Type

```typescript
import { col, cast, DType } from "Molniya";

// Cast to integer
df.withColumn("id_int", cast(col("id"), DType.int32))

// Cast to float
df.withColumn("price_float", cast(col("price"), DType.float64))

// Cast to string
df.withColumn("id_string", cast(col("id"), DType.string))
```

### Cast Multiple Columns

```typescript
import { col, cast, DType } from "Molniya";

// Cast multiple columns at once
df.withColumns({
  id: cast(col("id"), DType.int32),
  amount: cast(col("amount"), DType.float64),
  code: cast(col("code"), DType.string)
})
```

## Numeric Type Conversions

### Integer to Float

```typescript
import { col, cast, DType } from "Molniya";

// Integer division vs float division
df.withColumn("ratio_int", col("a").div(col("b")))           // Integer division
df.withColumn("ratio_float", cast(col("a"), DType.float64).div(col("b")))  // Float division
```

### Float to Integer

```typescript
import { col, cast, DType } from "Molniya";

// Truncate decimal (simple cast)
df.withColumn("price_truncated", cast(col("price"), DType.int32))

// Round then cast
df.withColumn("price_rounded", cast(col("price").round(), DType.int32))

// Floor
df.withColumn("price_floor", cast(col("price").floor(), DType.int32))

// Ceiling
df.withColumn("price_ceil", cast(col("price").ceil(), DType.int32))
```

### Between Integer Sizes

```typescript
import { col, cast, DType } from "Molniya";

// Downcast to save memory
df.withColumn("small_count", cast(col("count"), DType.int16))
  .withColumn("tiny_flag", cast(col("flag"), DType.int8))

// Upcast for calculations
df.withColumn("big_sum", cast(col("amount"), DType.int64))
```

## String Conversions

### String to Number

```typescript
import { col, cast, DType } from "Molniya";

// Parse integer from string
df.withColumn("id_parsed", cast(col("id_str"), DType.int32))

// Parse float from string
df.withColumn("price_parsed", cast(col("price_str"), DType.float64))

// Handle parsing errors with null
df.withColumn("amount_clean", 
  when(regexpMatch(col("amount_str"), "^[0-9.]+$"), 
    cast(col("amount_str"), DType.float64)
  ).otherwise(lit(null))
)
```

### Number to String

```typescript
import { col, cast, DType } from "Molniya";

// Convert number to string
df.withColumn("id_str", cast(col("id"), DType.string))

// Format with padding
df.withColumn("code", 
  lpad(cast(col("id"), DType.string), 6, "0")
)
```

### String to Boolean

```typescript
import { col, when, lit } from "Molniya";

// Parse yes/no, true/false, 1/0
df.withColumn("is_active",
  when(lower(col("active_str")).eq("yes"), lit(true))
    .when(lower(col("active_str")).eq("true"), lit(true))
    .when(col("active_str").eq("1"), lit(true))
    .when(lower(col("active_str")).eq("no"), lit(false))
    .when(lower(col("active_str")).eq("false"), lit(false))
    .when(col("active_str").eq("0"), lit(false))
    .otherwise(lit(null))
)
```

## Date and Time Conversions

### String to Date

```typescript
import { col, toDate } from "Molniya";

// Parse date string
df.withColumn("date_parsed", toDate(col("date_str"), "YYYY-MM-DD"))

// Different formats
df.withColumn("date_us", toDate(col("date_us"), "MM/DD/YYYY"))
df.withColumn("date_eu", toDate(col("date_eu"), "DD/MM/YYYY"))
```

### String to Timestamp

```typescript
import { col, toTimestamp } from "Molniya";

// Parse timestamp
df.withColumn("ts_parsed", toTimestamp(col("ts_str"), "YYYY-MM-DD HH:mm:ss"))

// ISO format
df.withColumn("ts_iso", toTimestamp(col("ts_iso"), "YYYY-MM-DDTHH:mm:ssZ"))
```

### Date to String

```typescript
import { col, formatDate } from "Molniya";

// Format date
df.withColumn("date_formatted", formatDate(col("date"), "YYYY-MM-DD"))

// Human readable
df.withColumn("date_display", formatDate(col("date"), "MMM DD, YYYY"))
// Result: "Jan 15, 2024"
```

### Timestamp to Date

```typescript
import { col, cast, DType } from "Molniya";

// Extract date from timestamp
df.withColumn("date_only", cast(col("timestamp"), DType.date))
```

### Epoch Conversions

```typescript
import { col, lit } from "Molniya";

// Seconds since epoch to timestamp
df.withColumn("ts_from_seconds", col("epoch_seconds").mul(1000))

// Milliseconds to date
df.withColumn("date_from_ms", cast(col("epoch_ms"), DType.date))
```

## Boolean Conversions

### To Boolean

```typescript
import { col, cast, DType, neq } from "Molniya";

// Number to boolean (0 = false, non-zero = true)
df.withColumn("has_value", cast(col("count"), DType.boolean))

// String to boolean via comparison
df.withColumn("is_active", col("status").eq(lit("active")))

// Null check to boolean
df.withColumn("has_email", col("email").isNotNull())
```

### From Boolean

```typescript
import { col, when, lit, cast, DType } from "Molniya";

// Boolean to number (1/0)
df.withColumn("active_flag", cast(col("is_active"), DType.int32))

// Boolean to string
df.withColumn("status_text",
  when(col("is_active"), lit("Active")).otherwise(lit("Inactive"))
)
```

## Handling Invalid Casts

### Safe Casting

```typescript
import { col, cast, DType, regexpMatch, when, lit } from "Molniya";

// Only cast valid numeric strings
df.withColumn("amount_safe",
  when(regexpMatch(col("amount_str"), "^-?[0-9]+(\\.[0-9]+)?$"),
    cast(col("amount_str"), DType.float64)
  ).otherwise(lit(null))
)
```

### Try Cast Pattern

```typescript
import { col, cast, DType, when } from "Molniya";

// Try cast with fallback
df.withColumn("amount_or_zero",
  when(col("amount_str").isNotNull(),
    cast(col("amount_str"), DType.float64)
  ).otherwise(lit(0))
)
```

## Common Recipes

### Currency Conversion

```typescript
import { col, cast, DType, round } from "Molniya";

// Convert cents to dollars
df.withColumn("amount_dollars", 
  cast(col("amount_cents"), DType.float64).div(100).round(2)
)

// Convert dollars to cents
df.withColumn("amount_cents",
  cast(col("amount_dollars"), DType.int32).mul(100)
)
```

### Percentage Handling

```typescript
import { col, cast, DType } from "Molniya";

// String percentage to decimal
df.withColumn("rate_decimal",
  cast(regexpReplace(col("rate_str"), "%", ""), DType.float64).div(100)
)

// Decimal to percentage string
df.withColumn("rate_percent",
  cast(col("rate").mul(100), DType.string).add("%")
)
```

### ID Formatting

```typescript
import { col, cast, DType, lpad } from "Molniya";

// Format ID with leading zeros
df.withColumn("formatted_id",
  lpad(cast(col("id"), DType.string), 10, "0")
)
// Result: "0000000123"
```

### Unix Timestamp Conversion

```typescript
import { col, cast, DType } from "Molniya";

// Unix timestamp (seconds) to date
df.withColumn("date",
  cast(col("unix_ts").mul(1000), DType.date)
)

// Date to unix timestamp
df.withColumn("unix_ts",
  cast(col("date"), DType.int64).div(1000)
)
```

### JSON String Parsing

```typescript
import { col } from "Molniya";

// Parse JSON string (returns object, then extract field)
df.withColumn("parsed", parseJson(col("json_str")))
  .withColumn("name", col("parsed").get("name"))
  .withColumn("value", cast(col("parsed").get("value"), DType.float64))
```

## Schema Casting

### Cast Entire DataFrame

```typescript
import { cast, DType } from "Molniya";

// Cast all columns to new schema
df.cast({
  id: DType.int32,
  amount: DType.float64,
  count: DType.int32,
  name: DType.string
})
```

### Selective Casting

```typescript
import { col, cast, DType } from "Molniya";

// Only cast specific columns
df.withColumn("id", cast(col("id"), DType.int32))
  .withColumn("amount", cast(col("amount"), DType.float64))
```

## Best Practices

1. **Check before casting**: Validate string format before parsing numbers
2. **Handle nulls**: Casting null returns null; use `fillNull()` if needed
3. **Watch for overflow**: Large numbers may overflow when downcasting
4. **Use appropriate types**: Choose the smallest type that fits your data
5. **Document conversions**: Add comments explaining non-obvious casts

## Type Conversion Reference

| From | To | Method | Notes |
|------|-----|--------|-------|
| String | Integer | `cast(col, DType.int32)` | Parses number from string |
| String | Float | `cast(col, DType.float64)` | Parses decimal from string |
| String | Date | `toDate(col, format)` | Use format pattern |
| String | Timestamp | `toTimestamp(col, format)` | Use format pattern |
| Integer | Float | `cast(col, DType.float64)` | Exact conversion |
| Float | Integer | `cast(col, DType.int32)` | Truncates decimal |
| Integer | String | `cast(col, DType.string)` | Decimal representation |
| Float | String | `cast(col, DType.string)` | May use scientific notation |
| Date | String | `formatDate(col, format)` | Format to string |
| Timestamp | Date | `cast(col, DType.date)` | Drops time component |
| Boolean | Integer | `cast(col, DType.int32)` | true=1, false=0 |
| Integer | Boolean | `cast(col, DType.boolean)` | 0=false, non-zero=true |
