# Strings & Dates

Molniya provides efficient string storage and date/time handling for data processing workflows.

## String Type

### Dictionary Encoding

Molniya stores strings using dictionary encoding for memory efficiency:

```
Raw strings:    ["apple", "banana", "apple", "cherry", "banana"]
Dictionary:     ["apple", "banana", "cherry"]
Storage:        [0, 1, 0, 2, 1]  (4-byte indices)
```

This provides significant savings when:
- Strings have repeated values (categories, statuses, names)
- String values are longer than 4 characters
- You need fast equality comparisons

```typescript
import { DType } from "molniya";

const schema = {
  // Dictionary-encoded string (4 bytes per value + dictionary)
  category: DType.string,
  status: DType.string,
  email: DType.string
};
```

### Nullable Strings

```typescript
const schema = {
  // Required string
  name: DType.string,
  
  // Optional string (may be null)
  middle_name: DType.nullable.string,
  nickname: DType.nullable.string
};
```

## String Operations

### String Concatenation

Combine strings using the `add()` method:

```typescript
import { col } from "molniya";

// Concatenate columns
df.withColumn("full_name", col("first").add(" ").add(col("last")))

// Add prefix/suffix
df.withColumn("email_formatted", col("email").add("@company.com"))
  .withColumn("greeting", lit("Hello, ").add(col("name")))
```

### String Length

```typescript
import { length } from "molniya";

// Get string length
df.withColumn("name_length", length(col("name")))
  .filter(length(col("email")).gt(5))
```

### Substring Extraction

```typescript
import { substring } from "molniya";

// Extract substring (start, length)
df.withColumn("area_code", substring(col("phone"), 0, 3))
  .withColumn("year", substring(col("date_str"), 0, 4))
  .withColumn("domain", substring(col("email"), col("email").indexOf("@").add(1), 100))
```

### String Contains

```typescript
import { contains } from "molniya";

// Check if string contains substring
df.filter(contains(col("email"), "@company.com"))
  .filter(contains(col("description"), "urgent"))
```

### Case Operations

```typescript
import { upper, lower } from "molniya";

// Convert case
df.withColumn("name_upper", upper(col("name")))
  .withColumn("email_lower", lower(col("email")))
  .filter(lower(col("status")).eq("active"))
```

### Starts With / Ends With

```typescript
import { startsWith, endsWith } from "molniya";

// Check prefixes and suffixes
df.filter(startsWith(col("phone"), "+1"))
  .filter(endsWith(col("email"), ".com"))
```

## Date Type

The `date` type stores dates as days since the Unix epoch (January 1, 1970):

```typescript
const schema = {
  // Date stored as 4-byte integer (days since epoch)
  birth_date: DType.date,
  order_date: DType.date,
  
  // Nullable date
  shipped_date: DType.nullable.date
};
```

## Timestamp Type

The `timestamp` type stores timestamps as milliseconds since the Unix epoch:

```typescript
const schema = {
  // Timestamp stored as 8-byte bigint (milliseconds)
  created_at: DType.timestamp,
  updated_at: DType.timestamp,
  
  // Nullable timestamp
  deleted_at: DType.nullable.timestamp
};
```

## Date/Timestamp Operations

### Creating Date/Timestamp Columns

```typescript
import { lit, toDate, toTimestamp } from "molniya";

// Convert string to date
df.withColumn("parsed_date", toDate(col("date_str"), "YYYY-MM-DD"))

// Convert string to timestamp
df.withColumn("parsed_ts", toTimestamp(col("datetime_str"), "YYYY-MM-DD HH:mm:ss"))

// Create from literal
df.withColumn("epoch", lit(new Date("1970-01-01")))
```

### Extracting Components

```typescript
import { year, month, day, hour, minute, second } from "molniya";

// Date components
df.withColumn("year", year(col("order_date")))
  .withColumn("month", month(col("order_date")))
  .withColumn("day", day(col("order_date")))
  .withColumn("quarter", quarter(col("order_date")))
  .withColumn("day_of_week", dayOfWeek(col("order_date")))

// Timestamp components
df.withColumn("hour", hour(col("created_at")))
  .withColumn("minute", minute(col("created_at")))
  .withColumn("second", second(col("created_at")))
```

### Date Arithmetic

```typescript
import { addDays, subDays, diffDays } from "molniya";

// Add/subtract days
df.withColumn("due_date", addDays(col("order_date"), 30))
  .withColumn("previous_order", subDays(col("order_date"), 7))

// Calculate difference
df.withColumn("days_since", diffDays(col("shipped_date"), col("order_date")))
  .withColumn("account_age_days", diffDays(lit(new Date()), col("created_at")))
```

### Date Truncation

```typescript
import { truncateDate } from "molniya";

// Truncate to period start
df.withColumn("month_start", truncateDate(col("date"), "month"))
  .withColumn("week_start", truncateDate(col("date"), "week"))
  .withColumn("year_start", truncateDate(col("date"), "year"))
```

## Working with CSV Dates

When reading dates from CSV files, they are initially read as strings:

```typescript
import { readCsv, DType, col, toDate } from "molniya";

// Read date as string, then convert
const df = await readCsv("orders.csv", {
  order_id: DType.int32,
  order_date_str: DType.string,  // Read as string first
  amount: DType.float64
});

// Convert to date type
const withDate = df.withColumn("order_date", toDate(col("order_date_str"), "YYYY-MM-DD"))
  .drop("order_date_str");  // Remove original string column
```

## Common Patterns

### Age Calculation

```typescript
import { col, lit, diffDays, toDate } from "molniya";

// Calculate age from birth date
const today = new Date();
df.withColumn("birth_date", toDate(col("birth_date_str"), "YYYY-MM-DD"))
  .withColumn("age_years", diffDays(lit(today), col("birth_date")).div(365.25).floor())
```

### Date Range Filtering

```typescript
import { col, and, gte, lte } from "molniya";

// Filter by date range
const startDate = new Date("2024-01-01");
const endDate = new Date("2024-12-31");

df.filter(and(
  col("order_date").gte(startDate),
  col("order_date").lte(endDate)
))
```

### Formatting Dates for Display

```typescript
import { formatDate } from "molniya";

// Format dates for output
df.withColumn("formatted", formatDate(col("date"), "MMM DD, YYYY"))
// Result: "Jan 15, 2024"
```

### Time-Based Grouping

```typescript
import { col, year, month } from "molniya";

// Group by year and month
df.withColumn("year", year(col("order_date")))
  .withColumn("month", month(col("order_date")))
  .groupBy(["year", "month"], [
    { name: "total", expr: sum("amount") },
    { name: "count", expr: count() }
  ])
```

## Best Practices

1. **Use `date` for dates, `timestamp` for datetimes**: Saves memory vs storing as strings
2. **Parse dates early**: Convert string dates to `date`/`timestamp` types after reading
3. **Use dictionary encoding**: String columns with repeated values are very efficient
4. **Consider timezone**: Molniya stores timestamps in UTC; handle timezone conversion in your application
5. **Index date columns**: When possible, filter on dates before other operations for better performance

## Type Conversion Summary

| From | To | Method |
|------|-----|--------|
| String | Date | `toDate(col, format)` |
| String | Timestamp | `toTimestamp(col, format)` |
| Date | String | `formatDate(col, format)` |
| Timestamp | Date | `toDate(col)` |
| Date | Timestamp | Cast or multiply by 86400000 |
| Epoch days | Date | Direct (int32) |
| Epoch ms | Timestamp | Direct (int64) |
