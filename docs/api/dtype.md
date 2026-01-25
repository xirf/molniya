# DType

Data type enumeration for defining column schemas.

## Overview

`DType` specifies the type of data stored in DataFrame and Series columns. It enables type checking, memory optimization, and proper parsing.

```typescript
import { DType } from "molniya";

const schema = {
  id: DType.Int32,
  name: DType.String,
  price: DType.Float64,
};
```

## Available Types

### DType.String

UTF-8 text strings.

```typescript
const schema = { name: DType.String };
```

**JavaScript type:** `string`

**Use for:**

- Names, labels, categories
- Text data
- Identifiers (non-numeric)

**Example:**

```typescript
const df = DataFrame.fromColumns(
  { name: ["Alice", "Bob", "Carol"] },
  { name: DType.String },
);
```

### DType.Int32

32-bit signed integers (-2³¹ to 2³¹-1).

```typescript
const schema = { age: DType.Int32 };
```

**JavaScript type:** `number`

**Range:** -2,147,483,648 to 2,147,483,647

**Use for:**

- Counts, quantities
- IDs (< 2 billion)
- Ages, scores
- Most integer data

**Example:**

```typescript
const df = DataFrame.fromColumns(
  { count: [1, 2, 3, 100] },
  { count: DType.Int32 },
);
```

### DType.Int64

64-bit signed integers (-2⁶³ to 2⁶³-1).

```typescript
const schema = { large_id: DType.Int64 };
```

**JavaScript type:** `bigint`

**Range:** -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807

**Use for:**

- Large IDs (e.g., database keys)
- Timestamps (milliseconds)
- Very large counts
- Financial data (cents)

**Example:**

```typescript
const df = DataFrame.fromColumns(
  { timestamp_ms: [1704067200000n, 1704153600000n] },
  { timestamp_ms: DType.Int64 },
);
```

**Note:** Values are BigInt in JavaScript:

```typescript
const value = df.get("timestamp_ms").get(0);
// value is bigint, not number
console.log(value + 1000n); // Use 'n' suffix
```

### DType.Float64

Double-precision floating-point numbers (IEEE 754).

```typescript
const schema = { price: DType.Float64 };
```

**JavaScript type:** `number`

**Precision:** ~15-17 significant digits

**Use for:**

- Prices, measurements
- Scientific data
- Percentages, ratios
- Any decimal values

**Example:**

```typescript
const df = DataFrame.fromColumns(
  { price: [10.99, 20.5, 5.25] },
  { price: DType.Float64 },
);
```

**Special values:**

```typescript
const df = DataFrame.fromColumns(
  { value: [1.5, Infinity, -Infinity, NaN] },
  { value: DType.Float64 },
);
```

### DType.Boolean

True/false values.

```typescript
const schema = { active: DType.Boolean };
```

**JavaScript type:** `boolean`

**Use for:**

- Flags, status
- Yes/no fields
- Active/inactive

**Example:**

```typescript
const df = DataFrame.fromColumns(
  { active: [true, false, true] },
  { active: DType.Boolean },
);
```

**CSV parsing:**

```csv
active
true
false
1
0
yes
no
```

Molniya recognizes: `true`/`false`, `1`/`0`, `yes`/`no` (case-insensitive).

### DType.Datetime

Date and time values.

```typescript
const schema = { created_at: DType.Datetime };
```

**JavaScript type:** `Date`

**Use for:**

- Timestamps
- Date fields
- Event times

**Example:**

```typescript
const df = DataFrame.fromColumns(
  {
    created_at: [new Date("2024-01-01"), new Date("2024-01-02")],
  },
  { created_at: DType.Datetime },
);
```

**CSV parsing:**
Accepts ISO 8601 format:

```csv
timestamp
2024-01-01
2024-01-01T10:30:00
2024-01-01T10:30:00Z
2024-01-01T10:30:00-05:00
```

All parsed to JavaScript `Date` objects.

## Type Selection Guide

Choose the right type for your data:

| Data            | DType              | Reason           |
| --------------- | ------------------ | ---------------- |
| Person name     | `String`           | Text             |
| Age (years)     | `Int32`            | Small integers   |
| User ID         | `Int32` or `Int64` | Depends on size  |
| Price (dollars) | `Float64`          | Decimals         |
| Quantity        | `Int32`            | Whole numbers    |
| Is active       | `Boolean`          | True/false       |
| Created date    | `Datetime`         | Timestamps       |
| Country code    | `String`           | Text             |
| Temperature     | `Float64`          | Decimals         |
| Product SKU     | `String`           | May have letters |

## Schema Definition

Define schemas for DataFrames and CSV reading:

```typescript
const schema = {
  id: DType.Int32,
  name: DType.String,
  email: DType.String,
  age: DType.Int32,
  salary: DType.Float64,
  active: DType.Boolean,
  joined: DType.Datetime,
};
```

Used in:

**DataFrame creation:**

```typescript
const df = DataFrame.fromColumns(data, schema);
```

**CSV reading:**

```typescript
const result = await readCsv("data.csv", schema);
```

**Lazy loading:**

```typescript
const query = LazyFrame.scanCsv("data.csv", schema);
```

## Type Inference

TypeScript infers Series types from DType:

```typescript
const df = DataFrame.fromColumns(
  { name: ["Alice"], age: [25] },
  { name: DType.String, age: DType.Int32 },
);

const names: Series<string> = df.get("name"); // ✓ string
const ages: Series<number> = df.get("age"); // ✓ number

// names.toArray() is string[]
// ages.toArray() is number[]
```

## Type Checking

Molniya validates types at runtime:

```typescript
// This will fail - wrong type
const df = DataFrame.fromColumns(
  { age: ["twenty-five"] }, // String, not number
  { age: DType.Int32 }, // Expects number
);
// Error: Cannot convert "twenty-five" to Int32
```

**CSV parsing errors:**

```csv
id,value
1,100.5
2,invalid
3,200.3
```

```typescript
const result = await readCsv("data.csv", {
  id: DType.Int32,
  value: DType.Float64,
});

// result.ok is false
// result.error.message includes "Cannot parse 'invalid' as Float64"
```

## Null Handling

All types support null values:

```typescript
const df = DataFrame.fromColumns(
  { value: [1, null, 3] },
  { value: DType.Int32 },
);

const series = df.get("value");
series.get(1); // null
series.isNull(1); // true
```

**CSV nulls:**

```typescript
const result = await readCsv("data.csv", schema, {
  nullValues: ["NA", "NULL", ""],
});
```

## Memory Considerations

Different types use different amounts of memory:

| DType      | Bytes per value | Notes                     |
| ---------- | --------------- | ------------------------- |
| `Boolean`  | 1               | Minimal                   |
| `Int32`    | 4               | Efficient for integers    |
| `Int64`    | 8               | Double the space of Int32 |
| `Float64`  | 8               | Standard decimal          |
| `String`   | Variable        | Depends on length         |
| `Datetime` | 8               | Stored as timestamp       |

**For large datasets:**

- Use `Int32` instead of `Int64` when possible
- Consider string length when using `String`
- Use `Boolean` for binary flags

**Example:**

```typescript
// 1 million rows
// Int32: ~4MB, Int64: ~8MB
const schema = {
  id: DType.Int32, // IDs < 2 billion
  amount: DType.Float64, // Need decimals
  code: DType.String, // Variable length
};
```

## Complete Examples

### E-commerce schema

```typescript
const productSchema = {
  product_id: DType.Int32,
  sku: DType.String,
  name: DType.String,
  category: DType.String,
  price: DType.Float64,
  quantity: DType.Int32,
  in_stock: DType.Boolean,
  created_at: DType.Datetime,
};
```

### User analytics schema

```typescript
const userSchema = {
  user_id: DType.Int64, // Large user base
  email: DType.String,
  age: DType.Int32,
  signup_date: DType.Datetime,
  is_premium: DType.Boolean,
  total_spent: DType.Float64,
  login_count: DType.Int32,
};
```

### Financial data schema

```typescript
const tradeSchema = {
  trade_id: DType.Int64,
  symbol: DType.String,
  price: DType.Float64,
  quantity: DType.Int32,
  timestamp: DType.Datetime,
  is_buy: DType.Boolean,
};
```

### Sensor data schema

```typescript
const sensorSchema = {
  sensor_id: DType.Int32,
  timestamp: DType.Datetime,
  temperature: DType.Float64,
  humidity: DType.Float64,
  pressure: DType.Float64,
  is_calibrated: DType.Boolean,
};
```

## Best Practices

**1. Choose the smallest type that fits your data**

```typescript
// Good: IDs fit in Int32
const schema = { user_id: DType.Int32 };

// Overkill: Using Int64 unnecessarily
const schema = { user_id: DType.Int64 };
```

**2. Use Int64 for timestamps**

```typescript
const schema = {
  timestamp_ms: DType.Int64, // Milliseconds since epoch
};
```

**3. Be consistent with null handling**

```typescript
// Define null values upfront
const result = await readCsv("data.csv", schema, {
  nullValues: ["NA", "NULL", ""],
});
```

**4. Document your schema**

```typescript
/**
 * User data schema
 * - user_id: Unique identifier (max: 2B)
 * - created_at: Account creation timestamp
 * - score: 0.0 to 100.0
 */
const userSchema = {
  user_id: DType.Int32,
  created_at: DType.Datetime,
  score: DType.Float64,
};
```

## See Also

- [Data Types Guide](/guide/data-types.md) - Detailed type usage
- [DataFrame API](./dataframe.md) - Using typed DataFrames
- [Series API](./series.md) - Type-safe column operations
- [CSV Reading](./csv-reading.md) - Schema in CSV parsing
