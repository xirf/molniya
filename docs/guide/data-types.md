# Data Types

Molniya provides a comprehensive set of data types optimized for analytical workloads.

## Type Overview

| Type        | Storage  | TypeScript Type | Description                        |
| ----------- | -------- | --------------- | ---------------------------------- |
| `int8`      | 1 byte   | `number`        | Signed 8-bit integer (-128 to 127) |
| `int16`     | 2 bytes  | `number`        | Signed 16-bit integer              |
| `int32`     | 4 bytes  | `number`        | Signed 32-bit integer              |
| `int64`     | 8 bytes  | `bigint`        | Signed 64-bit integer              |
| `uint8`     | 1 byte   | `number`        | Unsigned 8-bit integer (0 to 255)  |
| `uint16`    | 2 bytes  | `number`        | Unsigned 16-bit integer            |
| `uint32`    | 4 bytes  | `number`        | Unsigned 32-bit integer            |
| `uint64`    | 8 bytes  | `bigint`        | Unsigned 64-bit integer            |
| `float32`   | 4 bytes  | `number`        | 32-bit floating point              |
| `float64`   | 8 bytes  | `number`        | 64-bit floating point (double)     |
| `boolean`   | 1 byte   | `boolean`       | true/false                         |
| `string`    | 4 bytes* | `string`        | Dictionary-encoded string          |
| `date`      | 4 bytes  | `Date`          | Days since Unix epoch              |
| `timestamp` | 8 bytes  | `bigint`        | Milliseconds since Unix epoch      |

\* String columns store a 4-byte dictionary index. The actual string data is stored separately in the dictionary.

## Using DType

Access types through the `DType` object:

```typescript
import { DType } from "molniya";

// Non-nullable types
const schema = {
  id: DType.int32,
  amount: DType.float64,
  active: DType.boolean,
  name: DType.string
};

// Nullable types
const schemaWithNulls = {
  id: DType.int32,
  middleName: DType.nullable.string,  // Can be null
  age: DType.nullable.int32
};
```

## Numeric Types

### Integer Types

Choose the smallest type that fits your data range:

```typescript
// Small ranges
DType.int8   // -128 to 127
DType.uint8  // 0 to 255

// Medium ranges
DType.int16  // -32,768 to 32,767
DType.uint16 // 0 to 65,535

// Standard integer
DType.int32  // -2^31 to 2^31-1
DType.uint32 // 0 to 2^32-1

// Large numbers (returns bigint in JS)
DType.int64  // -2^63 to 2^63-1
DType.uint64 // 0 to 2^64-1
```

::: tip Memory Efficiency
Using `int16` instead of `int32` halves the memory usage for that column. For datasets with millions of rows, this adds up.
:::

### Floating Point Types

```typescript
// Single precision (7 decimal digits)
DType.float32

// Double precision (15 decimal digits)
DType.float64
```

Use `float64` for:
- Financial calculations
- Scientific data
- When precision matters

Use `float32` for:
- Graphics data
- ML features
- When memory is constrained

::: warning Floating Point Precision
Floating point numbers have precision limitations. Don't use them for exact decimal arithmetic (like currency). Consider using `int64` with fixed-point representation (store cents instead of dollars).
:::

## String Type

Strings are automatically dictionary-encoded:

```typescript
const schema = {
  category: DType.string,            // Non-nullable
  description: DType.nullable.string // Can be null
};
```

Dictionary encoding means:
- Each unique string is stored once in a dictionary
- The column stores 4-byte integer indices
- Great for columns with repeated values (categories, tags, etc.)

> [!NOTE]
> Dictionary encoding makes string comparisons fast (integer comparison) and reduces memory usage for repetitive data. 
> 
> If you have a dataset where every string is unique (like IDs, raw text for NLP, or high-cardinality log messages), forcing dictionary encoding is actually inefficient.
>
> This will be fixed on future release

## Boolean Type

```typescript
const schema = {
  active: DType.boolean,
  verified: DType.nullable.boolean
};
```

Booleans are stored as 1 byte per value (not bit-packed for performance reasons).

## Date and Timestamp Types

### Date

Stores days since Unix epoch (January 1, 1970):

```typescript
const schema = {
  birthDate: DType.date,
  hiredOn: DType.nullable.date
};
```

When reading CSV, dates should be in ISO format (`YYYY-MM-DD`).

### Timestamp

Stores milliseconds since Unix epoch as a `bigint`:

```typescript
const schema = {
  createdAt: DType.timestamp,
  updatedAt: DType.nullable.timestamp
};
```

::: warning Timestamp in JavaScript
Timestamps are returned as `bigint` values, not `Date` objects. Convert if needed:
```typescript
const ts = 1704067200000n;  // bigint
const date = new Date(Number(ts));
```
:::

## Nullable Types

All types have nullable variants:

```typescript
DType.nullable.int32
DType.nullable.float64
DType.nullable.string
DType.nullable.boolean
// ... etc
```

Nullable types:
- Use an additional null bitmap (1 bit per row)
- Allow `null` values in the column
- Have slight performance overhead vs non-nullable

## Type Conversion (Casting)

Convert between types using `cast()`:

```typescript
import { col } from "molniya";

// Cast int32 to float64 for division
df.withColumn("ratio", col("part").cast(DType.float64).div(col("total")))

// Cast float to int (truncates decimals)
df.withColumn("whole", col("amount").cast(DType.int32))
```

::: warning Data Loss
Casting from larger to smaller types may cause overflow. Casting from float to int truncates decimals.
:::

## Choosing the Right Type

### For IDs and Counts

```typescript
// Small dataset (< 2 billion rows)
DType.int32

// Large dataset
DType.int64  // or uint64 if always positive
```

### For Measurements

```typescript
// Precise (money, scientific)
DType.float64

// Approximate (sensor readings, ML)
DType.float32
```

### For Categories

```typescript
// Always use string with dictionary encoding
DType.string
```

### For Flags

```typescript
// Boolean is most efficient
DType.boolean
```

## Schema Validation

Molniya validates schemas at creation time:

```typescript
import { createSchema } from "molniya";

const result = createSchema({
  "invalid-column-name!": DType.int32  // Error: invalid characters
});

if (result.error) {
  console.error("Schema error:", result.error);
}
```

Valid column names:
- Start with a letter or underscore
- Contain only letters, numbers, and underscores
- Maximum 256 characters
