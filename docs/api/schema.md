# Schema

Type definition for DataFrame column structure.

## Overview

A Schema is a plain object that maps column names to their data types:

```typescript
type Schema = Record<string, DType>;
```

It defines:

- What columns exist
- The type of each column
- Expected structure of data

## Basic Usage

### Defining Schemas

```typescript
import { DType } from "molniya";

const userSchema = {
  id: DType.Int32,
  name: DType.String,
  email: DType.String,
  age: DType.Int32,
  active: DType.Boolean,
};
```

### Using Schemas

**DataFrame creation:**

```typescript
const df = DataFrame.fromColumns(data, userSchema);
```

**CSV reading:**

```typescript
const result = await readCsv("users.csv", userSchema);
```

**Lazy loading:**

```typescript
const query = LazyFrame.scanCsv("users.csv", userSchema);
```

## Schema Structure

### Column Names

Keys in the schema object are column names:

```typescript
const schema = {
  product_id: DType.Int32, // Column: "product_id"
  product_name: DType.String, // Column: "product_name"
  price: DType.Float64, // Column: "price"
};
```

**Rules:**

- Any valid JavaScript object key
- Case-sensitive
- No duplicate names
- Underscores and numbers OK

**Examples:**

```typescript
// Valid column names
const valid = {
  id: DType.Int32,
  user_id: DType.Int32,
  firstName: DType.String,
  "last-name": DType.String, // Quotes needed for hyphens
  col1: DType.Int32,
  _internal: DType.String,
};
```

### Data Types

Values are DType enum members:

```typescript
const schema = {
  text: DType.String, // Strings
  count: DType.Int32, // 32-bit integers
  big_id: DType.Int64, // 64-bit integers
  amount: DType.Float64, // Decimals
  flag: DType.Boolean, // true/false
  timestamp: DType.Datetime, // Dates
};
```

See [DType Reference](./dtype.md) for details on each type.

## Complete Examples

### E-commerce Products

```typescript
const productSchema = {
  product_id: DType.Int32,
  sku: DType.String,
  name: DType.String,
  description: DType.String,
  category: DType.String,
  brand: DType.String,
  price: DType.Float64,
  cost: DType.Float64,
  quantity: DType.Int32,
  in_stock: DType.Boolean,
  created_at: DType.Datetime,
  updated_at: DType.Datetime,
};

const result = await readCsv("products.csv", productSchema);
```

### User Analytics

```typescript
const analyticsSchema = {
  user_id: DType.Int64,
  session_id: DType.String,
  event_name: DType.String,
  event_timestamp: DType.Datetime,
  page_url: DType.String,
  referrer: DType.String,
  device_type: DType.String,
  is_mobile: DType.Boolean,
  duration_seconds: DType.Int32,
};

const query = LazyFrame.scanCsv("events.csv", analyticsSchema);
```

### Financial Transactions

```typescript
const transactionSchema = {
  transaction_id: DType.Int64,
  account_id: DType.Int64,
  transaction_date: DType.Datetime,
  amount_cents: DType.Int64, // Store as cents
  currency: DType.String,
  merchant: DType.String,
  category: DType.String,
  is_debit: DType.Boolean,
  is_pending: DType.Boolean,
};
```

### Sensor Data

```typescript
const sensorSchema = {
  sensor_id: DType.Int32,
  reading_time: DType.Datetime,
  temperature_c: DType.Float64,
  humidity_pct: DType.Float64,
  pressure_hpa: DType.Float64,
  battery_v: DType.Float64,
  is_calibrated: DType.Boolean,
  location: DType.String,
};
```

## Schema Reuse

Schemas can be reused across operations:

```typescript
const schema = {
  id: DType.Int32,
  name: DType.String,
  value: DType.Float64,
};

// Read multiple files with same schema
const result1 = await readCsv("data1.csv", schema);
const result2 = await readCsv("data2.csv", schema);

// Create DataFrames with same schema
const df1 = DataFrame.fromColumns(data1, schema);
const df2 = DataFrame.fromColumns(data2, schema);

// Lazy queries with same schema
const query1 = LazyFrame.scanCsv("file1.csv", schema);
const query2 = LazyFrame.scanCsv("file2.csv", schema);
```

## Partial Schemas

You can define schemas with only the columns you need:

```typescript
// CSV has 20 columns, but we only care about 3
const minimalSchema = {
  id: DType.Int32,
  name: DType.String,
  value: DType.Float64,
};

// With LazyFrame, only these columns are loaded
const query = LazyFrame.scanCsv("wide.csv", minimalSchema).select([
  "id",
  "value",
]); // Even more selective
```

**Performance benefit:** LazyFrame only loads specified columns.

## Schema Validation

Molniya validates data against schema:

### Type Checking

```typescript
const schema = {
  age: DType.Int32,
};

// This fails - string can't convert to Int32
const df = DataFrame.fromColumns({ age: ["twenty"] }, schema);
// Error: Cannot convert "twenty" to Int32
```

### Column Matching

```typescript
const schema = {
  id: DType.Int32,
  name: DType.String,
};

// This works - extra column ignored
const df = DataFrame.fromColumns(
  { id: [1], name: ["Alice"], extra: [true] },
  schema,
);

// This fails - missing column
const df2 = DataFrame.fromColumns(
  { id: [1] }, // Missing 'name'
  schema,
);
// Error: Missing column: name
```

## Schema Inference

Molniya does NOT infer schemas automatically. You must provide them:

```typescript
// ❌ This doesn't exist
const df = DataFrame.fromColumns(data); // Error

// ✓ Must provide schema
const df = DataFrame.fromColumns(data, schema);
```

**Why explicit schemas?**

- Prevents type errors
- Makes intent clear
- Ensures correct parsing
- Enables optimization

## Best Practices

### 1. Define schemas once

```typescript
// Good: Schema in one place
const USER_SCHEMA = {
  id: DType.Int32,
  name: DType.String,
  email: DType.String,
};

// Reuse everywhere
const df1 = await readCsv("users.csv", USER_SCHEMA);
const df2 = await readCsv("backup.csv", USER_SCHEMA);
```

### 2. Document schemas

```typescript
/**
 * Product catalog schema
 *
 * Columns:
 * - product_id: Unique identifier
 * - sku: Stock keeping unit code
 * - price: Price in USD
 * - quantity: Available stock count
 * - active: Whether product is listed
 */
const PRODUCT_SCHEMA = {
  product_id: DType.Int32,
  sku: DType.String,
  price: DType.Float64,
  quantity: DType.Int32,
  active: DType.Boolean,
};
```

### 3. Use meaningful names

```typescript
// Good: Clear names
const schema = {
  customer_id: DType.Int32,
  order_date: DType.Datetime,
  total_amount_usd: DType.Float64,
};

// Bad: Unclear names
const schema = {
  c_id: DType.Int32,
  dt: DType.Datetime,
  amt: DType.Float64,
};
```

### 4. Choose appropriate types

```typescript
// Good: Right-sized types
const schema = {
  user_id: DType.Int32, // < 2 billion users
  timestamp_ms: DType.Int64, // Large timestamps
  price: DType.Float64, // Needs decimals
};

// Wasteful: Oversized types
const schema = {
  user_id: DType.Int64, // Overkill for small IDs
  count: DType.Float64, // Int32 would work
};
```

### 5. Group related schemas

```typescript
// schemas.ts
export const SCHEMAS = {
  user: {
    id: DType.Int32,
    name: DType.String,
    email: DType.String,
  },

  order: {
    order_id: DType.Int32,
    user_id: DType.Int32,
    total: DType.Float64,
    date: DType.Datetime,
  },

  product: {
    product_id: DType.Int32,
    name: DType.String,
    price: DType.Float64,
  },
};

// Use:
import { SCHEMAS } from "./schemas";
const result = await readCsv("users.csv", SCHEMAS.user);
```

## Type Safety

TypeScript infers types from schema:

```typescript
const schema = {
  name: DType.String,
  age: DType.Int32,
  score: DType.Float64,
};

const df = DataFrame.fromColumns(data, schema);

// TypeScript knows these types:
const nameCol: Series<string> = df.get("name");
const ageCol: Series<number> = df.get("age");
const scoreCol: Series<number> = df.get("score");
```

## Schema Compatibility

Schemas must match across operations:

```typescript
const schema = {
  id: DType.Int32,
  value: DType.Float64,
};

const df1 = await readCsv("data1.csv", schema);
const df2 = await readCsv("data2.csv", schema);

// Can concat - same schema
const combined = concat([df1, df2]);

// Can't concat - different schemas
const otherSchema = {
  id: DType.Int32,
  name: DType.String, // Different column
};
const df3 = await readCsv("data3.csv", otherSchema);
const bad = concat([df1, df3]); // Error
```

## Advanced Patterns

### Schema Builder

```typescript
function buildSchema(columns: string[], type: DType): Schema {
  const schema: Schema = {};
  for (const col of columns) {
    schema[col] = type;
  }
  return schema;
}

// All columns are strings
const schema = buildSchema(["col1", "col2", "col3"], DType.String);
```

### Schema Extension

```typescript
const baseSchema = {
  id: DType.Int32,
  name: DType.String,
};

const extendedSchema = {
  ...baseSchema,
  created_at: DType.Datetime,
  active: DType.Boolean,
};
```

### Schema Validation Function

```typescript
function validateSchema(data: any, schema: Schema): string[] {
  const errors: string[] = [];

  // Check required columns
  for (const col in schema) {
    if (!(col in data)) {
      errors.push(`Missing column: ${col}`);
    }
  }

  return errors;
}

const errors = validateSchema(data, schema);
if (errors.length > 0) {
  console.error("Schema errors:", errors);
}
```

## See Also

- [DType Reference](./dtype.md) - Available data types
- [DataFrame API](./dataframe.md) - Using schemas with DataFrames
- [CSV Reading](./csv-reading.md) - Schema in CSV parsing
- [Data Types Guide](/guide/data-types.md) - Choosing the right types
