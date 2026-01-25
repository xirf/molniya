# Data Types

Understanding Molniya's type system is key to using it effectively.

## Why Types Matter

Molniya is schema-first. You define column types upfront, and this enables:

- **Type safety** - Catch errors at runtime
- **Optimization** - SIMD operations on numeric types
- **Memory efficiency** - Compact storage for primitives
- **Clear intent** - Your code documents what data it expects

## Available Types

### String

Text data of any length.

```typescript
import { DType } from "molniya";

const schema = {
  name: DType.String,
  description: DType.String,
};
```

**When to use:**

- Names, descriptions, categories
- Identifiers (though Int32/Int64 might be better)
- Any text that varies in length

**Operations:**

- String searching (contains, startsWith, endsWith)
- Case conversion (toLowerCase, toUpperCase)
- Trimming, splitting, replacing

### Int32

32-bit signed integers (-2,147,483,648 to 2,147,483,647).

```typescript
const schema = {
  id: DType.Int32,
  count: DType.Int32,
  year: DType.Int32,
};
```

**When to use:**

- IDs, counts, quantities
- Years, days, indices
- Any whole number in the range

**Why Int32 vs Int64?**

- Smaller memory footprint (4 bytes vs 8)
- Faster arithmetic on most CPUs
- Use Int32 unless you need bigger numbers

### Int64

64-bit signed integers (huge range).

```typescript
const schema = {
  user_id: DType.Int64,
  timestamp_ms: DType.Int64,
};
```

**When to use:**

- Very large IDs
- Timestamps (milliseconds since epoch)
- Financial amounts in cents (to avoid floating point)

### Float64

64-bit floating point numbers (JavaScript's default number type).

```typescript
const schema = {
  price: DType.Float64,
  temperature: DType.Float64,
  score: DType.Float64,
};
```

**When to use:**

- Prices, measurements, scores
- Percentages, ratios
- Scientific data

**Note on precision:**
Floating point has precision limits. For exact decimal math (like money), consider storing as Int64 cents.

### Boolean

True or false values.

```typescript
const schema = {
  is_active: DType.Boolean,
  has_premium: DType.Boolean,
};
```

**When to use:**

- Flags, switches, toggles
- Yes/no questions
- Status indicators

**Storage:**
Stored as single bits internally for memory efficiency.

### Datetime

Timestamps with nanosecond precision.

```typescript
const schema = {
  created_at: DType.Datetime,
  updated_at: DType.Datetime,
};
```

**When to use:**

- Timestamps, event times
- Date ranges, scheduling
- Time series data

**Format:**
Expects ISO 8601 strings in CSV:

```
2024-01-25T10:30:00Z
2024-01-25T10:30:00.123Z
2024-01-25T10:30:00.123456789Z
```

## Null Values

All types can be null. When loading CSV data:

```typescript
const result = await scanCsv("data.csv", {
  schema: {
    name: DType.String,
    age: DType.Int32,
  },
  nullValues: ["", "NA", "NULL"],
});
```

Checking for nulls:

```typescript
const ages = df.get("age").toArray();
const hasNulls = ages.some((age) => age === null);
const nullCount = ages.filter((age) => age === null).length;
```

## Type Inference

Molniya can infer types when creating DataFrames from data:

```typescript
import { DataFrame } from "molniya";

// Types are inferred:
// - number → Float64
// - string → String
// - boolean → Boolean
const df = DataFrame.fromColumns({
  id: [1, 2, 3], // Float64
  name: ["A", "B", "C"], // String
  active: [true, false, true], // Boolean
});
```

**Recommendation:** Use explicit schemas for CSV files. Inference is convenient for small, in-memory data.

## Choosing the Right Type

### Text that could be a category?

Use String, then analyze unique values:

```typescript
const categories = df.get("category").toArray();
const unique = new Set(categories);
console.log(`${unique.size} unique values`);
```

If there are only a few unique values, you might want to encode them as integers for performance.

### Whole numbers?

Start with Int32. Only use Int64 if you know you need the range.

### Decimals?

Use Float64. If you need exact precision (like money), consider Int64 cents.

### Dates?

Use Datetime if you need time-of-day. If you only care about dates, you could use String with "YYYY-MM-DD" format.

### True/false?

Always Boolean. Most compact.

## Type Conversion

Converting between types after loading:

```typescript
// String to Int32
const stringIds = df.get("id_string").toArray();
const intIds = stringIds.map((s) => parseInt(s, 10));

// Float64 to Int32 (floor)
const prices = df.get("price").toArray();
const rounded = prices.map((p) => Math.floor(p));

// String to Boolean
const flags = df.get("flag").toArray();
const bools = flags.map((f) => f.toLowerCase() === "true");
```

Then create a new DataFrame with the converted values.

## Common Patterns

### Mixed CSV types

CSV file with various types:

```csv
id,name,age,salary,active,joined
1,Alice,25,50000.50,true,2024-01-01
2,Bob,30,60000.75,false,2024-01-15
```

Schema:

```typescript
const schema = {
  id: DType.Int32,
  name: DType.String,
  age: DType.Int32,
  salary: DType.Float64,
  active: DType.Boolean,
  joined: DType.Datetime,
};
```

### Numeric IDs vs String IDs

**Use Int32/Int64 if:**

- IDs are sequential (1, 2, 3...)
- IDs are used for filtering/sorting
- Memory is constrained

**Use String if:**

- IDs are UUIDs or other text formats
- IDs might have leading zeros
- IDs are primarily for display

### Money handling

Option 1: Int64 cents

```typescript
const schema = {
  amount_cents: DType.Int64, // $10.50 → 1050
};

// Display as dollars
const cents = df.get("amount_cents").toArray();
const dollars = cents.map((c) => (c / 100).toFixed(2));
```

Option 2: Float64 dollars

```typescript
const schema = {
  amount: DType.Float64, // 10.50
};
```

**Recommendation:** Use Int64 cents for accounting. Use Float64 for display/analytics.

## Next Steps

Now that you understand types:

### Working with Types

Molniya provides various operations for working with different data types.
See the String Operations section for string-specific functions.
- [Cookbook](./cookbook.md) - Real examples with various types
