# Core Concepts

Understanding these fundamental concepts will help you use Molniya effectively.

## Columnar Storage

Molniya stores data in a columnar format, meaning each column is stored as a contiguous array rather than storing rows together:

```
Row-oriented (traditional):
[ {id: 1, name: "Alice"}, {id: 2, name: "Bob"} ]

Columnar (Molniya):
ids:    [1, 2]
names:  ["Alice", "Bob"]  // Actually dictionary-encoded indices
```

This provides several advantages:

- **Better cache locality** when processing single columns
- **Vectorized operations** using SIMD instructions
- **Efficient compression** of similar values within a column
- **Reduced memory bandwidth** for analytical queries

## Lazy Evaluation

Molniya uses lazy evaluation, which means operations don't execute immediately. Instead, they build a logical execution plan:

```typescript
const df = await readCsv("data.csv", schema);

// These just build a plan:
const step1 = df.filter(col("age").gt(18));
const step2 = step1.select("name", "salary");
const step3 = step2.limit(100);

// Execution happens here:
await step3.show();
```

The plan might be optimized before execution:
- Predicate pushdown (filter before reading all columns)
- Projection pushdown (only read needed columns)
- Operator fusion (combine multiple operations)

## Streaming vs Materialization

### Streaming Operations

These process data in chunks and work with datasets larger than memory:

- `filter()` - Row filtering
- `project()` / `select()` - Column selection
- `limit()` - Row limiting

### Materializing Operations

These load all data into memory:

- `sort()` - Requires all data to sort
- `groupBy()` - Currently buffers all data
- `join()` - Loads both DataFrames
- `collect()` - Explicitly materializes

When working with large files, structure your pipeline to do filtering and projection before materializing operations:

```typescript
// Good: Filter first, then sort a smaller dataset
const result = await df
  .filter(col("year").eq(2024))  // Streaming
  .sort("amount")                 // Materializes filtered data only
  .limit(10)
  .collect();

// Less efficient: Sorts everything first
const result = await df
  .sort("amount")                 // Materializes all data
  .filter(col("year").eq(2024))  // Then filters
  .limit(10)
  .collect();
```

## Dictionary Encoding

String columns use dictionary encoding by default:

```
Original: ["apple", "banana", "apple", "cherry", "banana"]
Dictionary: ["apple", "banana", "cherry"]
Encoded:   [0, 1, 0, 2, 1]
```

Benefits:
- **Memory efficiency**: Repeated strings stored once
- **Cache efficiency**: Integer comparisons are faster
- **Vectorization**: Integer arrays work better with SIMD

## Type System

Molniya has a strict type system built on `DType`:

```typescript
import { DType } from "molniya";

// Non-nullable types
DType.int8
DType.int16
DType.int32
DType.int64      // bigint in JS
DType.uint8
DType.uint16
DType.uint32
DType.uint64     // bigint in JS
DType.float32
DType.float64
DType.boolean
DType.string
DType.date       // Days since epoch
DType.timestamp  // Milliseconds since epoch

// Nullable variants
DType.nullable.int32
DType.nullable.string
```

TypeScript can infer the resulting types from operations:

```typescript
const df = await readCsv("data.csv", {
  id: DType.int32,
  amount: DType.float64
});

// TypeScript knows 'id' is number, 'amount' is number
type RowType = { id: number; amount: number };
```

## The Expression System

Expressions are the building blocks of transformations:

```typescript
import { col, lit, and, sum, avg } from "molniya";

// Column references
col("name")

// Literals
lit(100)
lit("hello")
lit(null)

// Arithmetic
col("price").mul(1.1)
add(col("a"), col("b"))

// Comparisons
col("age").gte(18)

// Logical
and(col("a").gt(0), col("b").lt(100))

// Aggregations (only valid in groupBy context)
sum("amount")
avg("age")
count()
```

Expressions are compiled to optimized JavaScript functions at execution time.

## Memory Management

Molniya uses a chunk-based memory model:

- Data is processed in chunks (default ~64K rows)
- Chunks are pooled and reused when possible
- Streaming operations recycle chunks as they go

For most users, this is transparent. But if you're processing extremely large files:

```typescript
// The pipeline automatically manages memory
const result = await readCsv("huge.csv", schema)
  .filter(col("active").eq(true))   // Chunks filtered and recycled
  .select("id", "name")              // Only needed columns kept
  .toArray();                        // Materializes result
```

## Error Handling

Molniya uses a Result type internally but throws errors for user-facing APIs:

```typescript
try {
  const df = await readCsv("missing.csv", schema);
  await df.show();
} catch (error) {
  // Error will have a descriptive message
  console.error(error.message);
}
```

Common errors:
- Column not found
- Type mismatch in operations
- File not found or unreadable
- Invalid expression

## Next Steps

Now that you understand the core concepts:

- Learn about [Data Types](./data-types) in detail
- See practical patterns in the [Cookbook](../cookbook/)
- Browse the [API Reference](../api/)
