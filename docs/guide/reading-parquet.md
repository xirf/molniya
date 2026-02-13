# Reading Parquet Files

Parquet is a columnar storage format optimized for analytics. Molniya provides **built-in native support** for reading Parquet files—no external dependencies required.

## Built-In Parquet Reader

Molniya includes a custom Parquet implementation that:
- ✅ **Zero Dependencies**: No hyparquet or other libraries needed
- ✅ **Streaming Architecture**: Reads row groups incrementally for low memory usage
- ✅ **Optimized Conversions**: Single-allocation type conversions (50% faster than naive approach)
- ✅ **Full Format Support**: Dictionary encoding, compression (Snappy), all Parquet types
- ✅ **Predicate Pushdown**: Skip row groups using column statistics

## Basic Usage

### readParquet()

Read a Parquet file into a DataFrame:

```typescript
import { readParquet, DType } from "molniya";

const df = await readParquet("data.parquet", {
  id: DType.int32,
  name: DType.string,
  amount: DType.float64,
  created_at: DType.timestamp
});

// Execute an action to materialize
await df.show();
```

## Schema Definition

Parquet files contain type metadata, but Molniya requires an explicit schema for type safety:

```typescript
const schema = {
  // Integer types
  user_id: DType.int64,
  age: DType.int32,
  
  // Float types
  price: DType.float64,
  rating: DType.float32,
  
  // String type
  category: DType.string,
  
  // Date/Time types
  birth_date: DType.date,
  created_at: DType.timestamp,
  
  // Boolean type
  is_active: DType.boolean
};

const df = await readParquet("users.parquet", schema);
```

### Schema Mapping

Parquet types are mapped to Molniya types:

| Parquet Type        | Molniya Type | Notes                   |
| ------------------- | ------------ | ----------------------- |
| `INT32`             | `int32`      | 32-bit signed integer   |
| `INT64`             | `int64`      | 64-bit signed integer   |
| `FLOAT`             | `float32`    | Single precision float  |
| `DOUBLE`            | `float64`    | Double precision float  |
| `BYTE_ARRAY`        | `string`     | Dictionary-encoded      |
| `BOOLEAN`           | `boolean`    | True/false values       |
| `INT96`             | `timestamp`  | Legacy timestamp format |
| `INT64 (timestamp)` | `timestamp`  | Modern timestamp format |

## Reading Options

### Column Projection

Read only specific columns to improve performance:

```typescript
const df = await readParquet("large_file.parquet", schema, {
  projection: ["id", "name", "amount"]  // Only read these columns
});
```

### Row Filtering

Filter rows during reading (predicate pushdown):

```typescript
import { col } from "molniya";

const df = await readParquet("data.parquet", schema, {
  filter: col("year").eq(2024)  // Only read 2024 data
});
```

::: tip Performance
Predicate pushdown filters data while reading, reducing I/O and memory usage.
:::

## Streaming Architecture

### Row Group Streaming

Parquet files are read in row groups (typically 64MB each) for memory-efficient streaming:

```typescript
// Process large files without loading everything into memory
const df = await readParquet("huge_dataset.parquet", schema);

// Stream through chunks
for await (const chunk of df.toChunks()) {
  // Process each chunk
  console.log(`Processed ${chunk.length} rows`);
}
```

### Memory Efficiency

**Memory usage comparison** for a 10GB Parquet file:
- Traditional approach: ~40GB RAM (full decompression)
- Molniya streaming: ~2.5GB RAM (one row group at a time)

```typescript
// Large file analysis with minimal memory
const result = await readParquet("100gb_file.parquet", schema)
  .filter(col("year").eq(2024))
  .groupBy("category", [
    { name: "total", expr: sum("amount") }
  ])
  .toArray();  // Only final results in memory
```

## Working with Nested Data

Parquet supports nested structures. Molniya flattens nested columns:

```typescript
// Parquet with nested structure: { user: { id, name }, orders: [...] }
// Becomes: user.id, user.name, orders (as JSON string)

const schema = {
  "user.id": DType.int32,
  "user.name": DType.string,
  "orders": DType.string  // JSON string
};

const df = await readParquet("nested.parquet", schema);
```

## Date and Timestamp Handling

### Reading Dates

```typescript
const schema = {
  // Date stored as int32 (days since epoch)
  birth_date: DType.date,
  
  // Timestamp stored as int64 (microseconds/nanoseconds since epoch)
  created_at: DType.timestamp
};

const df = await readParquet("events.parquet", schema);
```

### Timestamp Precision

Parquet stores timestamps with different precisions:

```typescript
// Molniya converts all timestamps to milliseconds
const df = await readParquet("data.parquet", {
  // Parquet INT64 with MILLIS precision
  event_time_ms: DType.timestamp,
  
  // Parquet INT64 with MICROS precision (converted to ms)
  event_time_us: DType.timestamp,
  
  // Parquet INT64 with NANOS precision (converted to ms)
  event_time_ns: DType.timestamp
});
```

## Nullable Columns

Handle nullable columns in Parquet:

```typescript
const schema = {
  // Required column
  id: DType.int32,
  
  // Optional columns (may contain nulls)
  middle_name: DType.nullable.string,
  deleted_at: DType.nullable.timestamp
};

const df = await readParquet("users.parquet", schema);
```

## Reading Multiple Files

Read and combine multiple Parquet files:

```typescript
import { readParquet, union } from "molniya";

// Read multiple files
const df1 = await readParquet("data_2023.parquet", schema);
const df2 = await readParquet("data_2024.parquet", schema);

// Union them together
const combined = df1.union(df2);
```

## Partitioned Datasets

Read Hive-style partitioned Parquet datasets:

```typescript
// Directory structure: data/year=2024/month=01/data.parquet
const df = await readParquet("data/year=2024/month=01/data.parquet", schema);

// With partition columns included
const dfWithPartitions = df
  .withColumn("year", lit(2024))
  .withColumn("month", lit(1));
```

## Error Handling

Handle common Parquet reading errors:

```typescript
try {
  const df = await readParquet("corrupted.parquet", schema);
} catch (error) {
  if (error.message.includes("Invalid Parquet")) {
    console.error("File is not valid Parquet format");
  } else if (error.message.includes("Schema mismatch")) {
    console.error("Schema doesn't match file contents");
  }
}
```

## Parquet vs CSV

When to use Parquet over CSV:

| Aspect             | Parquet           | CSV                  |
| ------------------ | ----------------- | -------------------- |
| **Size**           | 50-75% smaller    | Raw text             |
| **Speed**          | Much faster reads | Slower parsing       |
| **Types**          | Preserved         | Inferred             |
| **Compression**    | Built-in          | None (unless zipped) |
| **Query perf**     | Column pruning    | Full scan            |
| **Human readable** | No                | Yes                  |

## Performance Characteristics

### Optimized Type Conversions

Molniya's Parquet reader uses **single-allocation patterns** for all type conversions:

```typescript
// Optimized: Single allocation for DATE conversion
const result = new Array(data.length);
for (let i = 0; i < data.length; i++) {
  result[i] = daysToDate(data[i]);
}
```

**Impact**: 50% reduction in memory allocations during ingestion compared to naive `Array.from().map()` approach.

Applied to: DECIMAL, INT96, DATE, TIMESTAMP_MILLIS, TIMESTAMP_MICROS conversions.

### Predicate Pushdown

Skip entire row groups using column statistics:

```typescript
// Only reads row groups where max(age) >= 50
const df = await readParquet("users.parquet", schema, {
  filter: col("age").gte(50)
});
```

**Performance**: Can skip 70-90% of data for selective queries.

## Advanced: Offloading and Caching

### When to Cache

For **repeated queries on the same file**, consider caching to avoid repeated decompression:

```typescript
// Future feature: Automatic caching
const df = await readParquet("sales.parquet", schema, {
  cache: true  // Converts to .mbf format on first read
});

// First read: 20s (decompress + convert)
// Subsequent reads: 5s (stream from cache)
```

### MBF Offloading Strategy

**Use cases for .mbf conversion**:
1. **Production dashboards**: Same file queried hourly/daily
2. **Network-limited**: Slow download, many local reads
3. **CPU-bound**: Decompression is bottleneck

**When NOT to use caching**:
- One-time analysis or exploration
- Frequently updated data
- Storage-constrained environments

### Break-Even Analysis

```
Parquet direct:    20s × 10 reads = 200s total
With .mbf cache:   30s convert + (5s × 9 reads) = 75s total

Break-even: 3-5 reads depending on file size
```

::: tip Future Feature
Automatic .mbf caching is planned but not yet implemented.
:::

## Best Practices

1. **Use Parquet for analytics**: 50-75% smaller than CSV, much faster reads
2. **Define explicit schemas**: Type safety and early error detection
3. **Use column projection**: Read only needed columns (reduces I/O)
4. **Enable predicate pushdown**: Filter during reading (skip row groups)
5. **Leverage streaming**: Process incrementally for large files (low memory)
6. **Consider caching**: For repeated queries (3+ reads) on same file

## Complete Example

```typescript
import { readParquet, DType, col, sum, avg, desc } from "molniya";

const schema = {
  user_id: DType.int64,
  event_type: DType.string,
  amount: DType.nullable.float64,
  event_time: DType.timestamp
};

// Read with filtering
const df = await readParquet("events.parquet", schema, {
  projection: ["user_id", "event_type", "amount"],
  filter: col("event_time").gte(new Date("2024-01-01"))
});

// Analyze
const summary = await df
  .filter(col("amount").isNotNull())
  .groupBy("event_type", [
    { name: "total", expr: sum("amount") },
    { name: "average", expr: avg("amount") },
    { name: "count", expr: count() }
  ])
  .sort(desc("total"))
  .toArray();

console.log(summary);
```
