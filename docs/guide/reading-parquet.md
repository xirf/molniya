# Reading Parquet Files

Parquet is a columnar storage format optimized for analytics. Molniya provides native support for reading Parquet files with schema validation.

## Basic Usage

### readParquet()

Read a Parquet file into a DataFrame:

```typescript
import { readParquet, DType } from "Molniya";

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

| Parquet Type | Molniya Type | Notes |
|--------------|--------------|-------|
| `INT32` | `int32` | 32-bit signed integer |
| `INT64` | `int64` | 64-bit signed integer |
| `FLOAT` | `float32` | Single precision float |
| `DOUBLE` | `float64` | Double precision float |
| `BYTE_ARRAY` | `string` | Dictionary-encoded |
| `BOOLEAN` | `boolean` | True/false values |
| `INT96` | `timestamp` | Legacy timestamp format |
| `INT64 (timestamp)` | `timestamp` | Modern timestamp format |

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
import { col } from "Molniya";

const df = await readParquet("data.parquet", schema, {
  filter: col("year").eq(2024)  // Only read 2024 data
});
```

::: tip Performance
Predicate pushdown filters data while reading, reducing I/O and memory usage.
:::

## Streaming Reads

Parquet files are read in row groups for efficient streaming:

```typescript
// Process large files without loading everything into memory
const df = await readParquet("huge_dataset.parquet", schema);

// Stream through chunks
for await (const chunk of df.toChunks()) {
  // Process each chunk
  console.log(`Processed ${chunk.length} rows`);
}
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
import { readParquet, union } from "Molniya";

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

| Aspect | Parquet | CSV |
|--------|---------|-----|
| **Size** | 50-75% smaller | Raw text |
| **Speed** | Much faster reads | Slower parsing |
| **Types** | Preserved | Inferred |
| **Compression** | Built-in | None (unless zipped) |
| **Query perf** | Column pruning | Full scan |
| **Human readable** | No | Yes |

## Best Practices

1. **Use Parquet for analytics**: Better performance for analytical workloads
2. **Define explicit schemas**: Ensures type safety and catches errors early
3. **Use column projection**: Only read columns you need
4. **Enable predicate pushdown**: Filter during reading when possible
5. **Partition large datasets**: Organize data by frequently filtered columns

## Complete Example

```typescript
import { readParquet, DType, col, sum, avg, desc } from "Molniya";

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
