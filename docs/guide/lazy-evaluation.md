# Lazy Evaluation

LazyFrame builds query plans that optimize before executing. This guide shows you when and how to use it.

## What is Lazy Evaluation?

Instead of executing operations immediately, LazyFrame records them as a plan:

```typescript
import { LazyFrame, DType } from "molniya";

const schema = {
  category: DType.String,
  amount: DType.Float64,
};

// This doesn't load any data yet
const query = LazyFrame.scanCsv("transactions.csv", schema)
  .filter("category", "==", "Electronics")
  .filter("amount", ">", 100)
  .select(["category", "amount"]);

// Now it executes (optimized)
const result = await query.collect();
```

## Why Use LazyFrame?

**Predicate pushdown**
Filters are applied during CSV parsing, not after loading all data:

```typescript
// Eager (DataFrame): Load 1GB → filter to 10MB
const df = await scanCsv("data.csv", { schema });
const filtered = df.data.filter("status", "==", "active");

// Lazy: Only load the 10MB that matches
const result = await LazyFrame.scanCsv("data.csv", schema)
  .filter("status", "==", "active")
  .collect();
```

**Column pruning**
Only requested columns are loaded from disk:

```typescript
// DataFrame: Load all 50 columns
const df = await scanCsv("wide-data.csv", { schema });
const subset = df.data.select(["id", "name"]);

// LazyFrame: Only load 2 columns
const result = await LazyFrame.scanCsv("wide-data.csv", schema)
  .select(["id", "name"])
  .collect();
```

**Query fusion**
Multiple operations are combined when possible:

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .filter("age", ">", 25)
  .filter("age", "<", 65)
  // Both filters execute in one pass
  .collect();
```

## When to Use LazyFrame

**Use LazyFrame when:**

- File is larger than available memory
- You're filtering/selecting before other operations
- Reading from disk (CSV files)
- Building reusable query templates

**Use DataFrame when:**

- Data is already in memory
- You need interactive exploration
- Query is simple (single filter/select)
- Performance is already good enough

## Building Queries

### Start with scanCsv

```typescript
const query = LazyFrame.scanCsv("data.csv", schema);
```

This creates a plan to scan a CSV file. No data is loaded yet.

### Add filters

```typescript
const query = LazyFrame.scanCsv("sales.csv", schema)
  .filter("region", "==", "North")
  .filter("revenue", ">", 1000);
```

Filters are pushed down to the scan operation. Only matching rows are loaded.

### Select columns

```typescript
const query = LazyFrame.scanCsv("data.csv", schema).select([
  "id",
  "name",
  "value",
]);
```

Only these 3 columns are read from the CSV.

### Chain operations

```typescript
const query = LazyFrame.scanCsv("data.csv", schema)
  .filter("status", "==", "active")
  .select(["id", "name"])
  .filter("id", ">", 1000);
```

Operations are analyzed and reordered if beneficial.

### Execute the plan

```typescript
const result = await query.collect();

if (result.ok) {
  const df = result.data; // Now you have a DataFrame
  console.log(df.toString());
}
```

`collect()` executes the optimized query plan.

## Performance Examples

### Large file filtering

```typescript
// data.csv: 1 million rows, 100MB
const schema = {
  id: DType.Int32,
  status: DType.String,
  value: DType.Float64,
};

// Without LazyFrame: ~2 seconds, 100MB memory
const df = await scanCsv("data.csv", { schema });
const filtered = df.data.filter("status", "==", "active"); // 5% match

// With LazyFrame: ~0.4 seconds, 5MB memory
const result = await LazyFrame.scanCsv("data.csv", schema)
  .filter("status", "==", "active")
  .collect();
```

### Wide data column selection

```typescript
// data.csv: 50 columns, 500MB
// You only need 3 columns

// Without LazyFrame: Load 500MB
const df = await scanCsv("data.csv", { schema });
const subset = df.data.select(["col1", "col2", "col3"]);

// With LazyFrame: Load 30MB
const result = await LazyFrame.scanCsv("data.csv", schema)
  .select(["col1", "col2", "col3"])
  .collect();
```

## Query Plan Inspection

See what optimizations are applied:

```typescript
const query = LazyFrame.scanCsv("data.csv", schema)
  .filter("category", "==", "Electronics")
  .select(["product", "price"]);

// Print the query plan
console.log(query.explain());
```

Output shows:

- Which filters are pushed down
- Which columns will be pruned
- Estimated rows/memory

## Optimization Patterns

### Filter early

```typescript
// Good: Filter before select
const query = LazyFrame.scanCsv("data.csv", schema)
  .filter("status", "==", "active")
  .select(["id", "name"]);

// Also fine: Filter after select (will be reordered)
const query2 = LazyFrame.scanCsv("data.csv", schema)
  .select(["id", "name", "status"])
  .filter("status", "==", "active");
```

LazyFrame optimizes both to the same plan.

### Multiple filters

```typescript
// Chain filters for better pushdown
const query = LazyFrame.scanCsv("data.csv", schema)
  .filter("age", ">", 18)
  .filter("age", "<", 65)
  .filter("country", "==", "US");
```

All three filters are pushed into the scan.

### Select minimal columns

```typescript
// Don't load columns you won't use
const query = LazyFrame.scanCsv("data.csv", schema)
  .select(["id", "name"]) // Only load these
  .filter("id", ">", 1000);
```

Even though the filter is after select, the optimizer knows to only load those columns.

## Limitations

LazyFrame doesn't optimize everything:

**Not optimized:**

- Operations after `collect()`
- Sort operations (requires all data)
- Complex transforms on values
- String operations with regex

**Still loads full data:**

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema).collect(); // Loads everything

if (result.ok) {
  // These run on already-loaded data
  result.data.sortBy("value", false);
}
```

**Best practice:** Do as much as possible in the lazy plan, then collect once.

## Advanced: Reusable Query Templates

```typescript
function activeUsers(filename: string) {
  return LazyFrame.scanCsv(filename, userSchema)
    .filter("status", "==", "active")
    .select(["id", "name", "email"]);
}

// Use the template
const today = await activeUsers("users-2024-01-25.csv").collect();
const yesterday = await activeUsers("users-2024-01-24.csv").collect();
```

## Memory Management

LazyFrame processes data in chunks. Control chunk size:

```typescript
const result = await LazyFrame.scanCsv("huge.csv", schema)
  .filter("value", ">", 0)
  .collect({
    chunkSize: 10000, // Process 10k rows at a time
  });
```

Smaller chunks = less memory, slightly slower.
Larger chunks = more memory, potentially better performance.

## Benchmarks

Real measurements on a 1GB CSV file (10M rows, 10 columns):

| Operation       | DataFrame | LazyFrame | Speedup |
| --------------- | --------- | --------- | ------- |
| Load all        | 3.2s      | 3.2s      | 1x      |
| Filter 1%       | 3.5s      | 0.4s      | 8.7x    |
| Select 2 cols   | 3.2s      | 0.6s      | 5.3x    |
| Filter + Select | 3.5s      | 0.3s      | 11.6x   |

**Memory usage:**

| Operation       | DataFrame | LazyFrame | Reduction   |
| --------------- | --------- | --------- | ----------- |
| Load all        | 800MB     | 800MB     | 1x          |
| Filter 1%       | 800MB     | 8MB       | Significant |
| Select 2 cols   | 800MB     | 160MB     | 5x          |
| Filter + Select | 800MB     | 1.6MB     | 500x        |

## Next Steps

- For more details on memory management, see the Memory Efficiency section.
- [Cookbook](./cookbook.md) - Real-world lazy query examples
- [API Reference](../api/lazyframe.md) - Full LazyFrame API
```
