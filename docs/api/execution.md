# Execution Methods

Methods that trigger computation and return results.

## Overview

Molniya uses lazy evaluation - transformations build a logical plan but don't execute until you call an action method.

## collect()

Materialize the DataFrame into memory.

```typescript
collect(): Promise<DataFrame<T>>
```

**Example:**

```typescript
const df = await readCsv("data.csv", schema);
const result = await df
  .filter(col("status").eq("active"))
  .collect();

// Result is a new DataFrame with materialized data
```

::: warning Memory Usage
`collect()` loads all data into memory. For large datasets, use `toArray()` with limits or process in chunks.
:::

## toArray()

Convert DataFrame to an array of plain JavaScript objects.

```typescript
toArray(): Promise<Record<string, unknown>[]>
```

**Example:**

```typescript
const rows = await df
  .filter(col("score").gt(90))
  .limit(100)
  .toArray();

// Returns: [{ id: 1, name: "Alice", score: 95 }, ...]
```

## toChunks()

Get the underlying chunks (advanced use).

```typescript
toChunks(): Promise<Chunk[]>
```

**Example:**

```typescript
const chunks = await df.toChunks();

for (const chunk of chunks) {
  console.log(`Chunk has ${chunk.rowCount} rows`);
}
```

## count()

Get the total number of rows.

```typescript
count(): Promise<number>
```

**Example:**

```typescript
const totalRows = await df.count();
const activeCount = await df.filter(col("active").eq(true)).count();
```

::: tip Streaming Count
When no operators are applied, `count()` streams through the data without fully materializing it.
:::

## show()

Print a formatted table of the first N rows.

```typescript
show(maxRows?: number): Promise<void>
```

**Example:**

```typescript
await df.show();       // Show first 10 rows (default)
await df.show(5);      // Show first 5 rows
await df.show(100);    // Show first 100 rows
```

**Output format:**

```
┌────┬─────────┬────────┐
│ id │ name    │ score  │
├────┼─────────┼────────┤
│ 1  │ Alice   │ 95     │
│ 2  │ Bob     │ 87     │
│ 3  │ Charlie │ 92     │
└────┴─────────┴────────┘

Showing first 3 rows
```

## isEmpty()

Check if the DataFrame has any rows.

```typescript
isEmpty(): Promise<boolean>
```

**Example:**

```typescript
if (await df.isEmpty()) {
  console.log("No data found");
}
```

## concat()

Vertically concatenate two DataFrames.

```typescript
concat(other: DataFrame<T>): Promise<DataFrame<T>>
```

**Example:**

```typescript
const january = await readCsv("jan.csv", schema);
const february = await readCsv("feb.csv", schema);

const combined = await january.concat(february);
```

::: warning Schema Compatibility
Both DataFrames must have the same schema. Use `select()` to ensure column order matches.
:::

## Execution Order

Understanding when execution happens:

```typescript
// 1. Create lazy DataFrame (no execution)
const df = await readCsv("data.csv", schema);

// 2. Add transformations (still no execution)
const filtered = df.filter(col("status").eq("active"));
const projected = filtered.select("id", "name");

// 3. Add more transformations (still lazy)
const limited = projected.limit(100);

// 4. ACTION: Execution happens here
const rows = await limited.toArray();
```

## Execution Flow

```
User Code
    ↓
Build Plan (filter → select → limit)
    ↓
Call Action (toArray)
    ↓
Optimize Plan
    ↓
Execute Pipeline
    ↓
Stream Chunks → Apply Operators → Collect Results
    ↓
Return to User
```

## Performance Considerations

### Choose the Right Action

| Method | Use When | Memory |
|--------|----------|--------|
| `collect()` | You need a DataFrame for more operations | High |
| `toArray()` | You need plain JS objects | High |
| `count()` | You only need the row count | Low |
| `show()` | Debugging / inspection | Low |
| `isEmpty()` | Checking for data existence | Low |

### Streaming vs Materialization

```typescript
// Streaming: Low memory, processes chunks as they arrive
await readCsv("huge.csv", schema)
  .filter(col("year").eq(2024))
  .select("id", "amount")
  .count();

// Materializing: Loads data into memory
await readCsv("huge.csv", schema)
  .sort("amount")  // Requires all data
  .toArray();
```

## Error Handling

Execution errors throw descriptive messages:

```typescript
try {
  await df.collect();
} catch (error) {
  // Possible errors:
  // - File not found during streaming
  // - Type conversion failure
  // - Out of memory
  console.error("Execution failed:", error.message);
}
```

## Best Practices

1. **Use `limit()` during development** - Don't process millions of rows while testing
2. **Filter early** - Reduces data volume before expensive operations
3. **Select only needed columns** - Reduces memory usage
4. **Use `count()` for existence checks** - More efficient than `toArray()`

```typescript
// Good: Efficient development workflow
const result = await df
  .filter(col("status").eq("active"))
  .limit(1000)  // Limit during development
  .toArray();

// Production: Remove limit
const result = await df
  .filter(col("status").eq("active"))
  .toArray();
```
