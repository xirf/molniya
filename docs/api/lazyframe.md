# LazyFrame

LazyFrame builds query plans that execute optimally. Use it for large files or when you want automatic query optimization.

## Overview

LazyFrame records operations as a plan instead of executing immediately. When you call `collect()`, it optimizes and executes the plan.

**Benefits:**

- Predicate pushdown (filters during CSV parsing)
- Column pruning (only load needed columns)
- Query fusion (combine operations)
- Memory efficiency (process in chunks)

## Creating LazyFrames

### scanCsv

Create a LazyFrame from a CSV file path.

```typescript
import { LazyFrame, DType } from "molniya";

const query = LazyFrame.scanCsv("sales.csv", {
  product: DType.String,
  category: DType.String,
  revenue: DType.Float64,
});
```

**Parameters:**

- `path: string` - File path to CSV
- `schema: Record<string, DType>` - Column types
- `options?: Object` - Optional configuration
  - `chunkSize?: number` - Rows per chunk (default: 10000)
  - `delimiter?: string` - CSV delimiter (default: ",")
  - `hasHeader?: boolean` - Has header row (default: true)
  - `nullValues?: string[]` - Strings to treat as null

**Returns:** `LazyFrame` with scan operation

**Example:**

```typescript
const query = LazyFrame.scanCsv("data.tsv", schema, {
  delimiter: "\t",
  chunkSize: 5000,
  nullValues: ["NA", "NULL"],
});
```

## Query Building

### filter

Add a filter to the query plan.

```typescript
const query = LazyFrame.scanCsv("data.csv", schema)
  .filter("age", ">", 25)
  .filter("status", "==", "active");
```

**Parameters:**

- `column: string` - Column to filter
- `operator: FilterOperator` - Comparison operator
- `value: any` - Value to compare

**FilterOperator values:**

- `"=="`, `"!="`, `">"`, `">="`, `"<"`, `"<="`
- `"in"` - Value in array
- `"contains"`, `"startsWith"`, `"endsWith"` - String operations

**Returns:** `LazyFrame` with added filter

**Optimization:** Filters are pushed down to the scan operation when possible.

### select

Select specific columns.

```typescript
const query = LazyFrame.scanCsv("data.csv", schema).select([
  "id",
  "name",
  "value",
]);
```

**Parameters:**

- `columns: string[]` - Columns to select

**Returns:** `LazyFrame` with column selection

**Optimization:** Only selected columns are loaded from CSV.

### groupby

Group by columns and aggregate.

```typescript
const query = LazyFrame.scanCsv("sales.csv", schema).groupby(
  ["category"],
  [{ column: "revenue", agg: "sum", alias: "total" }],
);
```

**Parameters:**

- `groupKeys: string[]` - Columns to group by
- `aggregations: AggSpec[]` - Aggregation specifications

**AggSpec:**

```typescript
interface AggSpec {
  column: string; // Column to aggregate
  agg: "sum" | "mean" | "min" | "max" | "count";
  alias?: string; // Output column name
}
```

**Returns:** `LazyFrame` with groupby operation

## Execution

### collect

Execute the query plan and return a DataFrame.

```typescript
const result = await query.collect();

if (result.ok) {
  const df = result.data;
  console.log(df.toString());
} else {
  console.error("Query failed:", result.error.message);
}
```

**Returns:** `Promise<Result<DataFrame, Error>>`

**What happens:**

1. Query plan is optimized
2. Filters are pushed down to scan
3. Columns are pruned
4. Operations are fused when possible
5. Data is loaded and processed
6. DataFrame is returned

## Inspection

### getPlan

Get the internal query plan.

```typescript
const plan = query.getPlan();
console.log(plan);
```

**Returns:** `PlanNode` - Internal plan structure

### getSchema

Get the expected output schema.

```typescript
const schema = query.getSchema();
// { id: DType.Int32, name: DType.String }
```

**Returns:** `Record<string, DType>` - Output schema

### getColumnOrder

Get expected column order.

```typescript
const columns = query.getColumnOrder();
// ["id", "name"]
```

**Returns:** `string[]` - Column names in order

### explain

Get human-readable query plan.

```typescript
console.log(query.explain());
```

**Output example:**

```
ScanCSV(data.csv)
  Filter: category == "Electronics"
  Filter: revenue > 1000
  Select: [product, revenue]
```

**Returns:** `string` - Formatted plan description

## Optimization Examples

### Predicate Pushdown

Filters are applied during CSV parsing:

```typescript
// Inefficient: Load everything, then filter
const df = await scanCsv("huge.csv", { schema });
const filtered = df.data.filter("status", "==", "active");

// Optimized: Filter during CSV scan
const result = await LazyFrame.scanCsv("huge.csv", schema)
  .filter("status", "==", "active")
  .collect();
```

**Impact:** Optimizes performance for selective filters.

### Column Pruning

Only requested columns are loaded:

```typescript
// Inefficient: Load all 50 columns
const df = await scanCsv("wide.csv", { schema });
const subset = df.data.select(["col1", "col2"]);

// Optimized: Load only 2 columns
const result = await LazyFrame.scanCsv("wide.csv", schema)
  .select(["col1", "col2"])
  .collect();
```

**Impact:** Improves performance depending on column count.

### Query Fusion

Multiple operations are combined:

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .filter("age", ">", 18)
  .filter("age", "<", 65)
  // Both filters execute in one pass
  .collect();
```

## Complete Examples

### Large file filtering

```typescript
const query = LazyFrame.scanCsv("transactions.csv", {
  date: DType.Datetime,
  amount: DType.Float64,
  category: DType.String,
})
  .filter("category", "==", "Electronics")
  .filter("amount", ">", 100)
  .select(["date", "amount"]);

const result = await query.collect();

if (result.ok) {
  console.log(`Found ${result.data.shape[0]} matching transactions`);
}
```

### Groupby aggregation

```typescript
const query = LazyFrame.scanCsv("sales.csv", schema)
  .filter("year", "==", 2024)
  .groupby(
    ["category", "region"],
    [
      { column: "revenue", agg: "sum", alias: "total_revenue" },
      { column: "quantity", agg: "sum", alias: "total_quantity" },
    ],
  );

const result = await query.collect();
```

### Complex pipeline

```typescript
const query = LazyFrame.scanCsv("users.csv", schema)
  .filter("country", "==", "US")
  .filter("age", ">=", 18)
  .filter("email", "endsWith", "@company.com")
  .select(["id", "name", "email", "age"])
  .collect();
```

## Performance Tips

**1. Filter early**

```typescript
// Good: Filter first
const q = LazyFrame.scanCsv("data.csv", schema)
  .filter("status", "==", "active")
  .select(["id", "name"]);
```

**2. Select minimal columns**

```typescript
// Good: Only load what you need
const q = LazyFrame.scanCsv("data.csv", schema)
  .select(["id", "value"])
  .filter("value", ">", 0);
```

**3. Use appropriate chunk size**

```typescript
// For memory-constrained environments
const q = LazyFrame.scanCsv("huge.csv", schema, {
  chunkSize: 5000, // Smaller chunks
});

// For high-memory systems
const q = LazyFrame.scanCsv("data.csv", schema, {
  chunkSize: 50000, // Larger chunks
});
```

## Limitations

LazyFrame doesn't optimize everything:

**Not optimized:**

- Sort operations (require full data)
- Complex value transformations
- String regex operations

**Workaround:** Use LazyFrame for loading and filtering, then use DataFrame for complex operations:

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .filter("category", "==", "Electronics")
  .select(["product", "price"])
  .collect();

if (result.ok) {
  const sorted = result.data.sortBy("price", false);
  const top10 = sorted.head(10);
}
```

## See Also

- [DataFrame API](./dataframe.md) - For eager operations
- [Lazy Evaluation Guide](/guide/lazy-evaluation.md) - Concepts and benchmarks
- [CSV Reading](./csv-reading.md) - scanCsv options
