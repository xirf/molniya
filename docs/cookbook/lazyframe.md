# LazyFrame Recipes

LazyFrame builds a query plan instead of loading data immediately. When you call `.collect()`, it executes the optimized plan.

## Why LazyFrame?

LazyFrame is **significantly more efficient** for large files because of three optimizations:

**Predicate pushdown:** Filters are applied while reading the CSV, so you never load rows you'll filter out anyway.

**Column pruning:** Only requested columns are loaded from disk. If your CSV has 50 columns but you only need 3, you save 94% of I/O time.

**Chunked processing:** Data is processed in chunks (default 10,000 rows), so you can handle files larger than available memory.

**Rule of thumb:** File > 10MB? Use LazyFrame!

## Basic Pattern

LazyFrame uses a three-step pattern: scan → transform → collect.

**Important:** Nothing is executed until you call `.collect()`! This lets Molniya optimize the entire query.

```typescript
import { LazyFrame, DType } from "molniya";

// 1. Scan (build query plan - no data loaded yet)
const query = LazyFrame.scanCsv("huge.csv", {
  id: DType.Int32,
  name: DType.String,
  value: DType.Float64,
});

// 2. Add operations (still just planning)
const filtered = query.filter("value", ">", 100).select(["id", "name"]);

// 3. Execute (optimized)
const result = await filtered.collect();

if (result.ok) {
  const df = result.data; // Now you have a DataFrame
}
```

## Filtering During Scan

### Single filter

```typescript
const query = LazyFrame.scanCsv("data.csv", schema)
  .filter("status", "==", "active")
  .collect();

// Only loads active rows!
```

### Multiple filters (super fast)

```typescript
const query = LazyFrame.scanCsv("transactions.csv", schema)
  .filter("date", ">=", "2024-01-01")
  .filter("amount", ">", 1000)
  .filter("category", "==", "Electronics")
  .collect();

// All filters applied during CSV scan
```

### String filters

```typescript
const query = LazyFrame.scanCsv("users.csv", schema)
  .filter("email", "endsWith", "@company.com")
  .filter("name", "startsWith", "A")
  .collect();
```

## Column Pruning

### Load only needed columns

```typescript
// CSV has 50 columns, we only need 3
const query = LazyFrame.scanCsv("wide.csv", schema)
  .select(["id", "name", "revenue"])
  .collect();

// Only loads 3 columns from disk!
```

### Filter then select

```typescript
const query = LazyFrame.scanCsv("data.csv", schema)
  .filter("active", "==", true) // Filter first
  .select(["id", "name", "score"]) // Then select
  .collect();

// Optimal: filter reduces rows, select reduces columns
```

## Common Patterns

### Top N from large file

```typescript
const query = LazyFrame.scanCsv("millions.csv", schema)
  .filter("country", "==", "US")
  .select(["id", "revenue"])
  .collect();

// Then sort in memory (small result)
if (result.ok) {
  const top10 = result.data.sortBy("revenue", false).head(10);
}
```

### Find specific records

```typescript
const query = LazyFrame.scanCsv("logs.csv", schema)
  .filter("level", "==", "ERROR")
  .filter("timestamp", ">=", "2024-01-01")
  .collect();
```

### Filter by array

```typescript
const query = LazyFrame.scanCsv("data.csv", schema)
  .filter("category", "in", ["A", "B", "C"])
  .collect();
```

## Performance Tricks

### Chunk size tuning

**High memory:**

```typescript
const query = LazyFrame.scanCsv("data.csv", schema, {
  chunkSize: 50000, // Large chunks
}).collect();
```

**Low memory (slower but safe):**

```typescript
const query = LazyFrame.scanCsv("data.csv", schema, {
  chunkSize: 1000, // Tiny chunks
}).collect();
```

**Default (balanced):**

```typescript
const query = LazyFrame.scanCsv("data.csv", schema, {
  chunkSize: 10000, // Good for most cases
}).collect();
```

### Filter as early as possible

```typescript
// ✅ Good: Filter first
const query = LazyFrame.scanCsv("data.csv", schema)
  .filter("status", "==", "active") // Reduce rows early
  .select(["id", "name"]) // Then columns
  .collect();

// ❌ Wasteful: Select first
const query = LazyFrame.scanCsv("data.csv", schema)
  .select(["id", "name", "status"]) // Load extra column
  .filter("status", "==", "active") // Filter later
  .collect();
```

### Combine filters

```typescript
// All filters execute in one pass during scan
const query = LazyFrame.scanCsv("data.csv", schema)
  .filter("age", ">=", 18)
  .filter("age", "<=", 65)
  .filter("country", "==", "US")
  .collect();
```

## CSV Options

### Custom delimiter

```typescript
const query = LazyFrame.scanCsv("data.tsv", schema, {
  delimiter: "\t",
}).collect();
```

### Handle nulls

```typescript
const query = LazyFrame.scanCsv("messy.csv", schema, {
  nullValues: ["NA", "NULL", "-", ""],
}).collect();
```

### Skip header rows

```typescript
const query = LazyFrame.scanCsv("export.csv", schema, {
  skipRows: 3, // Skip metadata
}).collect();
```

### No header row

```typescript
const query = LazyFrame.scanCsv("data.csv", schema, {
  hasHeader: false,
}).collect();
```

## From String Data

### Parse CSV string

```typescript
const csvData = `name,age,city
Alice,25,NYC
Bob,30,LA`;

const query = LazyFrame.scanCsvFromString(csvData, {
  name: DType.String,
  age: DType.Int32,
  city: DType.String,
});

const result = await query.collect();
```

### From API response

```typescript
const response = await fetch("/api/data.csv");
const csvText = await response.text();

const query = LazyFrame.scanCsvFromString(csvText, schema);
const result = await query.collect();
```

## Query Inspection

### View query plan

```typescript
const query = LazyFrame.scanCsv("data.csv", schema)
  .filter("status", "==", "active")
  .select(["id", "name"]);

console.log(query.explain());
// Shows what will execute
```

### Get schema

```typescript
const schema = query.getSchema();
console.log(schema);
// { id: DType.Int32, name: DType.String }
```

### Get columns

```typescript
const columns = query.getColumnOrder();
console.log(columns);
// ["id", "name"]
```

## Real Examples

### Process huge log file

```typescript
const query = LazyFrame.scanCsv(
  "app.log.csv",
  {
    timestamp: DType.Datetime,
    level: DType.String,
    message: DType.String,
    user_id: DType.Int32,
  },
  {
    chunkSize: 5000, // Low memory
  },
)
  .filter("level", "==", "ERROR")
  .filter("timestamp", ">=", "2024-01-01")
  .select(["timestamp", "message", "user_id"]);

const result = await query.collect();
```

### Extract subset from large dataset

```typescript
const query = LazyFrame.scanCsv("transactions.csv", {
  id: DType.Int64,
  date: DType.Datetime,
  amount: DType.Float64,
  category: DType.String,
  user_id: DType.Int64,
})
  .filter("date", ">=", "2024-01-01")
  .filter("amount", ">", 1000)
  .filter("category", "in", ["Electronics", "Computers"])
  .select(["id", "date", "amount", "user_id"]);

const result = await query.collect();

if (result.ok) {
  // Now work with smaller DataFrame
  const top = result.data.sortBy("amount", false).head(100);
}
```

### Memory-efficient aggregation

```typescript
const query = LazyFrame.scanCsv("sales.csv", schema, {
  chunkSize: 10000,
})
  .filter("year", "==", 2024)
  .select(["product", "revenue"])
  .collect();

if (result.ok) {
  // Aggregate the reduced dataset
  const totals = new Map();
  const products = result.data.get("product").toArray();
  const revenues = result.data.get("revenue").toArray();

  products.forEach((p, i) => {
    totals.set(p, (totals.get(p) || 0) + revenues[i]);
  });
}
```

## Hot Tips

**Always collect() at the end:**

```typescript
const result = await query.collect(); // Returns Result<DataFrame>
if (result.ok) {
  const df = result.data; // DataFrame
}
```

**Filter BEFORE select:**

```typescript
// ✅ Fast
query.filter("status", "==", "active").select(["id", "name"]);

// ❌ Slower
query.select(["id", "name", "status"]).filter("status", "==", "active");
```

**Use LazyFrame for big files only:**

```typescript
// File < 10MB? Use readCsv (simpler)
const small = await readCsv("small.csv", schema);

// File > 10MB? Use LazyFrame (more efficient)
const big = await LazyFrame.scanCsv("big.csv", schema)
  .filter("active", "==", true)
  .collect();
```

**Chain operations freely:**

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .filter("country", "==", "US")
  .filter("age", ">=", 18)
  .filter("revenue", ">", 1000)
  .select(["id", "name", "revenue"])
  .collect();
// All optimized automatically!
```
