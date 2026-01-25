# CSV Reading

Functions for loading CSV files into DataFrames and LazyFrames.

## Overview

Molniya provides four CSV reading functions:

- **`scanCsv`** - Lazy loading with optimization (best for large files)
- **`scanCsvFromString`** - Lazy loading from string data
- **`readCsv`** - Eager loading (best for small files)
- **`readCsvFromString`** - Eager loading from string data

## Lazy Reading (Recommended)

### scanCsv

Create a LazyFrame from a CSV file. Operations are optimized before execution.

```typescript
import { LazyFrame, DType } from "molniya";

const query = LazyFrame.scanCsv("sales.csv", {
  date: DType.Datetime,
  product: DType.String,
  quantity: DType.Int32,
  revenue: DType.Float64,
});

const result = await query
  .filter("quantity", ">", 100)
  .select(["product", "revenue"])
  .collect();
```

**Parameters:**

- `path: string` - CSV file path
- `schema: Record<string, DType>` - Column types
- `options?: CsvOptions` - Optional configuration

**Returns:** `LazyFrame`

**Benefits:**

- Predicate pushdown (filters during scan)
- Column pruning (only load needed columns)
- Memory efficient (chunked processing)

### scanCsvFromString

Create a LazyFrame from CSV string data.

```typescript
const csvData = `name,age,city
Alice,25,NYC
Bob,30,LA`;

const query = LazyFrame.scanCsvFromString(csvData, {
  name: DType.String,
  age: DType.Int32,
  city: DType.String,
});
```

**Parameters:**

- `data: string` - CSV content
- `schema: Record<string, DType>` - Column types
- `options?: CsvOptions` - Optional configuration

**Returns:** `LazyFrame`

## Eager Reading

### readCsv

Load CSV file immediately into a DataFrame.

```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("data.csv", {
  id: DType.Int32,
  name: DType.String,
  value: DType.Float64,
});

if (result.ok) {
  const df = result.data;
  console.log(df.toString());
} else {
  console.error("Failed to load:", result.error.message);
}
```

**Parameters:**

- `path: string` - CSV file path
- `schema: Record<string, DType>` - Column types
- `options?: CsvOptions` - Optional configuration

**Returns:** `Promise<Result<DataFrame, Error>>`

**Use when:**

- File is small (< 10MB)
- Need immediate access to data
- Performing complex operations not optimized by LazyFrame

### readCsvFromString

Load CSV from string into a DataFrame.

```typescript
const csvData = `x,y
1,2
3,4`;

const result = await readCsvFromString(csvData, {
  x: DType.Int32,
  y: DType.Int32,
});
```

**Parameters:**

- `data: string` - CSV content
- `schema: Record<string, DType>` - Column types
- `options?: CsvOptions` - Optional configuration

**Returns:** `Promise<Result<DataFrame, Error>>`

## CSV Options

All CSV reading functions accept optional configuration:

```typescript
interface CsvOptions {
  delimiter?: string; // Column separator (default: ",")
  hasHeader?: boolean; // First row is header (default: true)
  nullValues?: string[]; // Strings treated as null (default: [""])
  chunkSize?: number; // Rows per chunk for lazy (default: 10000)
  skipRows?: number; // Skip N rows at start (default: 0)
  comment?: string; // Skip lines starting with this
  quoteChar?: string; // Quote character (default: '"')
  escapeChar?: string; // Escape character (default: '"')
}
```

### delimiter

Column separator character.

```typescript
// Tab-separated values
const result = await readCsv("data.tsv", schema, {
  delimiter: "\t",
});

// Pipe-separated
const result = await readCsv("data.psv", schema, {
  delimiter: "|",
});
```

### hasHeader

Whether first row contains column names.

```typescript
// File without header row
const result = await readCsv("data.csv", schema, {
  hasHeader: false,
});
```

**Note:** Column names still come from schema keys. This only affects whether to skip the first row.

### nullValues

Strings to interpret as null.

```typescript
const result = await readCsv("data.csv", schema, {
  nullValues: ["NA", "NULL", "n/a", ""],
});
```

**Default:** `[""]` (empty strings are null)

### chunkSize

For lazy reading, number of rows to process per chunk.

```typescript
// Smaller chunks for memory-constrained environments
const query = LazyFrame.scanCsv("huge.csv", schema, {
  chunkSize: 5000,
});

// Larger chunks for better performance
const query = LazyFrame.scanCsv("data.csv", schema, {
  chunkSize: 50000,
});
```

**Default:** 10000

**Trade-offs:**

- Smaller: Lower memory, more overhead
- Larger: Higher memory, less overhead

### skipRows

Skip N rows at the start of the file.

```typescript
// Skip first 3 rows (e.g., file metadata)
const result = await readCsv("data.csv", schema, {
  skipRows: 3,
});
```

### comment

Skip lines starting with this character.

```typescript
// Skip comment lines
const result = await readCsv("data.csv", schema, {
  comment: "#",
});
```

**Example data.csv:**

```csv
# This is a comment
# Another comment
name,age
Alice,25
# Inline comment (also skipped)
Bob,30
```

### quoteChar

Character used for quoting fields.

```typescript
const result = await readCsv("data.csv", schema, {
  quoteChar: "'", // Single quotes instead of double
});
```

**Default:** `"` (double quote)

**Example:**

```csv
name,description
Alice,'Product "A"'
Bob,'Item with, comma'
```

### escapeChar

Character used to escape quotes within quoted fields.

```typescript
const result = await readCsv("data.csv", schema, {
  escapeChar: "\\",
});
```

**Default:** `"` (double quote)

**Example with default:**

```csv
name,quote
Alice,"She said ""Hello"""
```

**Example with backslash:**

```csv
name,quote
Alice,"She said \"Hello\""
```

## Schema Definition

Schema maps column names to data types:

```typescript
const schema = {
  id: DType.Int32,
  name: DType.String,
  created_at: DType.Datetime,
  price: DType.Float64,
  active: DType.Boolean,
};
```

**Available types:**

- `DType.Int32` - 32-bit integers
- `DType.Int64` - 64-bit integers (as BigInt)
- `DType.Float64` - Double precision floats
- `DType.String` - UTF-8 strings
- `DType.Boolean` - true/false
- `DType.Datetime` - ISO 8601 dates

See [DType Reference](./dtype.md) for details.

## Complete Examples

### Basic eager loading

```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("users.csv", {
  id: DType.Int32,
  name: DType.String,
  email: DType.String,
  age: DType.Int32,
});

if (result.ok) {
  console.log(`Loaded ${result.data.shape[0]} users`);
  console.log(result.data.toString());
} else {
  console.error("Error:", result.error.message);
}
```

### Lazy loading with filters

```typescript
import { LazyFrame, DType } from "molniya";

// Only load active users over 18
const query = LazyFrame.scanCsv("users.csv", {
  id: DType.Int32,
  name: DType.String,
  age: DType.Int32,
  status: DType.String,
})
  .filter("status", "==", "active")
  .filter("age", ">=", 18)
  .select(["id", "name", "age"]);

const result = await query.collect();
```

**Performance:** If file has 1M rows but only 10K match filters, LazyFrame only loads ~10K rows.

### Custom delimiter and null values

```typescript
const result = await readCsv("data.tsv", schema, {
  delimiter: "\t",
  nullValues: ["NA", "NULL", "n/a", "-"],
  comment: "#",
});
```

### Loading from API response

```typescript
const response = await fetch("https://api.example.com/data.csv");
const csvText = await response.text();

const result = await readCsvFromString(csvText, {
  timestamp: DType.Datetime,
  value: DType.Float64,
  label: DType.String,
});
```

### Memory-efficient processing

```typescript
// Process 100MB file with only 10MB memory
const query = LazyFrame.scanCsv("large.csv", schema, {
  chunkSize: 5000, // Small chunks
})
  .filter("important", "==", true) // Reduces data early
  .select(["id", "value"]); // Only load 2 columns

const result = await query.collect();
```

### File with metadata header

```csv
# Data Export
# Generated: 2024-01-01
# Version: 2.0
id,name,value
1,Alice,100
2,Bob,200
```

```typescript
const result = await readCsv("export.csv", schema, {
  skipRows: 3, // Skip metadata lines
  comment: "#", // Also skip comment lines elsewhere
});
```

## Error Handling

All CSV reading functions return `Result<T, Error>`:

```typescript
const result = await readCsv("data.csv", schema);

if (result.ok) {
  const df = result.data;
  // Use DataFrame
} else {
  const error = result.error;

  if (error.message.includes("No such file")) {
    console.error("File not found");
  } else if (error.message.includes("Parse error")) {
    console.error("Invalid CSV format");
  } else {
    console.error("Unknown error:", error.message);
  }
}
```

See [Result Type](./result.md) for error handling patterns.

## Performance Guide

### When to use scanCsv (Lazy)

✅ **Use lazy when:**

- File > 10MB
- You filter or select columns
- Memory is limited
- Processing in chunks is beneficial

**Example:**

```typescript
// 1GB file, only need 1% of data
const query = LazyFrame.scanCsv("huge.csv", schema)
  .filter("category", "==", "Electronics") // 1% of rows
  .select(["id", "name"]); // 2 of 50 columns

// Only loads ~20MB instead of 1GB
const result = await query.collect();
```

### When to use readCsv (Eager)

✅ **Use eager when:**

- File < 10MB
- You need all data
- Performing complex operations
- Debugging or exploration

**Example:**

```typescript
// Small config file, need everything
const result = await readCsv("config.csv", schema);
if (result.ok) {
  const sorted = result.data.sortBy("priority", false);
  const grouped = sorted.groupby(
    ["category"],
    [
      /* aggs */
    ],
  );
}
```

### Chunk size tuning

```typescript
// High memory, optimize for speed
const fast = LazyFrame.scanCsv("data.csv", schema, {
  chunkSize: 100000, // Large chunks
});

// Low memory, optimize for efficiency
const efficient = LazyFrame.scanCsv("data.csv", schema, {
  chunkSize: 1000, // Small chunks
});
```

**Rule of thumb:** `chunkSize * columns * 8 bytes ≈ chunk memory`

## See Also

- [LazyFrame API](./lazyframe.md) - Query optimization
- [DataFrame API](./dataframe.md) - Eager operations
- [CSV Writing](./csv-writing.md) - Writing CSV files
- [CSV I/O Guide](/guide/io-csv.md) - Detailed examples
