# Working with CSV Files

Everything you need to know about loading and saving CSV files in Molniya.

## Basic Loading

The simplest case - a well-formed CSV with headers:

```typescript
import { scanCsv, DType } from "molniya";

const schema = {
  name: DType.String,
  age: DType.Int32,
};

const result = await scanCsv("users.csv", { schema });
```

## CSV Options

### Custom Delimiter

Not all "CSV" files use commas:

```typescript
// Tab-separated values
const result = await scanCsv("data.tsv", {
  schema,
  delimiter: "\t",
});

// Pipe-separated
const result = await scanCsv("data.psv", {
  schema,
  delimiter: "|",
});

// Semicolon (common in European locales)
const result = await scanCsv("data.csv", {
  schema,
  delimiter: ";",
});
```

### Without Header Row

If your CSV doesn't have column names in the first row:

```typescript
const result = await scanCsv("data.csv", {
  schema: {
    col1: DType.String, // Column names from schema
    col2: DType.Int32,
    col3: DType.Float64,
  },
  hasHeader: false,
});
```

The first row will be treated as data.

### Skip Rows

Skip the first N rows (useful for files with metadata):

```typescript
const result = await scanCsv("data.csv", {
  schema,
  skipRows: 3, // Skip first 3 rows
});
```

### Null Values

Define what strings should be treated as null:

```typescript
const result = await scanCsv("data.csv", {
  schema,
  nullValues: ["", "NA", "NULL", "N/A", "n/a"],
});

if (result.ok) {
  const ages = result.data.get("age").toArray();
  const nulls = ages.filter((age) => age === null);
  console.log(`${nulls.length} null ages`);
}
```

### Quote Character

For fields containing delimiters or newlines:

```typescript
const result = await scanCsv("data.csv", {
  schema,
  quoteChar: '"', // Default
});
```

Example CSV with quotes:

```csv
name,description
"Product A","Contains, commas"
"Product B","Has
newlines"
```

### Comment Lines

Skip lines starting with a specific character:

```typescript
const result = await scanCsv("data.csv", {
  schema,
  comment: "#",
});
```

CSV with comments:

```csv
# This is a comment
name,age
# Another comment
Alice,25
Bob,30
```

## Large Files

### Chunked Reading

Process large files in chunks to control memory:

```typescript
const result = await scanCsv("huge-file.csv", {
  schema,
  chunkSize: 10000, // Read 10k rows at a time
});
```

### Memory Limit

Set a hard memory limit:

```typescript
const result = await scanCsv("data.csv", {
  schema,
  memoryLimit: 100 * 1024 * 1024, // 100MB max
});

if (!result.ok && result.error.message.includes("memory")) {
  console.log("File too large, try chunking");
}
```

### LazyFrame for Large Files

For files larger than memory, use LazyFrame:

```typescript
import { LazyFrame } from "molniya";

const result = await LazyFrame.scanCsv("massive.csv", schema)
  .filter("status", "==", "active") // Only load matching rows
  .select(["id", "name"]) // Only load these columns
  .collect();
```

See [Lazy Evaluation](./lazy-evaluation.md) for details.

## String Data

### From String

If you have CSV data as a string:

```typescript
import { scanCsvFromString } from "molniya";

const csvData = `name,age
Alice,25
Bob,30`;

const result = await scanCsvFromString(csvData, { schema });
```

### From Buffer

If you have CSV data as a Buffer:

```typescript
const buffer = Buffer.from(csvData);
const result = await scanCsvFromString(buffer, { schema });
```

## Writing CSV Files

Export a DataFrame to CSV:

```typescript
import { writeCsv } from "molniya";

const df = /* your DataFrame */;

const result = writeCsv(df, "output.csv", {
  delimiter: ",",
  includeHeader: true,
});

if (result.ok) {
  console.log("Saved!");
} else {
  console.error("Write failed:", result.error);
}
```

### Custom Write Options

```typescript
const result = writeCsv(df, "output.tsv", {
  delimiter: "\t",
  quoteChar: '"',
  includeHeader: true,
  nullValue: "NA", // How to represent nulls
});
```

### Write to String

Get CSV as a string instead of writing to file:

```typescript
import { toCsv } from "molniya";

const csvString = toCsv(df, {
  delimiter: ",",
  includeHeader: true,
});

console.log(csvString);
```

## Common Issues

### Schema Mismatch

CSV columns don't match schema:

```typescript
// CSV has: name,age,city
// Schema only has: name,age

const schema = {
  name: DType.String,
  age: DType.Int32,
  // 'city' column will be ignored
};

const result = await scanCsv("data.csv", { schema });
```

**Behavior:** Extra columns in CSV are ignored. Missing columns cause an error.

### Type Conversion Errors

CSV value doesn't match expected type:

```csv
name,age
Alice,25
Bob,not-a-number
```

```typescript
const result = await scanCsv("data.csv", {
  schema: {
    name: DType.String,
    age: DType.Int32,
  },
});

if (!result.ok) {
  console.error("Parse error:", result.error.message);
  // "Failed to parse 'not-a-number' as Int32"
}
```

**Solution:** Either fix the data or use `nullValues` to treat invalid values as null.

### Encoding Issues

If you see weird characters:

```typescript
// Molniya assumes UTF-8
// For other encodings, convert first:

const file = Bun.file("data-latin1.csv");
const buffer = await file.arrayBuffer();
const text = new TextDecoder("latin1").decode(buffer);
const result = await scanCsvFromString(text, { schema });
```

### Line Ending Issues

Mixed line endings (\\r\\n vs \\n):

Molniya handles both automatically. No config needed.

## Performance Tips

### Schema Order

Column order in schema doesn't matter for performance:

```typescript
// Both equally fast
const schema1 = { name: DType.String, age: DType.Int32 };
const schema2 = { age: DType.Int32, name: DType.String };
```

### Numeric Types

Use Int32 instead of Float64 when possible:

```typescript
// Slower (if values are integers)
const schema = { id: DType.Float64 };

// Faster
const schema = { id: DType.Int32 };
```

### String Columns

If a string column has few unique values, consider loading as string then converting to integer codes for filtering.

## Real-World Examples

### Loading Sales Data

```typescript
const result = await scanCsv("sales-2024.csv", {
  schema: {
    order_id: DType.Int64,
    product: DType.String,
    category: DType.String,
    quantity: DType.Int32,
    price: DType.Float64,
    date: DType.Datetime,
  },
  nullValues: ["", "NA"],
  skipRows: 1, // Skip header comment row
});
```

### Loading Log Files

```typescript
const result = await scanCsv("access.log", {
  schema: {
    timestamp: DType.Datetime,
    ip: DType.String,
    method: DType.String,
    path: DType.String,
    status: DType.Int32,
  },
  delimiter: " ",
  hasHeader: false,
  comment: "#",
});
```

### Exporting Filtered Results

```typescript
const filtered = df
  .filter("status", "==", "active")
  .select(["id", "name", "email"]);

const result = writeCsv(filtered, "active-users.csv");
```

## Next Steps

- [Data Types](./data-types.md) - Understanding types for CSV columns
- [Lazy Evaluation](./lazy-evaluation.md) - Optimize large file loading
- [Cookbook](./cookbook.md) - More CSV examples
