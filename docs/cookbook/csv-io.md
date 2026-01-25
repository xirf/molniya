# CSV I/O Recipes

CSV is the most common data format. Molniya gives you two ways to load CSVs:

**`readCsv` (eager):** Loads entire file into memory immediately. Simple and fast for small files.

**`LazyFrame.scanCsv` (lazy):** Builds a query plan first, then executes optimized. Best for large files or when filtering.

**Rule of thumb:** Use `readCsv` for files < 10MB, `LazyFrame.scanCsv` for larger files.

## Basic Loading

### Simple CSV load

**What happens:** File is read, parsed, and type-checked according to your schema. If any row fails type conversion, you get an error.

```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("data.csv", {
  name: DType.String,
  age: DType.Int32,
  email: DType.String,
});

if (result.ok) {
  console.log(result.data.toString());
}
```

### From URL/API

```typescript
const response = await fetch("https://example.com/data.csv");
const csvText = await response.text();

const result = await readCsvFromString(csvText, {
  id: DType.Int32,
  value: DType.Float64,
});
```

## CSV Options

CSV files aren't always comma-separated. These options handle the variations you'll encounter in real data.

### Custom delimiter

**Why you need this:** Excel exports vary by locale (Europe uses semicolons), databases often export as TSV, and some systems use pipes.

```typescript
// Tab-separated
const result = await readCsv("data.tsv", schema, {
  delimiter: "\t",
});

// Pipe-separated
const result = await readCsv("data.psv", schema, {
  delimiter: "|",
});

// Semicolon (Excel Europe)
const result = await readCsv("data.csv", schema, {
  delimiter: ";",
});
```

### No header row

```typescript
const result = await readCsv("data.csv", schema, {
  hasHeader: false,
});

// Column names come from schema keys
```

### Handle missing values

```typescript
const result = await readCsv("messy.csv", schema, {
  nullValues: ["NA", "NULL", "n/a", "-", ""],
});

// All these become null in the DataFrame
```

### Skip rows

```typescript
// Skip metadata at top of file
const result = await readCsv("export.csv", schema, {
  skipRows: 3, // Skip first 3 rows
});
```

### Skip comments

```typescript
const result = await readCsv("data.csv", schema, {
  comment: "#", // Skip lines starting with #
});
```

### Custom quotes

```typescript
const result = await readCsv("data.csv", schema, {
  quoteChar: "'", // Single quotes instead of double
  escapeChar: "\\", // Backslash for escaping
});
```

## Saving CSV

### Basic save

```typescript
import { writeCsv } from "molniya";

const result = await writeCsv(df, "output.csv");

if (result.ok) {
  console.log("Saved!");
}
```

### Custom delimiter

```typescript
await writeCsv(df, "output.tsv", {
  delimiter: "\t",
});
```

### Without header

```typescript
await writeCsv(df, "output.csv", {
  includeHeader: false,
});
```

### Custom null representation

```typescript
await writeCsv(df, "output.csv", {
  nullValue: "NA",
});
```

### Quote everything

```typescript
await writeCsv(df, "output.csv", {
  quoteStyle: "always", // Quote all fields
});
```

### Windows line endings

```typescript
await writeCsv(df, "output.csv", {
  lineTerminator: "\r\n", // CRLF for Windows
});
```

## To String

### Get CSV string

```typescript
const csvString = df.toCsv();
console.log(csvString);
```

### With options

```typescript
const csvString = df.toCsv({
  delimiter: "\t",
  includeHeader: true,
  quoteStyle: "necessary",
});
```

### Send via API

```typescript
const csvData = df.toCsv();

await fetch("/api/upload", {
  method: "POST",
  headers: { "Content-Type": "text/csv" },
  body: csvData,
});
```

## Large Files (LazyFrame)

### Scan and filter

```typescript
import { LazyFrame } from "molniya";

const result = await LazyFrame.scanCsv("huge.csv", schema)
  .filter("status", "==", "active")
  .collect();
```

### Custom chunk size

```typescript
// Low memory
const result = await LazyFrame.scanCsv("huge.csv", schema, {
  chunkSize: 5000,
}).collect();

// High performance
const result = await LazyFrame.scanCsv("data.csv", schema, {
  chunkSize: 50000,
}).collect();
```

### With CSV options

```typescript
const result = await LazyFrame.scanCsv("data.tsv", schema, {
  delimiter: "\t",
  nullValues: ["NA", "NULL"],
  comment: "#",
  chunkSize: 10000,
})
  .filter("year", "==", 2024)
  .collect();
```

## Common Scenarios

### Excel-compatible export

```typescript
await writeCsv(df, "export.csv", {
  delimiter: ",",
  lineTerminator: "\r\n", // Windows
  quoteStyle: "always", // Quote everything
});
```

### PostgreSQL COPY format

```typescript
await writeCsv(df, "pg_import.csv", {
  delimiter: "\t",
  nullValue: "\\N",
  includeHeader: false,
  quoteStyle: "never",
});
```

### Load from messy source

```typescript
const result = await readCsv("messy.csv", schema, {
  delimiter: "\t",
  skipRows: 2, // Skip metadata
  comment: "#", // Skip comments
  nullValues: ["", "NA", "NULL", "-", "n/a"],
  quoteChar: '"',
  escapeChar: '"',
});
```

### Merge multiple CSV files

```typescript
import { concat } from "molniya";

const files = ["jan.csv", "feb.csv", "mar.csv"];
const dataframes = [];

for (const file of files) {
  const result = await readCsv(file, schema);
  if (result.ok) {
    dataframes.push(result.data);
  }
}

const combined = concat(dataframes);
```

### Split large file

```typescript
const result = await readCsv("large.csv", schema);

if (result.ok) {
  const df = result.data;
  const chunkSize = 10000;

  for (let i = 0; i < df.shape[0]; i += chunkSize) {
    const chunk = df.head(Math.min(chunkSize, df.shape[0] - i));
    await writeCsv(chunk, `chunk_${i / chunkSize}.csv`);
  }
}
```

### Filter and save subset

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .filter("category", "==", "Electronics")
  .filter("price", ">", 100)
  .collect();

if (result.ok) {
  await writeCsv(result.data, "electronics_over_100.csv");
}
```

## From/To JSON

### CSV to JSON

```typescript
const result = await readCsv("data.csv", schema);

if (result.ok) {
  const json = JSON.stringify(result.data.toArray(), null, 2);
  console.log(json);
}
```

### JSON to CSV

```typescript
const data = [
  { name: "Alice", age: 25 },
  { name: "Bob", age: 30 },
];

const df = DataFrame.fromRows(data, {
  name: DType.String,
  age: DType.Int32,
});

await writeCsv(df, "output.csv");
```

## Inspection

### Peek at file

```typescript
const result = await readCsv("data.csv", schema);

if (result.ok) {
  console.log(result.data.head(10).toString());
}
```

### Count rows without loading

```typescript
// Use wc or similar for now
// Feature coming: df.count() on LazyFrame
```

### Check file format

```typescript
const result = await readCsv("unknown.csv", schema);

if (!result.ok) {
  console.error("Format error:", result.error.message);

  // Try different delimiter
  const retry = await readCsv("unknown.csv", schema, {
    delimiter: "\t",
  });
}
```

## Hot Tips

**Use LazyFrame for big files:**

```typescript
// File > 10MB? Use LazyFrame
const big = await LazyFrame.scanCsv("big.csv", schema)
  .filter("active", "==", true)
  .collect();

// File < 10MB? Use readCsv (simpler)
const small = await readCsv("small.csv", schema);
```

**Specify nullValues early:**

```typescript
const result = await readCsv("data.csv", schema, {
  nullValues: ["NA", "NULL", "", "-"],
});
```

**Always check Result:**

```typescript
const result = await readCsv("data.csv", schema);

if (!result.ok) {
  console.error("Failed:", result.error.message);
  return;
}

const df = result.data; // Now safe to use
```

**Quote style matters:**

```typescript
// Fastest (no quoting overhead)
await writeCsv(df, "fast.csv", {
  quoteStyle: "never", // If data is safe
});

// Most compatible
await writeCsv(df, "safe.csv", {
  quoteStyle: "always", // Works everywhere
});

// Balanced (default)
await writeCsv(df, "balanced.csv", {
  quoteStyle: "necessary", // Quote only when needed
});
```
