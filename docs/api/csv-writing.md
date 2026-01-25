# CSV Writing

Functions for saving DataFrames to CSV format.

## Overview

Molniya provides two CSV writing functions:

- **`writeCsv`** - Write DataFrame to file
- **`toCsv`** - Convert DataFrame to CSV string

## Writing to File

### writeCsv

Save DataFrame to a CSV file.

```typescript
import { DataFrame, DType, writeCsv } from "molniya";

const df = DataFrame.fromColumns(
  {
    id: [1, 2, 3],
    name: ["Alice", "Bob", "Carol"],
    value: [10.5, 20.3, 30.7],
  },
  {
    id: DType.Int32,
    name: DType.String,
    value: DType.Float64,
  },
);

const result = await writeCsv(df, "output.csv");

if (result.ok) {
  console.log("File saved successfully");
} else {
  console.error("Write failed:", result.error.message);
}
```

**Parameters:**

- `dataframe: DataFrame` - DataFrame to save
- `path: string` - Output file path
- `options?: CsvWriteOptions` - Optional configuration

**Returns:** `Promise<Result<void, Error>>`

**Output (output.csv):**

```csv
id,name,value
1,Alice,10.5
2,Bob,20.3
3,Carol,30.7
```

## Converting to String

### toCsv

Convert DataFrame to CSV string.

```typescript
const csvString = df.toCsv();
console.log(csvString);
```

**Parameters:**

- `options?: CsvWriteOptions` - Optional configuration

**Returns:** `string` - CSV formatted data

**Use cases:**

- Sending CSV via API
- Copying to clipboard
- In-memory processing
- Testing

**Example:**

```typescript
const response = await fetch("/api/upload", {
  method: "POST",
  headers: { "Content-Type": "text/csv" },
  body: df.toCsv(),
});
```

## Write Options

Both functions accept optional configuration:

```typescript
interface CsvWriteOptions {
  delimiter?: string; // Column separator (default: ",")
  includeHeader?: boolean; // Write column names (default: true)
  quoteChar?: string; // Quote character (default: '"')
  quoteStyle?: QuoteStyle; // When to quote (default: "necessary")
  lineTerminator?: string; // Line ending (default: "\n")
  nullValue?: string; // Null representation (default: "")
}
```

### delimiter

Column separator character.

```typescript
// Tab-separated values
const result = await writeCsv(df, "data.tsv", {
  delimiter: "\t",
});

// Pipe-separated values
const result = await writeCsv(df, "data.psv", {
  delimiter: "|",
});
```

**Output (TSV):**

```
id	name	value
1	Alice	10.5
```

### includeHeader

Whether to write column names as first row.

```typescript
const result = await writeCsv(df, "data.csv", {
  includeHeader: false,
});
```

**Output (no header):**

```csv
1,Alice,10.5
2,Bob,20.3
3,Carol,30.7
```

**Use case:** Appending to existing file.

### quoteChar

Character used for quoting fields.

```typescript
const result = await writeCsv(df, "data.csv", {
  quoteChar: "'", // Single quotes
});
```

**Output:**

```csv
id,name,value
1,'Alice',10.5
```

**Default:** `"` (double quote)

### quoteStyle

Controls when fields are quoted.

```typescript
type QuoteStyle = "necessary" | "always" | "never";
```

**Options:**

**`"necessary"` (default)** - Quote only when needed:

```typescript
const result = await writeCsv(df, "data.csv", {
  quoteStyle: "necessary",
});
```

Output:

```csv
id,name,description
1,Alice,Simple text
2,Bob,"Text with, comma"
3,Carol,"Text with ""quotes"""
```

**`"always"` - Quote all fields:**

```typescript
const result = await writeCsv(df, "data.csv", {
  quoteStyle: "always",
});
```

Output:

```csv
"id","name","value"
"1","Alice","10.5"
"2","Bob","20.3"
```

**`"never"` - Never quote (unsafe):**

```typescript
const result = await writeCsv(df, "data.csv", {
  quoteStyle: "never",
});
```

Output:

```csv
id,name,value
1,Alice,10.5
```

⚠️ **Warning:** `"never"` can produce invalid CSV if data contains delimiters or newlines.

### lineTerminator

Line ending character(s).

```typescript
// Unix/Mac (LF)
const result = await writeCsv(df, "data.csv", {
  lineTerminator: "\n", // Default
});

// Windows (CRLF)
const result = await writeCsv(df, "data.csv", {
  lineTerminator: "\r\n",
});
```

**Default:** `"\n"` (Unix-style)

### nullValue

String to represent null values.

```typescript
const df = DataFrame.fromColumns(
  { name: ["Alice", null, "Carol"] },
  { name: DType.String },
);

const result = await writeCsv(df, "data.csv", {
  nullValue: "NA",
});
```

**Output:**

```csv
name
Alice
NA
Carol
```

**Default:** `""` (empty string)

## Complete Examples

### Basic file writing

```typescript
import { DataFrame, DType, writeCsv } from "molniya";

const df = DataFrame.fromColumns(
  {
    date: ["2024-01-01", "2024-01-02"],
    temperature: [72.5, 68.3],
    location: ["NYC", "LA"],
  },
  {
    date: DType.String,
    temperature: DType.Float64,
    location: DType.String,
  },
);

const result = await writeCsv(df, "weather.csv");

if (!result.ok) {
  console.error("Failed:", result.error.message);
}
```

### TSV with custom null value

```typescript
const result = await writeCsv(df, "data.tsv", {
  delimiter: "\t",
  nullValue: "NULL",
  quoteStyle: "necessary",
});
```

### Exporting for Excel

```typescript
// Excel-compatible CSV
const result = await writeCsv(df, "export.csv", {
  delimiter: ",",
  lineTerminator: "\r\n", // Windows line endings
  quoteStyle: "always", // Quote everything
});
```

### API response

```typescript
import { toCsv } from "molniya";

// Express.js endpoint
app.get("/api/export", (req, res) => {
  const df = /* ... build DataFrame ... */;

  const csv = df.toCsv({
    delimiter: ",",
    includeHeader: true,
  });

  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", "attachment; filename=data.csv");
  res.send(csv);
});
```

### Streaming large DataFrame

```typescript
// For very large DataFrames, write in chunks
async function writeChunked(df: DataFrame, path: string) {
  const chunkSize = 10000;
  const totalRows = df.shape[0];

  for (let start = 0; start < totalRows; start += chunkSize) {
    const chunk = df.slice(start, start + chunkSize);

    const result = await writeCsv(chunk, path, {
      includeHeader: start === 0, // Header only for first chunk
      // Append mode would be needed here (not yet implemented)
    });

    if (!result.ok) {
      return result;
    }
  }

  return { ok: true };
}
```

### Handling special characters

```typescript
const df = DataFrame.fromColumns(
  {
    text: [
      "Simple text",
      "Text with, comma",
      'Text with "quotes"',
      "Text with\nnewline",
    ],
  },
  { text: DType.String },
);

const result = await writeCsv(df, "special.csv", {
  quoteStyle: "necessary", // Auto-quotes when needed
});
```

**Output:**

```csv
text
Simple text
"Text with, comma"
"Text with ""quotes"""
"Text with
newline"
```

### Custom format for specific tool

```typescript
// Format for PostgreSQL COPY
const result = await writeCsv(df, "pg_import.csv", {
  delimiter: "\t",
  nullValue: "\\N",
  includeHeader: false,
  quoteStyle: "never",
});
```

## Error Handling

Both functions return `Result` type:

```typescript
const result = await writeCsv(df, "output.csv");

if (result.ok) {
  console.log("Success!");
} else {
  const error = result.error;

  if (error.message.includes("ENOENT")) {
    console.error("Directory does not exist");
  } else if (error.message.includes("EACCES")) {
    console.error("Permission denied");
  } else if (error.message.includes("ENOSPC")) {
    console.error("Disk full");
  } else {
    console.error("Write error:", error.message);
  }
}
```

Common errors:

- **ENOENT** - Directory doesn't exist
- **EACCES** - No write permission
- **ENOSPC** - Out of disk space
- **EISDIR** - Path is a directory

See [Result Type](./result.md) for patterns.

## Performance Tips

### For large DataFrames

```typescript
// Use appropriate options
const result = await writeCsv(df, "large.csv", {
  quoteStyle: "necessary", // Faster than "always"
  lineTerminator: "\n", // Simpler than "\r\n"
});
```

### For fast writes

```typescript
// Minimal processing
const result = await writeCsv(df, "fast.csv", {
  includeHeader: false, // Skip header
  quoteStyle: "never", // No quoting (if data is safe)
});
```

### For maximum compatibility

```typescript
// Works with most tools
const result = await writeCsv(df, "compatible.csv", {
  delimiter: ",",
  quoteStyle: "always",
  lineTerminator: "\r\n",
  includeHeader: true,
});
```

## Limitations

**Current limitations:**

- No append mode (always overwrites file)
- No streaming API (entire DataFrame in memory)
- No custom formatters (e.g., date format, number precision)

**Workarounds:**

For append:

```typescript
// Read existing file, concatenate, write
const existing = await readCsv("data.csv", schema);
if (existing.ok) {
  const combined = concat([existing.data, newData]);
  await writeCsv(combined, "data.csv");
}
```

For custom formatting:

```typescript
// Transform before writing
const formatted = df.select(["id", "date"]).map((row) => ({
  id: row.id,
  date: new Date(row.date).toISOString().split("T")[0],
}));
await writeCsv(formatted, "output.csv");
```

## See Also

- [CSV Reading](./csv-reading.md) - Loading CSV files
- [DataFrame API](./dataframe.md) - DataFrame operations
- [Result Type](./result.md) - Error handling
- [CSV I/O Guide](/guide/io-csv.md) - Detailed examples
