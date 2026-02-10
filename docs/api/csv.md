# CSV API

Functions and types for reading CSV files.

## Functions

### readCsv()

Read a CSV file into a DataFrame with streaming support.

```typescript
readCsv<T = Record<string, unknown>>(
  path: string,
  schema: CsvSchemaSpec,
  options?: CsvOptions
): Promise<DataFrame<T>>
```

**Example:**

```typescript
import { readCsv, DType } from "molniya";

const df = await readCsv("data.csv", {
  id: DType.int32,
  name: DType.string,
  amount: DType.float64
});
```

### fromCsvString()

Create a DataFrame from a CSV string.

```typescript
fromCsvString<T = Record<string, unknown>>(
  csvString: string,
  schema: CsvSchemaSpec,
  options?: CsvOptions
): DataFrame<T>
```

**Example:**

```typescript
import { fromCsvString, DType } from "molniya";

const df = fromCsvString("id,name\n1,Alice", {
  id: DType.int32,
  name: DType.string
});
```

### readCsvFile()

Read a CSV file (lower-level, returns `CsvSource`).

```typescript
readCsvFile(
  path: string,
  schemaSpec: CsvSchemaSpec,
  options?: CsvOptions
): Result<CsvSource>
```

### readCsvString()

Read CSV from string (lower-level, returns `CsvSource`).

```typescript
readCsvString(
  content: string,
  schemaSpec: CsvSchemaSpec,
  options?: CsvOptions
): Result<CsvSource>
```

## Types

### CsvSchemaSpec

Schema specification for CSV reading.

```typescript
type CsvSchemaSpec = Record<string, DType>;
```

**Example:**

```typescript
const schema: CsvSchemaSpec = {
  id: DType.int32,
  name: DType.string,
  price: DType.float64,
  active: DType.boolean
};
```

### CsvOptions

Options for CSV parsing.

```typescript
interface CsvOptions {
  /** Field delimiter character (default: ',') */
  delimiter?: string;
  
  /** Whether the first row is a header (default: true) */
  hasHeader?: boolean;
  
  /** Column indices to read (undefined = all columns) */
  projection?: number[];
}
```

**Example:**

```typescript
const options: CsvOptions = {
  delimiter: ";",
  hasHeader: false,
  projection: [0, 2, 3]  // Only columns 0, 2, and 3
};
```

### CsvSource

Class representing a CSV data source.

```typescript
class CsvSource {
  /** Create from file path */
  static fromFile(
    path: string,
    schemaSpec: CsvSchemaSpec,
    options?: CsvOptions
  ): Result<CsvSource>;
  
  /** Create from string content */
  static fromString(
    content: string,
    schemaSpec: CsvSchemaSpec,
    options?: CsvOptions
  ): Result<CsvSource>;
  
  /** Get the dictionary for string values */
  getDictionary(): Dictionary;
  
  /** Get the schema used for parsing */
  getSchema(): Schema;
  
  /** Parse string content synchronously */
  parseSync(): Chunk[];
  
  /** Read all chunks */
  collectChunks(): Promise<Result<Chunk[]>>;
  
  /** Stream chunks (async iterator) */
  [Symbol.asyncIterator](): AsyncIterator<Chunk>;
}
```

### CsvParser

Low-level CSV parser.

```typescript
class CsvParser {
  /** Parse a chunk of bytes */
  parse(data: Uint8Array): Chunk[];
  
  /** Finish parsing and return any remaining data */
  finish(): Chunk | null;
  
  /** Reset parser state */
  reset(): void;
  
  /** Get the dictionary */
  getDictionary(): Dictionary;
  
  /** Get the schema */
  getSchema(): Schema;
}
```

### createCsvParser()

Factory function for creating a CsvParser.

```typescript
createCsvParser(schema: Schema, options?: CsvOptions): CsvParser
```

## Delimiter Options

| File Type | Delimiter |
|-----------|-----------|
| Standard CSV | `,` |
| TSV (Tab-Separated) | `\t` |
| European CSV | `;` |
| Pipe-delimited | `\|` |

## Examples

### Tab-Separated File

```typescript
const df = await readCsv("data.tsv", schema, {
  delimiter: "\t"
});
```

### No Header Row

```typescript
const df = await readCsv("data_no_header.csv", schema, {
  hasHeader: false
});
```

### Column Projection

```typescript
// Only read specific columns by index
const df = await readCsv("wide_table.csv", schema, {
  projection: [0, 3, 5, 7]
});
```

### Streaming with CsvSource

```typescript
import { CsvSource, unwrap } from "molniya";

const source = unwrap(CsvSource.fromFile("huge.csv", schema));

// Stream chunks one at a time
for await (const chunk of source) {
  console.log(`Processing ${chunk.rowCount} rows`);
  // Process chunk...
}
```

### Custom Parsing

```typescript
import { createCsvParser, CsvSource, unwrap } from "molniya";

const schema = unwrap(createSchema({
  id: DType.int32,
  value: DType.float64
}));

const parser = createCsvParser(schema, { delimiter: ";" });

// Parse chunks manually
const bytes = new TextEncoder().encode("1;100\n2;200");
const chunks = parser.parse(bytes);
const final = parser.finish();
```

## Error Handling

```typescript
import { CsvSource, isErr } from "molniya";

const result = CsvSource.fromFile("data.csv", schema);

if (isErr(result)) {
  console.error("Failed to create source:", result.error);
}
```

Common CSV errors:
- File not found
- Permission denied
- Parse error (malformed CSV)
- Type conversion error
- Schema mismatch

## See Also

- [Reading CSV Guide](../guide/reading-csv) - Detailed guide
- [DataFrame Creation](./dataframe-creation) - Other creation methods
