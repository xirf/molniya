# DataFrame Creation Functions

Functions for creating DataFrames from various sources.

## readCsv()

Read a CSV file with streaming support.

```typescript
readCsv<T = Record<string, unknown>>(
  path: string,
  schema: CsvSchemaSpec,
  options?: CsvOptions
): Promise<DataFrame<T>>
```

**Parameters:**
- `path` - Path to the CSV file
- `schema` - Schema specification with column names and types
- `options` - Optional parsing options

**Example:**

```typescript
import { readCsv, DType } from "molniya";

const df = await readCsv("data.csv", {
  id: DType.int32,
  name: DType.string,
  amount: DType.float64
});
```

**With Options:**

```typescript
const df = await readCsv("data.csv", schema, {
  delimiter: ";",        // Semicolon-separated
  hasHeader: false,      // No header row
  projection: [0, 2, 3]  // Only read columns 0, 2, and 3
});
```

::: tip Streaming
`readCsv()` returns immediately and streams data when you execute an action. This allows processing files larger than memory.
:::

## readParquet()

Read a Parquet file.

```typescript
readParquet<T = Record<string, unknown>>(
  path: string
): Promise<DataFrame<T>>
```

**Example:**

```typescript
import { readParquet } from "molniya";

const df = await readParquet("data.parquet");
```

::: warning Memory Usage
Currently, `readParquet()` loads the entire file into memory. For large files, consider filtering immediately after reading.
:::

## fromCsvString()

Create a DataFrame from a CSV string (synchronous).

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

const csvData = `
id,name,score
1,Alice,95
2,Bob,87
3,Charlie,92
`.trim();

const df = fromCsvString(csvData, {
  id: DType.int32,
  name: DType.string,
  score: DType.int32
});
```

::: tip Testing
`fromCsvString()` is useful for tests and examples where you want self-contained, reproducible code.
:::

## fromRecords()

Create a DataFrame from an array of objects.

```typescript
fromRecords<T = Record<string, unknown>>(
  records: Record<string, unknown>[],
  schema: SchemaSpec
): DataFrame<T>
```

**Example:**

```typescript
import { fromRecords, DType } from "molniya";

const records = [
  { id: 1, name: "Alice", score: 95 },
  { id: 2, name: "Bob", score: 87 },
  { id: 3, name: "Charlie", score: 92 }
];

const df = fromRecords(records, {
  id: DType.int32,
  name: DType.string,
  score: DType.int32
});
```

::: warning Performance
`fromRecords()` is convenient but slower than `fromCsvString()` for large datasets. Consider converting to CSV format for better performance.
:::

## DataFrame.empty()

Create an empty DataFrame with a given schema.

```typescript
DataFrame.empty<T = Record<string, unknown>>(
  schema: Schema,
  dictionary: Dictionary | null
): DataFrame<T>
```

**Example:**

```typescript
import { DataFrame, createSchema, unwrap, DType } from "molniya";

const schema = unwrap(createSchema({
  id: DType.int32,
  name: DType.string
}));

const empty = DataFrame.empty(schema, null);
console.log(await empty.count());  // 0
```

## Comparison Table

| Function | Source | Streaming | Best For |
|----------|--------|-----------|----------|
| `readCsv()` | File | Yes | Large files, production |
| `readParquet()` | File | No | Parquet files |
| `fromCsvString()` | String | No | Tests, small data |
| `fromRecords()` | Array | No | Programmatic creation |
| `DataFrame.empty()` | - | N/A | Placeholders, initialization |

## TypeScript Type Inference

All creation functions support generic type parameters:

```typescript
// Explicit type
interface User {
  id: number;
  name: string;
}

const df = await readCsv<User>("users.csv", schema);

// Inferred from schema (default)
const df = await readCsv("users.csv", schema);
// TypeScript infers: DataFrame<{ id: number, name: string }>
```

## Error Handling

```typescript
try {
  const df = await readCsv("missing.csv", schema);
} catch (error) {
  if (error.message.includes("ENOENT")) {
    console.error("File not found");
  }
}

try {
  const df = fromCsvString("invalid,data\n1,2,3", schema);
  // May throw if column count doesn't match schema
} catch (error) {
  console.error("Parse error:", error.message);
}
```

## Best Practices

1. **Use `readCsv()` for production** - Streaming handles large files
2. **Use `fromCsvString()` for tests** - Self-contained, reproducible
3. **Specify schemas explicitly** - Enables type checking and validation
4. **Handle errors** - File I/O can fail

```typescript
// Good: Explicit schema with error handling
async function loadData(path: string) {
  const schema = {
    id: DType.int32,
    name: DType.string,
    amount: DType.float64
  };
  
  try {
    return await readCsv(path, schema);
  } catch (error) {
    console.error(`Failed to load ${path}:`, error);
    throw error;
  }
}
```

## See Also

- [Reading CSV Guide](../guide/reading-csv) - Detailed CSV options
- [DataFrame Class](./dataframe) - DataFrame methods
