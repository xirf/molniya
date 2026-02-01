# Reading CSV Files

Molniya provides streaming CSV reading with schema validation.

## Basic Usage

### readCsv()

Read a CSV file into a DataFrame.

```typescript
import { readCsv, DType } from "Molniya";

const df = await readCsv("data.csv", {
  id: DType.int32,
  name: DType.string,
  amount: DType.float64
});
```

The function returns immediately with a lazy DataFrame. No data is read until you call an action like `show()` or `collect()`.

## Schema Definition

Every column must have a type specified:

```typescript
const schema = {
  // Integer types
  id: DType.int32,
  small_count: DType.int16,
  big_id: DType.int64,        // Returns bigint
  
  // Float types
  price: DType.float64,
  rating: DType.float32,
  
  // Other types
  name: DType.string,         // Dictionary-encoded
  active: DType.boolean,
  created: DType.timestamp    // Returns bigint (ms since epoch)
};

const df = await readCsv("data.csv", schema);
```

## Streaming Behavior

`readCsv()` streams data in chunks:

```typescript
// This returns immediately - no data read yet
const df = await readCsv("huge_file.csv", schema);

// Streaming operations process chunks as they arrive
const filtered = df.filter(col("status").eq("active"));
const limited = filtered.limit(100);

// Now data flows through: read chunk → filter → limit
await limited.show();
```

This allows processing files larger than available memory.

## Options

### CsvOptions

```typescript
interface CsvOptions {
  delimiter?: string;      // Default: ','
  hasHeader?: boolean;     // Default: true
  projection?: number[];   // Column indices to read
}
```

### Custom Delimiter

```typescript
// Tab-separated file
const df = await readCsv("data.tsv", schema, {
  delimiter: "\t"
});

// Semicolon-separated (common in Europe)
const df = await readCsv("data.csv", schema, {
  delimiter: ";"
});
```

### No Header Row

```typescript
// CSV without header
const df = await readCsv("data_no_header.csv", schema, {
  hasHeader: false
});
```

### Column Projection

Read only specific columns by index:

```typescript
// Only read columns 0, 2, and 3
const df = await readCsv("data.csv", schema, {
  projection: [0, 2, 3]
});
```

::: tip Projection Performance
Using `projection` can significantly improve performance by skipping unused columns entirely during parsing.
:::

## From CSV String

For testing or small datasets, use `fromCsvString()`:

```typescript
import { fromCsvString, DType } from "Molniya";

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

await df.show();
```

## Type Conversions

CSV values are parsed according to the schema:

| CSV Value | DType | Result |
|-----------|-------|--------|
| `"123"` | `int32` | `123` |
| `"123.45"` | `float64` | `123.45` |
| `"true"` | `boolean` | `true` |
| `"hello"` | `string` | `"hello"` |
| `""` | nullable type | `null` |
| `""` | non-nullable | Error or 0/empty string |

## Error Handling

```typescript
try {
  const df = await readCsv("missing.csv", schema);
  await df.show();
} catch (error) {
  if (error.message.includes("ENOENT")) {
    console.error("File not found");
  } else if (error.message.includes("parse")) {
    console.error("Parse error - check schema matches CSV");
  }
}
```

Common errors:
- File not found
- Schema column count doesn't match CSV
- Type conversion failure (e.g., "abc" for int32 column)
- Invalid CSV format

## Performance Tips

1. **Use appropriate types** - Don't use `int64` if `int32` is sufficient
2. **Filter early** - Apply filters before materializing
3. **Project columns** - Only read columns you need
4. **Limit during testing** - Use `.limit(1000)` while developing

```typescript
// Efficient: Filter streams, only needed columns kept
const result = await readCsv("huge.csv", schema)
  .filter(col("year").eq(2024))
  .select("id", "amount")
  .limit(100)
  .toArray();

// Less efficient: Materializes everything
const result = await readCsv("huge.csv", schema)
  .toArray()  // Loads all data
  .then(rows => rows.filter(r => r.year === 2024));
```

## Advanced: CsvSource

For more control, use `CsvSource` directly:

```typescript
import { CsvSource } from "Molniya";

const source = CsvSource.fromFile("data.csv", schema);

// Access schema and dictionary
console.log(source.getSchema());
console.log(source.getDictionary());

// Parse synchronously (for strings only)
const chunks = source.parseSync();

// Or stream
for await (const chunk of source) {
  // Process chunk
}
```

## See Also

- [Data Types](./data-types) - All available types
- [Creating DataFrames](./creating-dfs) - Other ways to create DataFrames
- [Core Concepts](./core-concepts) - Understanding lazy evaluation
