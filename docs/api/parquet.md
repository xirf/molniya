# Parquet API

API reference for reading Parquet files.

> [!WARNING] Warning - Experimental
> Current parquet implementation only support file with no or snappy compression. Other compression algorithms are not supported yet.
>
> Current parquet implementation load all data into memory and slower than CSV. It is not recommended to use this API for large files.


## readParquet()

Read a Parquet file into a DataFrame.

```typescript
readParquet<T = Record<string, unknown>>(
  path: string,
  schema: SchemaSpec
): Promise<DataFrame<T>>
```

**Parameters:**
- `path` - Path to the Parquet file
- `schema` - Schema specification with column names and types

**Returns:** Promise resolving to a DataFrame

**Example:**

```typescript
import { readParquet, DType } from "Molniya";

const df = await readParquet("data.parquet", {
  id: DType.int64,
  name: DType.string,
  amount: DType.float64,
  created_at: DType.timestamp
});
```

## ParquetReader

Low-level Parquet reader for advanced use cases.

```typescript
class ParquetReader {
  constructor(filePath: string);
  
  /** Read metadata from the file */
  readMetadata(): Promise<FileMetaData>;
  
  /** Read specific row groups */
  readRowGroups(schema: Schema, rowGroups: number[]): Promise<Chunk[]>;
  
  /** Read entire file */
  readAll(schema: Schema): Promise<Chunk[]>;
}
```

**Example:**

```typescript
import { ParquetReader, DType } from "Molniya";

const reader = new ParquetReader("large_file.parquet");
const metadata = await reader.readMetadata();

// Read specific row groups for partial processing
const chunks = await reader.readRowGroups(schema, [0, 1, 2]);
```

## Schema Mapping

Parquet types are mapped to Molniya types:

| Parquet Type | Molniya Type | Notes |
|--------------|--------------|-------|
| `INT32` | `int32` | 32-bit signed integer |
| `INT64` | `int64` | 64-bit signed integer |
| `FLOAT` | `float32` | Single precision float |
| `DOUBLE` | `float64` | Double precision float |
| `BYTE_ARRAY` | `string` | Dictionary-encoded |
| `BOOLEAN` | `boolean` | True/false values |
| `INT96` | `timestamp` | Legacy timestamp format |

## Supported Compression

- **SNAPPY** - Default, fast compression/decompression
- **UNCOMPRESSED** - No compression

## Supported Encodings

- **PLAIN** - Plain values
- **RLE** - Run-length encoding for dictionary indices
- **RLE_DICTIONARY** - Dictionary encoding for strings

## Limitations

- Nested structures are flattened
- Complex types (arrays, maps) not yet supported
- Write support not yet implemented
- Predicate pushdown not yet implemented

## Example: Reading Large Files

```typescript
import { readParquet, DType } from "Molniya";

const schema = {
  user_id: DType.int64,
  event_type: DType.string,
  timestamp: DType.timestamp,
  value: DType.float64
};

// Stream large files in chunks
const df = await readParquet("events.parquet", schema);

for await (const chunk of df.toChunks()) {
  // Process each chunk
  console.log(`Processed ${chunk.length} rows`);
}
```

## Example: Selective Column Reading

```typescript
import { readParquet, DType } from "Molniya";

// Only specify columns you need
const minimalSchema = {
  id: DType.int32,
  name: DType.string
};

const df = await readParquet("large_file.parquet", minimalSchema);
```
