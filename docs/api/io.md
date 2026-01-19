# I/O API

Methods for reading data into and writing data out of Molniya.

## Reading Data

### `readCsv`
Reads a CSV file into a DataFrame.

```typescript
readCsv(path: string, options?: CsvReadOptions): Promise<{ 
    df: DataFrame,
    errors: CsvError[]
  }>
```

**Returns:**
An object containing:
- `df`: The resulting DataFrame.
- `errors`: An array of parsing errors (if any were strictly malformed but recoverable).

**Options:**
```typescript
interface CsvReadOptions {
  delimiter?: string;       // Default: ','
  hasHeader?: boolean;      // Default: true
  encoding?: string;        // Default: 'utf-8'
  maxRows?: number;         // Stop after N rows
  sampleRows?: number;      // Rows to scan for type inference (Default: 100)
  nullValues?: string[];    // Strings to treat as null (e.g. ['NA', 'null'])
  datetime?: {
    defaultZone?: string;   // Default timezone (e.g. 'UTC')
    columns?: Record<string, { format: 'iso'|'sql'|'unix-s'|'unix-ms', zone?: string }>
  }
}
```

**Example:**
```typescript
const { df } = await readCsv('./data.csv', {
  delimiter: '\t',
  maxRows: 500
});
```

---

### `scanCsv` (Lazy Loading)
Creates a `LazyFrame` for processing files larger than memory.

```typescript
scanCsv(path: string, options?: CsvReadOptions & { lazyConfig?: LazyFrameConfig }): Promise<LazyFrame>
```

**Lazy Configuration**
```typescript
interface LazyFrameConfig {
  maxCacheMemory?: number; // Cache limit in bytes (default: 100MB)
  chunkSize?: number;      // Rows per chunk (default: 10,000)
  raw?: boolean;           // Keep strings as bytes (Zero-copy, extremely weird but fast)
  forceGc?: boolean;       // Force garbage collection after each chunk
}
```

> [!NOTE]
> `LazyFrame` supports a subset of operations (filter, select, head) and executes them only when you call `.collect()`.

---

## Writing Data

### `writeCsv`
Writes a DataFrame to a CSV file.

```typescript
writeCsv(df: DataFrame, path: string): Promise<void>
```

**Example:**
```typescript
await writeCsv(resultDf, './report.csv');
```

---

### `toCsv`
Converts a DataFrame to a CSV string.

```typescript
toCsv(df: DataFrame): string
```

**Example:**
```typescript
const csvString = toCsv(df);
console.log(csvString);
```

