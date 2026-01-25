# Result

Type-safe error handling using Result pattern.

## Overview

Molniya uses the Result pattern for operations that can fail. Instead of throwing exceptions, functions return `Result<T, E>` which is either:

- `{ ok: true, data: T }` - Success
- `{ ok: false, error: E }` - Failure

This makes errors explicit and forces you to handle them.

## Type Definition

```typescript
type Result<T, E = Error> = { ok: true; data: T } | { ok: false; error: E };
```

**Generic parameters:**

- `T` - Success value type
- `E` - Error type (default: `Error`)

## Basic Usage

### Checking Results

```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("data.csv", {
  id: DType.Int32,
  name: DType.String,
});

if (result.ok) {
  // TypeScript knows result.data exists
  const df = result.data;
  console.log(df.toString());
} else {
  // TypeScript knows result.error exists
  console.error("Failed:", result.error.message);
}
```

### Extracting Values

```typescript
// With type guard
if (result.ok) {
  const df = result.data; // DataFrame
  // Use df...
}

// Without guard (unsafe)
const df = result.ok ? result.data : null;
```

## Functions Returning Result

### CSV Operations

**readCsv**

```typescript
const result: Result<DataFrame, Error> = await readCsv("data.csv", schema);
```

**readCsvFromString**

```typescript
const result: Result<DataFrame, Error> = await readCsvFromString(
  csvData,
  schema,
);
```

**writeCsv**

```typescript
const result: Result<void, Error> = await writeCsv(df, "output.csv");
```

### LazyFrame Operations

**collect**

```typescript
const query = LazyFrame.scanCsv("data.csv", schema);
const result: Result<DataFrame, Error> = await query.collect();
```

## Error Handling Patterns

### Simple Check

```typescript
const result = await readCsv("data.csv", schema);

if (!result.ok) {
  console.error("Error:", result.error.message);
  return;
}

const df = result.data;
// Continue with df...
```

### Early Return

```typescript
async function processData() {
  const result = await readCsv("data.csv", schema);

  if (!result.ok) {
    return result; // Propagate error
  }

  const df = result.data;
  // Process df...

  return { ok: true, data: processed };
}
```

### Error Recovery

```typescript
const result = await readCsv("data.csv", schema);

const df = result.ok ? result.data : DataFrame.fromColumns({}, {}); // Empty DataFrame
```

### Multiple Operations

```typescript
const readResult = await readCsv("input.csv", schema);
if (!readResult.ok) {
  console.error("Read failed:", readResult.error.message);
  return;
}

const df = readResult.data.filter("value", ">", 0);

const writeResult = await writeCsv(df, "output.csv");
if (!writeResult.ok) {
  console.error("Write failed:", writeResult.error.message);
  return;
}

console.log("Success!");
```

### Error Type Checking

```typescript
const result = await readCsv("data.csv", schema);

if (!result.ok) {
  const error = result.error;

  // Check error message
  if (error.message.includes("ENOENT")) {
    console.error("File not found");
  } else if (error.message.includes("Parse error")) {
    console.error("Invalid CSV format");
  } else {
    console.error("Unknown error:", error.message);
  }
}
```

## Common Error Types

### File Errors

```typescript
const result = await readCsv("missing.csv", schema);

if (!result.ok) {
  const msg = result.error.message;

  if (msg.includes("ENOENT")) {
    console.error("File does not exist");
  } else if (msg.includes("EACCES")) {
    console.error("Permission denied");
  }
}
```

**Common file error codes:**

- `ENOENT` - File not found
- `EACCES` - Permission denied
- `EISDIR` - Path is a directory
- `ENOSPC` - No space left on device

### Parse Errors

```typescript
const result = await readCsv("data.csv", schema);

if (!result.ok) {
  if (result.error.message.includes("Parse error")) {
    console.error("CSV format is invalid");
    console.error("Details:", result.error.message);
  }
}
```

**Parsing issues:**

- Invalid type conversion
- Malformed CSV structure
- Missing required columns
- Schema mismatch

### Type Errors

```typescript
const result = await readCsv("data.csv", {
  age: DType.Int32,
});

if (!result.ok) {
  if (result.error.message.includes("Cannot convert")) {
    console.error("Type conversion failed");
    // Data contains non-numeric values in age column
  }
}
```

## Creating Results

When writing your own functions:

### Success Result

```typescript
function processData(df: DataFrame): Result<DataFrame, Error> {
  const filtered = df.filter("value", ">", 0);

  if (filtered.isEmpty()) {
    return {
      ok: false,
      error: new Error("No data after filtering"),
    };
  }

  return {
    ok: true,
    data: filtered,
  };
}
```

### Error Result

```typescript
function validateData(df: DataFrame): Result<void, Error> {
  if (df.shape[0] === 0) {
    return {
      ok: false,
      error: new Error("DataFrame is empty"),
    };
  }

  if (!df.columnOrder.includes("id")) {
    return {
      ok: false,
      error: new Error("Missing required column: id"),
    };
  }

  return { ok: true, data: undefined };
}
```

## Utility Functions

### unwrap (built-in)

Molniya provides a built-in `unwrap` function that extracts the value from a Result or throws the error:

```typescript
import { unwrap, readCsv } from "molniya";

// Throws error if Result is not ok
const df = unwrap(await readCsv("data.csv", schema));
```

::: warning
Use `unwrap` with caution! It throws errors and should only be used when you're confident the operation will succeed, or in examples/tests. In production code, prefer explicit error handling with `if (!result.ok)` checks.
:::

### Unwrap with default

```typescript
function unwrapOr<T, E>(result: Result<T, E>, defaultValue: T): T {
  return result.ok ? result.data : defaultValue;
}

const df = unwrapOr(
  await readCsv("data.csv", schema),
  DataFrame.fromColumns({}, {}),
);
```

### Map result

```typescript
function mapResult<T, U, E>(
  result: Result<T, E>,
  fn: (data: T) => U,
): Result<U, E> {
  if (result.ok) {
    return { ok: true, data: fn(result.data) };
  }
  return result;
}

const result = await readCsv("data.csv", schema);
const rowCount = mapResult(result, (df) => df.shape[0]);
```

### Chain operations

```typescript
async function pipeline(
  path: string,
  schema: Record<string, DType>,
): Result<number, Error> {
  const readResult = await readCsv(path, schema);
  if (!readResult.ok) return readResult;

  const df = readResult.data;
  const filtered = df.filter("value", ">", 0);

  if (filtered.isEmpty()) {
    return {
      ok: false,
      error: new Error("No valid data"),
    };
  }

  return { ok: true, data: filtered.shape[0] };
}
```

## TypeScript Integration

Result works seamlessly with TypeScript:

```typescript
// Type is inferred
const result = await readCsv("data.csv", schema);
// result: Result<DataFrame, Error>

if (result.ok) {
  // TypeScript knows result.data is DataFrame
  const rows = result.data.shape[0];

  // This would error:
  // const error = result.error;  // Property doesn't exist
} else {
  // TypeScript knows result.error is Error
  const message = result.error.message;

  // This would error:
  // const df = result.data;  // Property doesn't exist
}
```

## Complete Examples

### CSV processing pipeline

```typescript
async function processCsv(inputPath: string, outputPath: string) {
  // Read
  const readResult = await readCsv(inputPath, schema);
  if (!readResult.ok) {
    console.error("Read failed:", readResult.error.message);
    return { ok: false, error: readResult.error };
  }

  // Process
  const df = readResult.data
    .filter("status", "==", "active")
    .sortBy("date", false)
    .head(100);

  if (df.isEmpty()) {
    const error = new Error("No data to write");
    console.error(error.message);
    return { ok: false, error };
  }

  // Write
  const writeResult = await writeCsv(df, outputPath);
  if (!writeResult.ok) {
    console.error("Write failed:", writeResult.error.message);
    return writeResult;
  }

  console.log("Success!");
  return { ok: true, data: df.shape[0] };
}
```

### Lazy query with error handling

```typescript
async function analyzeData(path: string) {
  const query = LazyFrame.scanCsv(path, schema)
    .filter("revenue", ">", 1000)
    .groupby(["category"], [{ column: "revenue", agg: "sum", alias: "total" }]);

  const result = await query.collect();

  if (!result.ok) {
    if (result.error.message.includes("ENOENT")) {
      console.error(`File not found: ${path}`);
    } else {
      console.error("Query failed:", result.error.message);
    }
    return null;
  }

  return result.data;
}
```

### Validation chain

```typescript
function validateAndProcess(df: DataFrame): Result<DataFrame, Error> {
  // Check 1: Not empty
  if (df.isEmpty()) {
    return {
      ok: false,
      error: new Error("DataFrame is empty"),
    };
  }

  // Check 2: Has required columns
  const required = ["id", "name", "value"];
  for (const col of required) {
    if (!df.columnOrder.includes(col)) {
      return {
        ok: false,
        error: new Error(`Missing column: ${col}`),
      };
    }
  }

  // Check 3: Valid data
  const values = df.get("value").toArray();
  if (values.some((v) => v < 0)) {
    return {
      ok: false,
      error: new Error("Negative values not allowed"),
    };
  }

  // All checks passed
  return { ok: true, data: df };
}
```

## Why Result Pattern?

**Advantages over exceptions:**

1. **Explicit error handling** - Can't ignore errors
2. **Type safety** - Errors are part of the type
3. **Better IDE support** - TypeScript knows what's available
4. **Predictable flow** - No hidden control flow
5. **Easier testing** - Test both paths clearly

**Example comparison:**

```typescript
// With exceptions (implicit)
try {
  const df = await readCsv("data.csv", schema);
  // May throw at any point
  const result = process(df);
} catch (error) {
  console.error(error);
}

// With Result (explicit)
const result = await readCsv("data.csv", schema);
if (!result.ok) {
  console.error(result.error);
  return;
}
const df = result.data;
// Guaranteed to have df here
```

## See Also

- [CSV Reading](./csv-reading.md) - Functions returning Result
- [CSV Writing](./csv-writing.md) - Write operations with Result
- [LazyFrame API](./lazyframe.md) - collect() returns Result
