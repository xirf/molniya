# API Reference

Welcome to the Molniya API reference. This section provides detailed documentation for all classes, functions, and types.

## Core Classes

**[DataFrame](./dataframe.md)**  
The main data structure for working with tabular data. Provides methods for filtering, selecting, sorting, and transforming data.

**[LazyFrame](./lazyframe.md)**  
Query builder that creates optimized execution plans. Use for large files or when you want automatic query optimization.

**[Series](./series.md)**  
A single column of typed data. Provides operations for searching, sorting, and string manipulation.

## I/O Functions

**[CSV Reading](./csv-reading.md)**  
Functions for loading CSV files: `scanCsv`, `scanCsvFromString`, `readCsv`, `readCsvFromString`.

**[CSV Writing](./csv-writing.md)**  
Functions for exporting data to CSV: `writeCsv`, `toCsv`.

## Types

**[DType](./dtype.md)**  
Data type enumeration for defining column types.

**[Result](./result.md)**  
Result type for explicit error handling.

**[Schema](./schema.md)**  
Type definitions for schema objects.

## Quick Links

- [Getting Started Guide](/guide/getting-started)
- [Cookbook](/guide/cookbook)
- [GitHub Repository](https://github.com/xirf/molniya)

## Type Signatures

Molniya is fully typed. All functions and classes include TypeScript type signatures. Hover over functions in your IDE to see parameter types and return types.

## Error Handling

Most operations return `Result<T, E>` types:

```typescript
const result = await scanCsv("data.csv", { schema });

if (result.ok) {
  const df = result.data; // T
} else {
  const error = result.error; // E
}
```

See [Result API](./result.md) for details.
