# API Reference

Complete reference for Molniya's public API.

## Overview

Molniya's API is organized into several modules:

| Module            | Description                                                 |
| ----------------- | ----------------------------------------------------------- |
| **DataFrame**     | Core data structure and transformation methods              |
| **Expressions**   | Building blocks for filters, calculations, and aggregations |
| **Data Types**    | Type system for schema definitions                          |
| **I/O**           | Reading and writing data files                              |
| **Operators**     | Low-level streaming operations                              |
| **Visualization** | Chart generation from DataFrames                            |

## Import Patterns

```typescript
// Main exports - covers most use cases
import { 
  readCsv, readParquet, fromRecords, fromCsvString,
  DataFrame, DType, 
  col, lit, and, or, not,
  sum, avg, min, max, count, first, last,
  asc, desc
} from "molniya";

// Advanced: Direct access to operators
import { filter, project, aggregate, sort, limit } from "molniya";

// Advanced: Schema and types
import { createSchema, DTypeKind } from "molniya";
```

## API Categories

### DataFrame Operations

- [DataFrame Class](./dataframe) - Core class and properties
- [Creation Functions](./dataframe-creation) - `readCsv`, `fromRecords`, etc.
- [Inspection Methods](./inspection) - `show`, `count`, `schema`
- [Execution Methods](./execution) - `collect`, `toArray`, `toChunks`

### Transformations

- [Filtering](./filtering) - `filter`, `where`
- [Projection](./projection) - `select`, `drop`, `rename`
- [Column Operations](./column-ops) - `withColumn`, `cast`, `fillNull`
- [Sorting & Limiting](./sort-limit) - `sort`, `limit`, `head`, `slice`

### Aggregation & Joins

- [GroupBy](./groupby) - Grouping and the `RelationalGroupedDataset` class
- [Aggregation Functions](./aggregations) - `sum`, `avg`, `count`, etc.
- [Joins](./joins) - `innerJoin`, `leftJoin`, `crossJoin`, etc.

### Expressions

- [Expression Builders](./expr-builders) - `col`, `lit`, arithmetic functions
- [ColumnRef](./column-ref) - Column reference methods
- [Operators](./operators) - Comparison and logical operators

### Data Types

- [DType](./dtype) - Data type factory
- [DTypeKind](./dtype-kind) - Type identifiers
- [Schema](./schema) - Schema creation and manipulation

### I/O

- [CSV](./csv) - CSV reading options and functions
- [Parquet](./parquet) - Parquet file support

### Visualization

- [Plotting](./plotting) - Bar, line, scatter, and histogram charts

## TypeScript Types

Molniya exports several TypeScript types for advanced use:

```typescript
import type { 
  DataFrame,
  Expr,
  ColumnRef,
  Schema,
  DType,
  DTypeKind,
  CsvOptions,
  CsvSchemaSpec,
  AggSpec,
  SortKey,
  JoinConfig
} from "molniya";
```

## Error Handling

Most API methods throw descriptive errors for invalid operations:

```typescript
try {
  const df = await readCsv("missing.csv", schema);
} catch (error) {
  // Error: File not found or unreadable
}

try {
  df.filter(col("nonexistent").gt(0));
} catch (error) {
  // Error: Column not found
}
```

## Version Compatibility

This documentation covers Molniya v0.1.0. The API is subject to change while in pre-1.0 development.

## See Also

- [Getting Started Guide](../guide/getting-started) - Introduction to concepts
- [Cookbook](../cookbook/) - Practical usage patterns
- [Examples](../examples/) - Real-world scenarios
