/**
 * Molniya - High-performance data manipulation for Bun.js
 *
 * A Pandas-like library leveraging Bun's speed with Elysia-style type inference.
 *
 * @example
 * ```ts
 * import { readCsv, DataFrame, Series, m } from 'molniya';
 *
 * // Read CSV with automatic type inference
 * const df = await readCsv('./data.csv');
 * df.print();
 *
 * // Create DataFrame with explicit schema
 * const df2 = DataFrame.from(
 *   { age: m.int32(), name: m.string() },
 *   [{ age: 25, name: 'Alice' }]
 * );
 *
 * // Access typed columns
 * const ages = df2.col('age'); // Series<'int32'>
 *
 * // For large files (10GB+), use lazy loading
 * const lazy = await scanCsv('./huge_dataset.csv');
 * const first10 = await lazy.head(10);
 * ```
 */

// Core type system
export { m } from './core/types';
export type {
  DType,
  DTypeKind,
  Schema,
  InferSchema,
  InferDType,
  StorageType,
} from './core/types';

// Data structures
export { Series } from './core/series';
export type { ISeries, SeriesView } from './core/series';

export { DataFrame } from './core/dataframe';
export type { IDataFrame, DataFrameView } from './core/dataframe';

// Lazy loading for large files
export { LazyFrame } from './core/lazyframe';
export type { ILazyFrame, LazyFrameConfig, LazyFrameView } from './core/lazyframe';

// I/O
export { readCsv, readCsvNode, scanCsv, toCsv, writeCsv } from './io/csv';
export type { CsvOptions, CsvWriteOptions } from './io/csv';
export { toJson, toJsonRecords } from './io/json';

// Errors
export {
  MolniyaError,
  ColumnNotFoundError,
  IndexOutOfBoundsError,
  TypeMismatchError,
  InvalidOperationError,
  FileError,
  ParseError,
  SchemaError,
} from './errors';
