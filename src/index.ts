// Types
export type { Result } from './types/result';
export { ok, err, unwrap } from './types/result';
export { DType } from './types/dtypes';
export type { Schema } from './core/schema';

// Series
export { Series, SeriesStringMethods } from './types/series';

// Core column types
export type { Column } from './core/column';
export { enableNullTracking, setColumnValue, getColumnValue } from './core/column';

// Null utilities
export type { NullBitmap } from './utils/nulls';
export { isNull, setNull, setNotNull, createNullBitmap } from './utils/nulls';

// DataFrame
export type { DataFrame } from './dataframe/dataframe';
export {
  createDataFrame,
  getRowCount,
  getColumnNames,
  getColumn,
  addColumn,
} from './dataframe/dataframe';

// DataFrame factory functions (high-level API)
export { from, fromArrays } from './dataframe/factory';
export type { ColumnSpec, InferSchemaType } from './dataframe/factory';

// Slicing operations
export { head, tail } from './dataframe/slicing';

// Aggregation operations
export { sum, mean, min, max, count, median, mode } from './dataframe/aggregation';

// Operations
export type { FilterOperator } from './types/operators';
export { filter, select } from './dataframe/operations';

// Print/formatting
export { formatDataFrame } from './dataframe/print';
export type { PrintOptions } from './dataframe/print';

// Missing data operations
export { isna, notna, dropna, fillna } from './dataframe/missing';

// Manipulation operations
export { drop, rename } from './dataframe/manipulation';

// Join operations
export type { JoinType } from './dataframe/joins';
export { merge, concat, join } from './dataframe/joins';
// Row operations
export { append, duplicate, dropDuplicates, unique } from './dataframe/row-ops';
// Type conversion operations
export { astype } from './dataframe/convert';

// String operations
export {
  strLower,
  strUpper,
  strStrip,
  strContains,
  strStartsWith,
  strEndsWith,
  strReplace,
  strLen,
} from './dataframe/string-ops';

// Dictionary (for string operations)
export { getString, internString } from './memory/dictionary';

// Sorting utilities
export type { SortDirection, SortSpec } from './utils/sort';
export {
  createRowIndices,
  sortByColumn,
  sortByColumns,
  findGroupBoundaries,
  isSorted,
} from './utils/sort';

// GroupBy
export type { AggFunc, AggSpec } from './dataframe/groupby';
export { groupby } from './dataframe/groupby';

// LazyFrame (Lazy Evaluation)
export { LazyFrame } from './lazyframe/lazyframe';
export type {
  PlanNode,
  ScanPlan,
  FilterPlan,
  SelectPlan,
  GroupByPlan,
} from './lazyframe/plan';
export { QueryPlan, executePlan, optimizePlan } from './lazyframe';

// Cache system
export { CacheManager, getCacheManager, resetCacheManager } from './lazyframe/cache';

// IO operations
export { readCsv, readCsvFromString, scanCsv, scanCsvFromString } from './io';
export type { CsvOptions, CsvScanOptions } from './io';
export {
  ColumnarBatchBuilder,
  DEFAULT_BATCH_BYTES,
} from './io';
export type {
  ColumnarBatch,
  ColumnarBatchColumn,
  ColumnarBatchIterator,
  ColumnarData,
} from './io';

// This will be the main export file for the molniya library
// More exports will be added as we implement more features
