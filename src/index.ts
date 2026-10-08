export { Query, fromRows, sum, avg, count, min, max, first, last, type Sink } from "./query.ts";
export {
  col,
  lit,
  call,
  eq,
  ne,
  lt,
  le,
  gt,
  ge,
  add,
  sub,
  mul,
  div,
  and,
  or,
  not,
  isNull,
  coalesce,
  lower,
  upper,
  length,
  contains,
  type Expr,
  type Op,
} from "./expr.ts";
export {
  schema,
  type Schema,
  type SchemaSpec,
  type Field,
  type DataType,
  type Scalar,
  type Row,
} from "./schema.ts";
export {
  fromCsv,
  readCsv,
  csvChunks,
  writeCsv,
  fileIO,
  type CsvOptions,
  type AtomicIO,
  type AtomicWriter,
} from "./csv.ts";
export { Batch, Column, type ColumnStats, type Encoding } from "./column.ts";
export { Context, type ExecutionOptions, type ExecutionMetrics } from "./context.ts";
export { MemoryBudget, type MemorySnapshot, type Allocation } from "./memory.ts";
export { EngineError, type ErrorCode } from "./errors.ts";
export type {
  Source,
  Statistics,
  Plan,
  PhysicalPlan,
  Aggregate,
  SortKey,
  JoinHow,
} from "./plan.ts";
export { DiskSpillStore, type SpillStore } from "./spill.ts";
