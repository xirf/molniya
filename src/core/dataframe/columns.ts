import type { DataFrame } from './dataframe';
import type { DType, DTypeKind, InferSchema, Schema } from '../types';
import { Series } from '../series';

/**
 * Internal DataFrame context for column operations.
 * Provides access to DataFrame internals without circular imports.
 */
export interface ColumnContext<S extends Schema> {
  readonly schema: S;
  readonly shape: readonly [rows: number, cols: number];
  _columns: Map<keyof S, Series<DTypeKind>>;
  _columnOrder: (keyof S)[];
  rows(): IterableIterator<InferSchema<S>>;
  _selectRows(indices: number[]): DataFrame<S>;
}

/**
 * Drop specified columns from a DataFrame.
 */
export function drop<S extends Schema, K extends keyof S>(
  ctx: ColumnContext<S>,
  fromColumns: (
    schema: S,
    columns: Map<keyof S, Series<DTypeKind>>,
    columnOrder: (keyof S)[],
    rowCount: number,
  ) => DataFrame<S>,
  columns: K[],
): DataFrame<Omit<S, K>> {
  const dropSet = new Set<keyof S>(columns);
  const newColumnOrder = ctx._columnOrder.filter((c) => !dropSet.has(c));

  const newSchema = {} as Omit<S, K>;
  const newColumns = new Map<keyof Omit<S, K>, Series<DTypeKind>>();

  for (const colName of newColumnOrder) {
    (newSchema as Record<string, unknown>)[colName as string] = ctx.schema[colName];
    newColumns.set(colName as keyof Omit<S, K>, ctx._columns.get(colName)!);
  }

  return fromColumns(
    newSchema as unknown as S,
    newColumns as unknown as Map<keyof S, Series<DTypeKind>>,
    newColumnOrder as unknown as (keyof S)[],
    ctx.shape[0],
  ) as unknown as DataFrame<Omit<S, K>>;
}

/**
 * Rename columns by mapping.
 */
export function rename<S extends Schema, M extends Partial<Record<keyof S, string>>>(
  ctx: ColumnContext<S>,
  fromColumns: (
    schema: S,
    columns: Map<keyof S, Series<DTypeKind>>,
    columnOrder: (keyof S)[],
    rowCount: number,
  ) => DataFrame<S>,
  mapping: M,
): DataFrame<S> {
  const newColumnOrder: (keyof S)[] = [];
  const newSchema = {} as S;
  const newColumns = new Map<keyof S, Series<DTypeKind>>();

  for (const colName of ctx._columnOrder) {
    const newName = (mapping[colName] ?? colName) as keyof S;
    newColumnOrder.push(newName);
    (newSchema as Record<string, unknown>)[newName as string] = ctx.schema[colName];
    newColumns.set(newName, ctx._columns.get(colName)!);
  }

  return fromColumns(newSchema, newColumns, newColumnOrder, ctx.shape[0]);
}

/**
 * Add or replace a column.
 */
export function assign<S extends Schema, NewCol extends string, D extends DTypeKind>(
  ctx: ColumnContext<S>,
  fromColumns: (
    schema: S,
    columns: Map<keyof S, Series<DTypeKind>>,
    columnOrder: (keyof S)[],
    rowCount: number,
  ) => DataFrame<S>,
  name: NewCol,
  values: unknown[] | ((row: InferSchema<S>, index: number) => unknown),
): DataFrame<S & Record<NewCol, DType<D>>> {
  let columnData: unknown[];

  if (typeof values === 'function') {
    columnData = [];
    let idx = 0;
    for (const row of ctx.rows()) {
      columnData.push(values(row, idx++));
    }
  } else {
    columnData = values;
  }

  // Infer dtype from first non-null value
  const firstVal = columnData.find((v) => v !== null && v !== undefined);
  let newSeries: Series<DTypeKind>;

  if (typeof firstVal === 'number') {
    if (Number.isInteger(firstVal)) {
      newSeries = Series.int32(columnData as number[]);
    } else {
      newSeries = Series.float64(columnData as number[]);
    }
  } else if (typeof firstVal === 'boolean') {
    newSeries = Series.bool(columnData as boolean[]);
  } else {
    newSeries = Series.string(columnData as string[]);
  }

  // Build new schema and columns
  type NewSchema = S & Record<NewCol, DType<D>>;
  const newSchema = { ...ctx.schema, [name]: newSeries.dtype } as NewSchema;
  const newColumns = new Map<keyof NewSchema, Series<DTypeKind>>();

  for (const [colName, series] of ctx._columns) {
    newColumns.set(colName as keyof NewSchema, series);
  }
  newColumns.set(name as keyof NewSchema, newSeries);

  const newColumnOrder = [...ctx._columnOrder, name] as (keyof NewSchema)[];

  return fromColumns(
    newSchema as unknown as S,
    newColumns as unknown as Map<keyof S, Series<DTypeKind>>,
    newColumnOrder as unknown as (keyof S)[],
    ctx.shape[0],
  ) as unknown as DataFrame<NewSchema>;
}

/**
 * Drop rows with any missing values.
 */
export function dropna<S extends Schema>(ctx: ColumnContext<S>): DataFrame<S> {
  const matchingIndices: number[] = [];

  for (let i = 0; i < ctx.shape[0]; i++) {
    let hasMissing = false;

    for (const colName of ctx._columnOrder) {
      const series = ctx._columns.get(colName)!;
      const value = series.at(i);

      if (
        value === null ||
        value === undefined ||
        (typeof value === 'number' && Number.isNaN(value))
      ) {
        hasMissing = true;
        break;
      }
    }

    if (!hasMissing) {
      matchingIndices.push(i);
    }
  }

  return ctx._selectRows(matchingIndices);
}

/**
 * Fill missing values with specified value.
 */
export function fillna<S extends Schema>(
  ctx: ColumnContext<S>,
  fromColumns: (
    schema: S,
    columns: Map<keyof S, Series<DTypeKind>>,
    columnOrder: (keyof S)[],
    rowCount: number,
  ) => DataFrame<S>,
  value: number | string | boolean,
): DataFrame<S> {
  const newColumns = new Map<keyof S, Series<DTypeKind>>();

  for (const [colName, series] of ctx._columns) {
    newColumns.set(colName, series.fillna(value as never));
  }

  return fromColumns(ctx.schema, newColumns, ctx._columnOrder, ctx.shape[0]);
}

/**
 * Detect missing values.
 */
export function isna<S extends Schema>(
  ctx: ColumnContext<S>,
  fromColumns: <T extends Schema>(
    schema: T,
    columns: Map<keyof T, Series<DTypeKind>>,
    columnOrder: (keyof T)[],
    rowCount: number,
  ) => DataFrame<T>,
): DataFrame<{ [K in keyof S]: DType<'bool'> }> {
  type BoolSchema = { [K in keyof S]: DType<'bool'> };
  const newSchema = {} as BoolSchema;
  const newColumns = new Map<keyof BoolSchema, Series<DTypeKind>>();

  for (const colName of ctx._columnOrder) {
    const series = ctx._columns.get(colName)!;
    (newSchema as Record<string, DType<'bool'>>)[colName as string] = { kind: 'bool', nullable: false };
    newColumns.set(colName as keyof BoolSchema, series.isna());
  }

  return fromColumns<BoolSchema>(
    newSchema,
    newColumns,
    ctx._columnOrder as (keyof BoolSchema)[],
    ctx.shape[0],
  );
}

/**
 * Create a deep copy of the DataFrame.
 */
export function copy<S extends Schema>(
  ctx: ColumnContext<S>,
  fromColumns: (
    schema: S,
    columns: Map<keyof S, Series<DTypeKind>>,
    columnOrder: (keyof S)[],
    rowCount: number,
  ) => DataFrame<S>,
): DataFrame<S> {
  const newColumns = new Map<keyof S, Series<DTypeKind>>();

  for (const [colName, series] of ctx._columns) {
    newColumns.set(colName, series.copy());
  }

  return fromColumns(ctx.schema, newColumns, [...ctx._columnOrder], ctx.shape[0]);
}

/**
 * Random sample of n rows.
 */
export function sample<S extends Schema>(ctx: ColumnContext<S>, n: number): DataFrame<S> {
  const numRows = Math.min(n, ctx.shape[0]);
  const allIndices = Array.from({ length: ctx.shape[0] }, (_, i) => i);

  // Fisher-Yates shuffle for first n elements
  for (let i = 0; i < numRows; i++) {
    const j = i + Math.floor(Math.random() * (allIndices.length - i));
    [allIndices[i], allIndices[j]] = [allIndices[j]!, allIndices[i]!];
  }

  const sampleIndices = allIndices.slice(0, numRows);
  return ctx._selectRows(sampleIndices);
}

/**
 * Integer-location based indexing - single row.
 */
export function ilocSingle<S extends Schema>(ctx: ColumnContext<S>, index: number): InferSchema<S> {
  const row = {} as InferSchema<S>;
  for (const colName of ctx._columnOrder) {
    const series = ctx._columns.get(colName)!;
    (row as Record<string, unknown>)[colName as string] = series.at(index);
  }
  return row;
}

/**
 * Integer-location based indexing - range.
 */
export function ilocRange<S extends Schema>(
  ctx: ColumnContext<S>,
  start: number,
  end: number,
): DataFrame<S> {
  const indices: number[] = [];
  const resolvedEnd = Math.min(end, ctx.shape[0]);
  for (let i = start; i < resolvedEnd; i++) {
    indices.push(i);
  }
  return ctx._selectRows(indices);
}

/**
 * Remove duplicate rows.
 * @param cols Optional columns to consider for determining duplicates
 */
export function dropDuplicates<S extends Schema, K extends keyof S>(
  ctx: ColumnContext<S>,
  cols?: K[],
): DataFrame<S> {
  const checkCols = cols ?? (ctx._columnOrder as K[]);
  const seen = new Set<string>();
  const keepIndices: number[] = [];

  for (let i = 0; i < ctx.shape[0]; i++) {
    // Create a key from the values of the check columns
    const keyParts: string[] = [];
    for (const colName of checkCols) {
      const series = ctx._columns.get(colName)!;
      const val = series.at(i);
      keyParts.push(String(val));
    }
    const key = keyParts.join('\0'); // Use null char as separator

    if (!seen.has(key)) {
      seen.add(key);
      keepIndices.push(i);
    }
  }

  return ctx._selectRows(keepIndices);
}

/**
 * Replace values across all columns.
 */
export function replace<S extends Schema>(
  ctx: ColumnContext<S>,
  fromColumns: (
    schema: S,
    columns: Map<keyof S, Series<DTypeKind>>,
    columnOrder: (keyof S)[],
    rowCount: number,
  ) => DataFrame<S>,
  oldValue: unknown,
  newValue: unknown,
): DataFrame<S> {
  const newColumns = new Map<keyof S, Series<DTypeKind>>();

  for (const [colName, series] of ctx._columns) {
    newColumns.set(colName, series.replace(oldValue as never, newValue as never));
  }

  return fromColumns(ctx.schema, newColumns, ctx._columnOrder, ctx.shape[0]);
}

/**
 * Clip numeric columns to a range.
 */
export function clip<S extends Schema>(
  ctx: ColumnContext<S>,
  fromColumns: (
    schema: S,
    columns: Map<keyof S, Series<DTypeKind>>,
    columnOrder: (keyof S)[],
    rowCount: number,
  ) => DataFrame<S>,
  min?: number,
  max?: number,
): DataFrame<S> {
  const newColumns = new Map<keyof S, Series<DTypeKind>>();

  for (const [colName, series] of ctx._columns) {
    if (series.dtype.kind === 'float64' || series.dtype.kind === 'int32') {
      newColumns.set(colName, series.clip(min, max));
    } else {
      newColumns.set(colName, series);
    }
  }

  return fromColumns(ctx.schema, newColumns, ctx._columnOrder, ctx.shape[0]);
}

/**
 * Forward fill missing values across all columns.
 */
export function ffill<S extends Schema>(
  ctx: ColumnContext<S>,
  fromColumns: (
    schema: S,
    columns: Map<keyof S, Series<DTypeKind>>,
    columnOrder: (keyof S)[],
    rowCount: number,
  ) => DataFrame<S>,
): DataFrame<S> {
  const newColumns = new Map<keyof S, Series<DTypeKind>>();

  for (const [colName, series] of ctx._columns) {
    newColumns.set(colName, series.ffill());
  }

  return fromColumns(ctx.schema, newColumns, ctx._columnOrder, ctx.shape[0]);
}

/**
 * Backward fill missing values across all columns.
 */
export function bfill<S extends Schema>(
  ctx: ColumnContext<S>,
  fromColumns: (
    schema: S,
    columns: Map<keyof S, Series<DTypeKind>>,
    columnOrder: (keyof S)[],
    rowCount: number,
  ) => DataFrame<S>,
): DataFrame<S> {
  const newColumns = new Map<keyof S, Series<DTypeKind>>();

  for (const [colName, series] of ctx._columns) {
    newColumns.set(colName, series.bfill());
  }

  return fromColumns(ctx.schema, newColumns, ctx._columnOrder, ctx.shape[0]);
}
