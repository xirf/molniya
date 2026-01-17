import type { Series } from '../series';
import type { DTypeKind, InferSchema, Schema } from '../types';
import type { DataFrame } from './dataframe';

/**
 * Internal DataFrame context for operations.
 * Provides access to DataFrame internals without circular imports.
 */
export interface OperationsContext<S extends Schema> {
  readonly schema: S;
  readonly shape: readonly [rows: number, cols: number];
  _columns: Map<keyof S, Series<DTypeKind>>;
  _columnOrder: (keyof S)[];
  rows(): IterableIterator<InferSchema<S>>;
  _selectRows(indices: number[]): DataFrame<S>;
}

/**
 * Filter rows by predicate function.
 */
export function filter<S extends Schema>(
  ctx: OperationsContext<S>,
  fn: (row: InferSchema<S>, index: number) => boolean,
): DataFrame<S> {
  const matchingIndices: number[] = [];
  let idx = 0;

  for (const row of ctx.rows()) {
    if (fn(row, idx)) {
      matchingIndices.push(idx);
    }
    idx++;
  }

  return ctx._selectRows(matchingIndices);
}

/**
 * SQL-like filtering on a column.
 */
export function where<S extends Schema, K extends keyof S>(
  ctx: OperationsContext<S>,
  column: K,
  op: '=' | '!=' | '>' | '>=' | '<' | '<=' | 'in' | 'contains',
  value: unknown,
): DataFrame<S> {
  const series = ctx._columns.get(column)!;
  const matchingIndices: number[] = [];

  for (let i = 0; i < ctx.shape[0]; i++) {
    const cellValue = series.at(i);
    let matches = false;

    switch (op) {
      case '=':
        matches = cellValue === value;
        break;
      case '!=':
        matches = cellValue !== value;
        break;
      case '>':
        matches = Number(cellValue) > Number(value);
        break;
      case '>=':
        matches = Number(cellValue) >= Number(value);
        break;
      case '<':
        matches = Number(cellValue) < Number(value);
        break;
      case '<=':
        matches = Number(cellValue) <= Number(value);
        break;
      case 'in':
        matches = (value as unknown[]).includes(cellValue);
        break;
      case 'contains':
        matches = String(cellValue).includes(String(value));
        break;
    }

    if (matches) matchingIndices.push(i);
  }

  return ctx._selectRows(matchingIndices);
}

/**
 * Sort DataFrame by column.
 */
export function sort<S extends Schema, K extends keyof S>(
  ctx: OperationsContext<S>,
  column: K,
  ascending = true,
): DataFrame<S> {
  const series = ctx._columns.get(column)!;

  // Create index array and sort it
  const indices = Array.from({ length: ctx.shape[0] }, (_, i) => i);

  indices.sort((a, b) => {
    const valA = series.at(a);
    const valB = series.at(b);

    if (typeof valA === 'number' && typeof valB === 'number') {
      return ascending ? valA - valB : valB - valA;
    }

    const strA = String(valA);
    const strB = String(valB);
    return ascending ? strA.localeCompare(strB) : strB.localeCompare(strA);
  });

  return ctx._selectRows(indices);
}

/**
 * Apply a function to each row.
 */
export function apply<S extends Schema, R>(
  ctx: OperationsContext<S>,
  fn: (row: InferSchema<S>, index: number) => R,
): R[] {
  const results: R[] = [];
  let idx = 0;
  for (const row of ctx.rows()) {
    results.push(fn(row, idx++));
  }
  return results;
}

/**
 * Summary statistics for all numeric columns.
 */
export function describe<S extends Schema>(
  ctx: OperationsContext<S>,
): Record<string, { count: number; mean: number; std: number; min: number; max: number }> {
  const result: Record<
    string,
    { count: number; mean: number; std: number; min: number; max: number }
  > = {};

  for (const colName of ctx._columnOrder) {
    const series = ctx._columns.get(colName)!;
    if (series.dtype.kind === 'float64' || series.dtype.kind === 'int32') {
      result[colName as string] = (series as Series<'float64' | 'int32'>).describe();
    }
  }

  return result;
}

/**
 * Get basic info about the DataFrame.
 */
export function info<S extends Schema>(
  ctx: OperationsContext<S>,
): { rows: number; columns: number; dtypes: Record<string, string> } {
  const dtypes: Record<string, string> = {};
  for (const colName of ctx._columnOrder) {
    const dtype = ctx.schema[colName];
    if (dtype) dtypes[colName as string] = dtype.kind;
  }
  return {
    rows: ctx.shape[0],
    columns: ctx.shape[1],
    dtypes,
  };
}
