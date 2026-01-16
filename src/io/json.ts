import type { DataFrame } from '../core/dataframe';
import type { InferSchema, Schema } from '../core/types';

/**
 * Convert DataFrame to JSON string (array of objects).
 *
 * @example
 * ```ts
 * const df = DataFrame.fromColumns({ a: [1, 2], b: ['x', 'y'] });
 * const json = toJson(df);
 * // '[{"a":1,"b":"x"},{"a":2,"b":"y"}]'
 * ```
 */
export function toJson<S extends Schema>(df: DataFrame<S>): string {
  return JSON.stringify(toJsonRecords(df));
}

/**
 * Convert DataFrame to array of row objects.
 * This is more memory-efficient than toJson() for processing.
 *
 * @example
 * ```ts
 * const df = DataFrame.fromColumns({ a: [1, 2], b: ['x', 'y'] });
 * const records = toJsonRecords(df);
 * // [{ a: 1, b: 'x' }, { a: 2, b: 'y' }]
 * ```
 */
export function toJsonRecords<S extends Schema>(df: DataFrame<S>): InferSchema<S>[] {
  const columns = df.columns();
  const records: InferSchema<S>[] = [];

  for (let rowIdx = 0; rowIdx < df.shape[0]; rowIdx++) {
    const row = {} as InferSchema<S>;
    for (const colName of columns) {
      const series = df._columns.get(colName)!;
      const value = series.at(rowIdx);
      // NaN will become null in JSON.stringify automatically
      (row as Record<string, unknown>)[colName as string] = value;
    }
    records.push(row);
  }

  return records;
}
