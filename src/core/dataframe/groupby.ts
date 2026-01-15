import type { Series } from '../series';
import type { DType, DTypeKind, InferSchema, Schema } from '../types';
import { InvalidOperationError } from '../../errors';

// Forward declaration for DataFrame to avoid circular import
interface DataFrameLike<S extends Schema> {
  shape: readonly [number, number];
  schema: S;
  rows(): IterableIterator<InferSchema<S>>;
  col<K extends keyof S>(name: K): Series<S[K]['kind']>;
  columns(): (keyof S)[];
}

type AggFunction = 'sum' | 'mean' | 'min' | 'max' | 'count' | 'first' | 'last';

/**
 * GroupBy - Split-Apply-Combine operations.
 *
 * Groups a DataFrame by one or more columns and allows
 * aggregation operations on each group.
 *
 * @example
 * ```ts
 * df.groupby('category').agg({ price: 'mean', quantity: 'sum' })
 * ```
 */
export class GroupBy<S extends Schema, K extends keyof S> {
  private readonly _df: DataFrameLike<S>;
  private readonly _groupCols: K[];
  private readonly _groups: Map<string, number[]>;

  constructor(df: DataFrameLike<S>, groupCols: K[]) {
    this._df = df;
    this._groupCols = groupCols;
    this._groups = this._buildGroups();
  }

  private _buildGroups(): Map<string, number[]> {
    const groups = new Map<string, number[]>();
    let idx = 0;

    for (const row of this._df.rows()) {
      // Create group key from group columns
      const keyParts = this._groupCols.map((col) => String(row[col as keyof InferSchema<S>]));
      const key = keyParts.join('|||');

      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key)!.push(idx);
      idx++;
    }

    return groups;
  }

  /**
   * Aggregate groups with specified operations.
   * @example groupby.agg({ price: 'mean', count: 'sum' })
   */
  agg<A extends Partial<Record<keyof S, AggFunction>>>(
    operations: A,
  ): Array<Record<string, unknown>> {
    const results: Array<Record<string, unknown>> = [];

    for (const [groupKey, indices] of this._groups) {
      const row: Record<string, unknown> = {};

      // Add group column values
      const keyParts = groupKey.split('|||');
      this._groupCols.forEach((col, i) => {
        row[col as string] = keyParts[i];
      });

      // Apply aggregations
      for (const [colName, op] of Object.entries(operations)) {
        const series = this._df.col(colName as keyof S);
        const values = indices.map((i) => series.at(i));

        row[colName] = this._aggregate(values, op as AggFunction, series.dtype.kind);
      }

      results.push(row);
    }

    return results;
  }

  /**
   * Shortcut for sum aggregation.
   */
  sum(...cols: (keyof S)[]): Array<Record<string, unknown>> {
    const ops: Partial<Record<keyof S, AggFunction>> = {};
    for (const col of cols) {
      ops[col] = 'sum';
    }
    return this.agg(ops);
  }

  /**
   * Shortcut for mean aggregation.
   */
  mean(...cols: (keyof S)[]): Array<Record<string, unknown>> {
    const ops: Partial<Record<keyof S, AggFunction>> = {};
    for (const col of cols) {
      ops[col] = 'mean';
    }
    return this.agg(ops);
  }

  /**
   * Count rows in each group.
   */
  count(): Array<Record<string, unknown>> {
    const results: Array<Record<string, unknown>> = [];

    for (const [groupKey, indices] of this._groups) {
      const row: Record<string, unknown> = {};

      const keyParts = groupKey.split('|||');
      this._groupCols.forEach((col, i) => {
        row[col as string] = keyParts[i];
      });

      row.count = indices.length;
      results.push(row);
    }

    return results;
  }

  /**
   * Get number of groups.
   */
  get size(): number {
    return this._groups.size;
  }

  private _aggregate(values: (unknown | undefined)[], op: AggFunction, dtype: DTypeKind): unknown {
    const nums = values.filter((v) => v !== undefined && v !== null) as number[];

    switch (op) {
      case 'sum':
        return nums.reduce((a, b) => a + b, 0);
      case 'mean':
        return nums.length > 0 ? nums.reduce((a, b) => a + b, 0) / nums.length : Number.NaN;
      case 'min':
        return nums.length > 0 ? Math.min(...nums) : null;
      case 'max':
        return nums.length > 0 ? Math.max(...nums) : null;
      case 'count':
        return values.length;
      case 'first':
        return values[0];
      case 'last':
        return values[values.length - 1];
      default:
        throw new InvalidOperationError(op, 'not a valid aggregation', `valid options: sum, mean, min, max, count, first, last`);
    }
  }
}
