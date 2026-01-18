import { InvalidOperationError } from '../../errors';
import type { Series } from '../series';
import type { DType, DTypeKind, InferSchema, Prettify, Schema } from '../types';
import type { IDataFrame } from './interface';

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
 * Calculates the schema of the aggregation result.
 */
export type AggSchema<
  S extends Schema,
  K extends keyof S,
  A extends Partial<Record<keyof S, AggFunction>>,
> = Prettify<
  Pick<S, K> & {
    [P in keyof A]: A[P] extends 'count'
      ? DType<'int32'>
      : A[P] extends 'mean'
        ? DType<'float64'>
        : P extends keyof S
          ? S[P]
          : never;
  }
>;

// Simple definition for the factory to avoid circular types
type DataFrameFactory = (data: Record<string, unknown[]>) => any;

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
  private readonly _groups: Map<string, number[]>; // Key -> Row Indices
  private readonly _factory: DataFrameFactory;

  constructor(df: DataFrameLike<S>, groupCols: K[], factory: DataFrameFactory) {
    this._df = df;
    this._groupCols = groupCols;
    this._factory = factory;
    this._groups = this._buildGroups();
  }

  private _buildGroups(): Map<string, number[]> {
    const groups = new Map<string, number[]>();
    let idx = 0;

    // TODO: Optimize key generation to avoid string concatenation if possible
    // For now, this is reasonably fast for JS Map keys
    for (const row of this._df.rows()) {
      // Create group key from group columns
      const keyParts = this._groupCols.map((col) => String(row[col as keyof InferSchema<S>]));
      const key = keyParts.join('|||');

      let group = groups.get(key);
      if (!group) {
        group = [];
        groups.set(key, group);
      }
      group.push(idx);
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
  ): IDataFrame<AggSchema<S, K, A>> {
    // 1. Initialize column buffers
    // We need buffers for Group Columns AND Aggregation Columns
    const groupColBuffers: Record<string, unknown[]> = {};
    for (const col of this._groupCols) {
      groupColBuffers[col as string] = [];
    }

    const aggColBuffers: Record<string, unknown[]> = {};
    const aggOps = Object.entries(operations);
    for (const [colName] of aggOps) {
      aggColBuffers[colName] = [];
    }

    // 2. Iterate groups once
    for (const [, indices] of this._groups) {
      const firstIdx = indices[0];
      if (firstIdx === undefined) continue;

      // a) Push Group Key Values
      for (const col of this._groupCols) {
        // We can just take the value from the first row of the group
        // Direct access via column vector would be faster than this._df.col(col).at()
        // if we exposed raw access, but .at() is what we have on DataFrameLike.
        // Optimization: Get the Series once outside the loop?
        // But we are inside the loop over groups, not rows.
        groupColBuffers[col as string]!.push(this._df.col(col).at(firstIdx));
      }

      // b) Calculate and Push Aggregated Values
      for (const [colName, op] of aggOps) {
        const series = this._df.col(colName as keyof S);
        
        // This mapping of indices to values is O(group_size).
        // It creates a minimal temporary array `values`.
        // To strictly "never create object", we might want to avoid this array,
        // but `_aggregate` needs a list.
        // Creating a small array of numbers is usually fine in JS engines (packed variant).
        const values = indices.map((i) => series.at(i));

        const result = this._aggregate(values, op as AggFunction, series.dtype.kind);
        aggColBuffers[colName]!.push(result);
      }
    }

    // 3. Construct Result
    // Merge buffers
    const resultData = { ...groupColBuffers, ...aggColBuffers };

    // 4. Create DataFrame via Factory
    return this._factory(resultData);
  }

  /**
   * Shortcut for sum aggregation.
   */
  sum<C extends keyof S>(...cols: C[]): IDataFrame<Prettify<Pick<S, K | C>>> {
    const ops: Partial<Record<keyof S, AggFunction>> = {};
    for (const col of cols) {
      ops[col] = 'sum';
    }
    return this.agg(ops) as unknown as IDataFrame<Prettify<Pick<S, K | C>>>;
  }

  /**
   * Shortcut for mean aggregation.
   */
  mean<C extends keyof S>(
    ...cols: C[]
  ): IDataFrame<Prettify<Pick<S, K> & Record<C, DType<'float64'>>>> {
    const ops: Partial<Record<keyof S, AggFunction>> = {};
    for (const col of cols) {
      ops[col] = 'mean';
    }
    return this.agg(ops) as unknown as IDataFrame<
      Prettify<Pick<S, K> & Record<C, DType<'float64'>>>
    >;
  }

  /**
   * Count rows in each group.
   */
  count(): IDataFrame<Prettify<Pick<S, K> & { count: DType<'int32'> }>> {
    const groupColBuffers: Record<string, unknown[]> = {};
    for (const col of this._groupCols) {
      groupColBuffers[col as string] = [];
    }
    const countBuffer: number[] = [];

    for (const [, indices] of this._groups) {
      const firstIdx = indices[0];
      if (firstIdx === undefined) continue;

      for (const col of this._groupCols) {
        groupColBuffers[col as string]!.push(this._df.col(col).at(firstIdx));
      }
      countBuffer.push(indices.length);
    }

    // Explicitly casting return because the factory return type is loose
    return this._factory({
      ...groupColBuffers,
      count: countBuffer,
    }) as unknown as IDataFrame<Prettify<Pick<S, K> & { count: DType<'int32'> }>>;
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
        throw new InvalidOperationError(
          op,
          'not a valid aggregation',
          'valid options: sum, mean, min, max, count, first, last',
        );
    }
  }

  /**
   * Return a string representation of the GroupBy object.
   */
  toString(): string {
    return `GroupBy\n  Columns: [${this._groupCols.join(', ')}]\n  Groups: ${this.size}`;
  }

  /**
   * Print the GroupBy object summary to the console.
   */
  print(): void {
    console.log(this.toString());
  }
}
