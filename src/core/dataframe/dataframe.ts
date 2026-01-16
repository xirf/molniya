import { ColumnNotFoundError, SchemaError } from '../../errors';
import { Series } from '../series';
import type { DType, DTypeKind, InferSchema, RenameSchema, Schema } from '../types';
import * as cols from './columns';
import { formatDataFrame } from './display';
import { GroupBy } from './groupby';
import type { IDataFrame } from './interface';
import * as ops from './operations';

/**
 * DataFrame - A typed 2D columnar data structure.
 *
 * Stores data as a collection of typed Series, providing efficient
 * column-oriented operations and type-safe access.
 *
 * @example
 * ```ts
 * const df = DataFrame.from(
 *   { age: m.int32(), name: m.string() },
 *   [{ age: 25, name: 'Alice' }, { age: 30, name: 'Bob' }]
 * );
 * df.col('age'); // Series<'int32'>
 * df.print();
 * ```
 */
export class DataFrame<S extends Schema> implements IDataFrame<S> {
  readonly schema: S;
  readonly shape: readonly [rows: number, cols: number];

  /** @internal */
  readonly _columns: Map<keyof S, Series<DTypeKind>>;
  /** @internal */
  readonly _columnOrder: (keyof S)[];

  /**
   * Private constructor - use factory methods instead.
   */
  private constructor(
    schema: S,
    columns: Map<keyof S, Series<DTypeKind>>,
    columnOrder: (keyof S)[],
    rowCount: number,
  ) {
    this.schema = schema;
    this._columns = columns;
    this._columnOrder = columnOrder;
    this.shape = [rowCount, columnOrder.length] as const;
  }

  // Factory Methods
  // ===============================================================

  /**
   * Creates a DataFrame from schema and row data.
   */
  static from<S extends Schema>(schema: S, data: InferSchema<S>[]): DataFrame<S> {
    const columnOrder = Object.keys(schema) as (keyof S)[];
    const columns = new Map<keyof S, Series<DTypeKind>>();
    const rowCount = data.length;

    for (const colName of columnOrder) {
      const dtype = schema[colName];
      if (!dtype) continue;
      const values = data.map((row) => row[colName as keyof InferSchema<S>]);

      const series = DataFrame._createSeries(dtype, values);
      columns.set(colName, series);
    }

    return new DataFrame<S>(schema, columns, columnOrder, rowCount);
  }

  /**
   * Creates a DataFrame from column data
   * Automatically infers schema from provided data.
   *
   * @example
   * ```ts
   * const df = DataFrame.fromColumns({
   *   age: [25, 30, 22],               // float64
   *   name: ['Alice', 'Bob', 'Carol'], // string
   *   score: [95.5, 87.2, 91.8]        // float64
   * });
   * ```
   */
  static fromColumns<T extends Record<string, unknown[]>>(
    data: T,
  ): DataFrame<{
    [K in keyof T]: DType<
      T[K] extends (number | null | undefined)[]
        ? 'float64' | 'int32'
        : T[K] extends (string | null | undefined)[]
          ? 'string'
          : T[K] extends (boolean | null | undefined)[]
            ? 'bool'
            : never
    >;
  }> {
    const columnOrder = Object.keys(data);
    const columns = new Map<string, Series<DTypeKind>>();
    const schema: Record<string, DType<DTypeKind>> = {};
    let rowCount = 0;

    for (let i = 0; i < columnOrder.length; i++) {
      const colName = columnOrder[i]!;
      const values = data[colName];

      if (!Array.isArray(values)) {
        throw new SchemaError(
          `Column '${colName}' must be an array`,
          'All column values must be arrays',
        );
      }

      if (i === 0) {
        rowCount = values.length;
      } else if (values.length !== rowCount) {
        throw new SchemaError(
          `Column '${colName}' has ${values.length} rows, expected ${rowCount}`,
          'All columns must have the same length',
        );
      }

      const series = DataFrame._inferAndCreateSeries(values);
      columns.set(colName, series);
      schema[colName] = series.dtype;
    }

    type InferredSchema = {
      [K in keyof T]: DType<
        T[K] extends (number | null | undefined)[]
          ? 'float64' | 'int32'
          : T[K] extends (string | null | undefined)[]
            ? 'string'
            : T[K] extends (boolean | null | undefined)[]
              ? 'bool'
              : never
      >;
    };

    return new DataFrame(
      schema as InferredSchema,
      columns as Map<keyof InferredSchema, Series<DTypeKind>>,
      columnOrder as (keyof InferredSchema)[],
      rowCount,
    );
  }

  /** @internal */
  static _inferAndCreateSeries(values: unknown[]): Series<DTypeKind> {
    const len = values.length;
    if (len === 0) return Series.float64([]);

    // Find first non-null value
    let sampleValue: unknown;
    for (let i = 0; i < len; i++) {
      const v = values[i];
      if (v !== null && v !== undefined) {
        sampleValue = v;
        break;
      }
    }

    if (sampleValue === undefined) {
      return Series.float64(values as number[]);
    }

    const sampleType = typeof sampleValue;

    if (sampleType === 'number') {
      // Check if all are integers (no object creation in loop)
      let allIntegers = true;
      for (let i = 0; i < len; i++) {
        const v = values[i];
        if (v !== null && v !== undefined && !Number.isInteger(v as number)) {
          allIntegers = false;
          break;
        }
      }
      return allIntegers ? Series.int32(values as number[]) : Series.float64(values as number[]);
    }

    if (sampleType === 'string') return Series.string(values as string[]);
    if (sampleType === 'boolean') return Series.bool(values as boolean[]);

    throw new SchemaError(
      `Cannot infer dtype from value type: ${sampleType}`,
      'Supported types: number, string, boolean',
    );
  }

  /**
   * Creates a DataFrame from Series map (internal use).
   */
  static _fromColumns<S extends Schema>(
    schema: S,
    columns: Map<keyof S, Series<DTypeKind>>,
    columnOrder: (keyof S)[],
    rowCount: number,
  ): DataFrame<S> {
    return new DataFrame<S>(schema, columns, columnOrder, rowCount);
  }

  /**
   * Creates an empty DataFrame with given schema.
   */
  static empty<S extends Schema>(schema: S): DataFrame<S> {
    return DataFrame.from(schema, []);
  }

  /**
   * Concatenate DataFrames vertically.
   * All DataFrames must have the same schema (column names and types).
   *
   * @example
   * ```ts
   * const df1 = DataFrame.fromColumns({ a: [1, 2], b: ['x', 'y'] });
   * const df2 = DataFrame.fromColumns({ a: [3, 4], b: ['z', 'w'] });
   * const combined = DataFrame.concat(df1, df2);
   * // combined has 4 rows
   * ```
   */
  static concat<S extends Schema>(...dfs: DataFrame<S>[]): DataFrame<S> {
    if (dfs.length === 0) {
      throw new SchemaError(
        'Cannot concat empty array of DataFrames',
        'Provide at least one DataFrame',
      );
    }
    if (dfs.length === 1) return dfs[0]!;

    const first = dfs[0]!;
    const columnOrder = first._columnOrder;
    const schema = first.schema;

    // Validate schemas match
    for (let i = 1; i < dfs.length; i++) {
      const df = dfs[i]!;
      if (df._columnOrder.length !== columnOrder.length) {
        throw new SchemaError(
          `DataFrame at index ${i} has ${df._columnOrder.length} columns, expected ${columnOrder.length}`,
          'All DataFrames must have the same columns',
        );
      }
      for (const col of columnOrder) {
        if (!df._columns.has(col)) {
          throw new SchemaError(
            `DataFrame at index ${i} is missing column '${String(col)}'`,
            'All DataFrames must have the same columns',
          );
        }
        const dtype1 = schema[col]?.kind;
        const dtype2 = df.schema[col]?.kind;
        if (dtype1 !== dtype2) {
          throw new SchemaError(
            `Column '${String(col)}' has type '${dtype2}' at index ${i}, expected '${dtype1}'`,
            'All DataFrames must have matching column types',
          );
        }
      }
    }

    // Calculate total row count
    let totalRows = 0;
    for (const df of dfs) totalRows += df.shape[0];

    // Concatenate each column
    const newColumns = new Map<keyof S, Series<DTypeKind>>();
    for (const colName of columnOrder) {
      const dtype = schema[colName]!;
      const values: unknown[] = [];
      for (const df of dfs) {
        const series = df._columns.get(colName)!;
        for (let i = 0; i < series.length; i++) {
          values.push(series.at(i));
        }
      }
      newColumns.set(colName, DataFrame._createSeries(dtype, values));
    }

    return DataFrame._fromColumns(schema, newColumns, columnOrder, totalRows);
  }

  /**
   * SQL-like join with another DataFrame.
   *
   * @example
   * ```ts
   * const users = DataFrame.fromColumns({ id: [1, 2], name: ['Alice', 'Bob'] });
   * const orders = DataFrame.fromColumns({ userId: [1, 1, 2], product: ['Apple', 'Banana', 'Cherry'] });
   * const joined = users.merge(orders, { left: 'id', right: 'userId', how: 'inner' });
   * ```
   */
  merge<R extends Schema>(
    right: DataFrame<R>,
    options: {
      left: keyof S;
      right: keyof R;
      how?: 'inner' | 'left' | 'right' | 'outer';
    },
  ): DataFrame<S & Omit<R, keyof S>> {
    const { left: leftCol, right: rightCol, how = 'inner' } = options;

    // Build hash index on right DataFrame
    const rightIndex = new Map<unknown, number[]>();
    const rightSeries = right._columns.get(rightCol)!;
    for (let i = 0; i < right.shape[0]; i++) {
      const key = rightSeries.at(i);
      const existing = rightIndex.get(key);
      if (existing) {
        existing.push(i);
      } else {
        rightIndex.set(key, [i]);
      }
    }

    // Build hash index on left DataFrame for right/outer joins
    const leftIndex = new Map<unknown, number[]>();
    const leftSeries = this._columns.get(leftCol)!;
    for (let i = 0; i < this.shape[0]; i++) {
      const key = leftSeries.at(i);
      const existing = leftIndex.get(key);
      if (existing) {
        existing.push(i);
      } else {
        leftIndex.set(key, [i]);
      }
    }

    // Collect matched row pairs
    const leftIndices: (number | null)[] = [];
    const rightIndices: (number | null)[] = [];
    const usedRightIndices = new Set<number>();

    // Process left rows
    for (let leftIdx = 0; leftIdx < this.shape[0]; leftIdx++) {
      const key = leftSeries.at(leftIdx);
      const rightMatches = rightIndex.get(key);

      if (rightMatches && rightMatches.length > 0) {
        for (const rightIdx of rightMatches) {
          leftIndices.push(leftIdx);
          rightIndices.push(rightIdx);
          usedRightIndices.add(rightIdx);
        }
      } else if (how === 'left' || how === 'outer') {
        leftIndices.push(leftIdx);
        rightIndices.push(null);
      }
    }

    // Add unmatched right rows for right/outer joins
    if (how === 'right' || how === 'outer') {
      for (let rightIdx = 0; rightIdx < right.shape[0]; rightIdx++) {
        if (!usedRightIndices.has(rightIdx)) {
          leftIndices.push(null);
          rightIndices.push(rightIdx);
        }
      }
    }

    // Build result columns
    type MergedSchema = S & Omit<R, keyof S>;
    const resultColumns = new Map<keyof MergedSchema, Series<DTypeKind>>();
    const resultColumnOrder: (keyof MergedSchema)[] = [];
    const resultSchema = { ...this.schema } as unknown as MergedSchema;

    // Add left columns
    for (const colName of this._columnOrder) {
      const series = this._columns.get(colName)!;
      const dtype = this.schema[colName]!;
      const values: unknown[] = leftIndices.map((idx) => (idx !== null ? series.at(idx) : null));
      resultColumns.set(colName as keyof MergedSchema, DataFrame._createSeries(dtype, values));
      resultColumnOrder.push(colName as keyof MergedSchema);
    }

    // Add right columns (excluding join key if same name)
    for (const colName of right._columnOrder) {
      if (this._columns.has(colName as unknown as keyof S)) continue;
      const series = right._columns.get(colName)!;
      const dtype = right.schema[colName]!;
      const values: unknown[] = rightIndices.map((idx) => (idx !== null ? series.at(idx) : null));
      resultColumns.set(colName as keyof MergedSchema, DataFrame._createSeries(dtype, values));
      resultColumnOrder.push(colName as keyof MergedSchema);
      (resultSchema as Record<string, unknown>)[colName as string] = dtype;
    }

    return DataFrame._fromColumns(
      resultSchema,
      resultColumns,
      resultColumnOrder,
      leftIndices.length,
    );
  }

  /**
   * Return DataFrame with unique rows (alias for dropDuplicates with no args).
   */
  unique(): DataFrame<S> {
    return this.dropDuplicates();
  }

  /** @internal */
  static _createSeries(dtype: DType<DTypeKind>, values: unknown[]): Series<DTypeKind> {
    switch (dtype.kind) {
      case 'float64':
        return Series.float64(values as number[]);
      case 'int32':
        return Series.int32(values as number[]);
      case 'string':
        return Series.string(values as string[]);
      case 'bool':
        return Series.bool(values as boolean[]);
      default:
        throw new SchemaError(
          `unknown dtype '${dtype.kind}'`,
          'supported types: float64, int32, string, bool',
        );
    }
  }

  // Column Access
  // ===============================================================

  /**
   * Gets a column as a typed Series.
   * Type is inferred from the schema.
   */
  col<K extends keyof S>(name: K): Series<S[K]['kind']> {
    const series = this._columns.get(name);
    if (!series) {
      throw new ColumnNotFoundError(String(name), this._columnOrder.map(String));
    }
    return series as Series<S[K]['kind']>;
  }

  /**
   * Gets column names in order.
   */
  columns(): (keyof S)[] {
    return [...this._columnOrder];
  }

  // Row Operations
  // ===============================================================

  /**
   * Returns first n rows (default: 5).
   */
  head(n = 5): DataFrame<S> {
    const len = Math.min(n, this.shape[0]);
    const newColumns = new Map<keyof S, Series<DTypeKind>>();

    for (const [name, series] of this._columns) {
      newColumns.set(name, series.head(len));
    }

    return DataFrame._fromColumns(this.schema, newColumns, this._columnOrder, len);
  }

  /**
   * Returns last n rows (default: 5).
   */
  tail(n = 5): DataFrame<S> {
    const len = Math.min(n, this.shape[0]);
    const newColumns = new Map<keyof S, Series<DTypeKind>>();

    for (const [name, series] of this._columns) {
      newColumns.set(name, series.tail(len));
    }

    return DataFrame._fromColumns(this.schema, newColumns, this._columnOrder, len);
  }

  /**
   * Selects specific columns.
   */
  select<K extends keyof S>(...cols: K[]): DataFrame<Pick<S, K>> {
    const newSchema = {} as Pick<S, K>;
    const newColumns = new Map<K, Series<DTypeKind>>();

    for (const colName of cols) {
      newSchema[colName] = this.schema[colName];
      newColumns.set(colName, this._columns.get(colName)!);
    }

    return DataFrame._fromColumns(
      newSchema,
      newColumns as Map<keyof Pick<S, K>, Series<DTypeKind>>,
      cols,
      this.shape[0],
    );
  }

  // Iteration
  // ===============================================================

  /**
   * Iterates over rows as objects.
   */
  *rows(): IterableIterator<InferSchema<S>> {
    for (let i = 0; i < this.shape[0]; i++) {
      const row = {} as InferSchema<S>;
      for (const colName of this._columnOrder) {
        const series = this._columns.get(colName)!;
        (row as Record<string, unknown>)[colName as string] = series.at(i);
      }
      yield row;
    }
  }

  // Filtering & Selection (delegated to operations.ts)
  // ===============================================================

  /**
   * Filter rows by predicate function.
   * Returns a new DataFrame with only matching rows.
   */
  filter(fn: (row: InferSchema<S>, index: number) => boolean): DataFrame<S> {
    return ops.filter(this, fn);
  }

  /**
   * SQL-like filtering on a column.
   * @example df.where('age', '>', 25)
   */
  where<K extends keyof S>(
    column: K,
    op: '=' | '!=' | '>' | '>=' | '<' | '<=' | 'in' | 'contains',
    value: unknown,
  ): DataFrame<S> {
    return ops.where(this, column, op, value);
  }

  /**
   * Sort DataFrame by one or more columns.
   * @example df.sort('age') or df.sort('age', false)
   */
  sort<K extends keyof S>(column: K, ascending = true): DataFrame<S> {
    return ops.sort(this, column, ascending);
  }

  // Groupby & Aggregation
  // ===============================================================

  /**
   * Group by one or more columns.
   * Returns a GroupBy object for aggregation.
   */
  groupby<K extends keyof S>(...columns: K[]): GroupBy<S, K> {
    return new GroupBy(this, columns);
  }

  // Apply & Transform (delegated to operations.ts)
  // ===============================================================

  /**
   * Apply a function to each row.
   * Returns an array of results.
   */
  apply<R>(fn: (row: InferSchema<S>, index: number) => R): R[] {
    return ops.apply(this, fn);
  }

  /**
   * Summary statistics for all numeric columns.
   */
  describe(): Record<
    string,
    { count: number; mean: number; std: number; min: number; max: number }
  > {
    return ops.describe(this);
  }

  /**
   * Get basic info about the DataFrame.
   */
  info(): { rows: number; columns: number; dtypes: Record<string, string> } {
    return ops.info(this);
  }

  /**
   * Convert DataFrame to array of row objects.
   */
  toArray(): InferSchema<S>[] {
    return [...this.rows()];
  }

  // Column Manipulation (delegated to columns.ts)
  // ===============================================================

  /**
   * Drop specified columns.
   * Returns a new DataFrame without those columns.
   */
  drop<K extends keyof S>(...columns: K[]): DataFrame<Omit<S, K>> {
    return cols.drop(this, DataFrame._fromColumns, columns);
  }

  /**
   * Rename columns by mapping.
   * Returns a new DataFrame with renamed columns.
   */
  rename<const M extends { [K in keyof S]?: string }>(mapping: M): DataFrame<RenameSchema<S, M>> {
    // Cast through unknown since TypeScript can't prove RenameSchema<S,M> extends Schema
    // at the generic level, though it always does at concrete instantiation
    return cols.rename(this, DataFrame._fromColumns, mapping) as unknown as DataFrame<
      RenameSchema<S, M>
    >;
  }

  /**
   * Add or replace a column.
   * Accepts an array of values or a function that computes values from each row.
   */
  assign<NewCol extends string, D extends DTypeKind>(
    name: NewCol,
    values: unknown[] | ((row: InferSchema<S>, index: number) => unknown),
  ): DataFrame<S & Record<NewCol, DType<D>>> {
    return cols.assign(this, DataFrame._fromColumns, name, values) as unknown as DataFrame<
      S & Record<NewCol, DType<D>>
    >;
  }

  // Missing Value Operations (delegated to columns.ts)
  // ===============================================================

  /**
   * Drop rows with any missing values (null, undefined, NaN).
   */
  dropna(): DataFrame<S> {
    return cols.dropna(this);
  }

  /**
   * Fill missing values with specified value.
   * Returns a new DataFrame.
   */
  fillna(value: number | string | boolean): DataFrame<S> {
    return cols.fillna(this, DataFrame._fromColumns, value);
  }

  /**
   * Detect missing values.
   * Returns a DataFrame of booleans indicating missing values.
   */
  isna(): DataFrame<{ [K in keyof S]: DType<'bool'> }> {
    return cols.isna(this, DataFrame._fromColumns);
  }

  // Copying & Sampling (delegated to columns.ts)
  // ===============================================================

  /**
   * Create a deep copy of the DataFrame.
   */
  copy(): DataFrame<S> {
    return cols.copy(this, DataFrame._fromColumns);
  }

  /**
   * Random sample of n rows.
   * Returns a new DataFrame.
   */
  sample(n: number): DataFrame<S> {
    return cols.sample(this, n);
  }

  // Indexing (delegated to columns.ts)
  // ===============================================================

  /**
   * Integer-location based indexing.
   * Single index returns row object, range returns DataFrame.
   */
  iloc(index: number): InferSchema<S>;
  iloc(range: string): DataFrame<S>;
  iloc(start: number, end: number): DataFrame<S>;
  iloc(startOrIndexOrRange: number | string, end?: number): InferSchema<S> | DataFrame<S> {
    if (typeof startOrIndexOrRange === 'string') {
      return cols.ilocString(this, startOrIndexOrRange);
    }

    if (end === undefined) {
      return cols.ilocSingle(this, startOrIndexOrRange);
    }
    return cols.ilocRange(this, startOrIndexOrRange, end);
  }

  /**
   * Select rows by array of indices.
   */
  loc(indices: number[]): DataFrame<S> {
    return this._selectRows(indices);
  }

  // Data Cleaning (delegated to columns.ts)
  // ===============================================================

  /**
   * Remove duplicate rows.
   * @param columns Optional columns to consider for determining duplicates
   */
  dropDuplicates<K extends keyof S>(...columns: K[]): DataFrame<S> {
    const colsToCheck = columns.length > 0 ? columns : undefined;
    return cols.dropDuplicates(this, colsToCheck);
  }

  /**
   * Replace values across all columns.
   * @param oldValue Value to find and replace
   * @param newValue Replacement value
   */
  replace(oldValue: unknown, newValue: unknown): DataFrame<S> {
    return cols.replace(this, DataFrame._fromColumns, oldValue, newValue);
  }

  /**
   * Clip numeric columns to a range.
   * Non-numeric columns are unchanged.
   * @param min Minimum value (values below are set to min)
   * @param max Maximum value (values above are set to max)
   */
  clip(min?: number, max?: number): DataFrame<S> {
    return cols.clip(this, DataFrame._fromColumns, min, max);
  }

  /**
   * Forward fill missing values.
   * Replaces NaN/null with the previous valid value in each column.
   */
  ffill(): DataFrame<S> {
    return cols.ffill(this, DataFrame._fromColumns);
  }

  /**
   * Backward fill missing values.
   * Replaces NaN/null with the next valid value in each column.
   */
  bfill(): DataFrame<S> {
    return cols.bfill(this, DataFrame._fromColumns);
  }

  // Internal Helpers
  // ===============================================================

  /** @internal */
  _selectRows(indices: number[]): DataFrame<S> {
    const newColumns = new Map<keyof S, Series<DTypeKind>>();

    for (const [colName, series] of this._columns) {
      const dtype = this.schema[colName];
      if (!dtype) continue;
      const values: unknown[] = indices.map((i) => series.at(i));
      newColumns.set(colName, DataFrame._createSeries(dtype, values));
    }

    return DataFrame._fromColumns(this.schema, newColumns, this._columnOrder, indices.length);
  }

  // Display (delegated to display.ts)
  // ===============================================================

  /**
   * Prints DataFrame to console as ASCII table.
   */
  print(): void {
    console.log(this.toString());
  }

  /**
   * Formats DataFrame as ASCII table string.
   */
  toString(): string {
    return formatDataFrame(this);
  }
}
