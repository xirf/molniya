import type { ISeries } from '../series';
import type { DTypeKind, InferSchema, Schema } from '../types';

/**
 * Read-only view interface for DataFrame.
 */
export interface DataFrameView<S extends Schema> {
  readonly schema: S;
  readonly shape: readonly [rows: number, cols: number];

  col<K extends keyof S>(name: K): ISeries<S[K]['kind']>;
}

/**
 * Base interface for all DataFrame types.
 */
export interface IDataFrame<S extends Schema> extends DataFrameView<S> {
  /** Get first n rows */
  head(n?: number): IDataFrame<S>;

  /** Get last n rows */
  tail(n?: number): IDataFrame<S>;

  /** Select specific columns */
  select<K extends keyof S>(...cols: K[]): IDataFrame<Pick<S, K>>;

  /** Iterate over rows */
  rows(): IterableIterator<InferSchema<S>>;

  /** Get column names */
  columns(): (keyof S)[];

  /** Print formatted output to console */
  print(): void;

  /** Format as string representation */
  toString(): string;
}
