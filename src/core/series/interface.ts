import type { DType, DTypeKind, InferDType, StorageType } from '../types';

/**
 * Read-only view interface for Series.
 * Enables zero-copy slicing operations.
 */
export interface SeriesView<T extends DTypeKind> {
  readonly dtype: DType<T>;
  readonly length: number;

  at(index: number): InferDType<DType<T>> | undefined;

  [Symbol.iterator](): Iterator<InferDType<DType<T>>>;
}

/**
 * Base interface for all Series types.
 */
export interface ISeries<T extends DTypeKind> extends SeriesView<T> {
  /** Get first n elements */
  head(n?: number): ISeries<T>;

  /** Get last n elements */
  tail(n?: number): ISeries<T>;

  /** Slice elements from start to end */
  slice(start: number, end?: number): ISeries<T>;

  /** Get iterator over values */
  values(): IterableIterator<InferDType<DType<T>>>;

  /** Print formatted output to console */
  print(): void;

  /** Format as string representation */
  toString(): string;

  /** Get underlying storage (for internal use) */
  _storage(): StorageType<T>;

  /** Ordinal encoding - maps categories to integers */
  toOrdinal(): ISeries<'int32'>;
}
