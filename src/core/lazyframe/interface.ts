import type { DataFrame } from '../dataframe';
import type { Series } from '../series';
import type { DTypeKind, InferSchema, Schema } from '../types';

/**
 * Read-only view interface for LazyFrame.
 */
export interface LazyFrameView<S extends Schema> {
  readonly schema: S;
  readonly shape: readonly [rows: number, cols: number];

  col<K extends keyof S>(name: K): Promise<Series<S[K]['kind']>>;
}

/**
 * Configuration options for LazyFrame operations.
 */
export interface LazyFrameConfig {
  /** Maximum memory for row cache in bytes (default: 100MB) */
  maxCacheMemory?: number;

  /** Number of rows per cache chunk (default: 10,000) */
  chunkSize?: number;
}

/**
 * Base interface for LazyFrame - a memory-efficient DataFrame for large files.
 *
 * Unlike DataFrame, LazyFrame keeps data on disk and loads rows on-demand.
 * Operations return new LazyFrame instances with deferred execution.
 */
export interface ILazyFrame<S extends Schema> extends LazyFrameView<S> {
  /** Get first n rows as DataFrame */
  head(n?: number): Promise<DataFrame<S>>;

  /** Get last n rows as DataFrame */
  tail(n?: number): Promise<DataFrame<S>>;

  /** Select specific columns */
  select<K extends keyof S>(...cols: K[]): ILazyFrame<Pick<S, K>>;

  /** Filter rows by predicate (streaming - low memory) */
  filter(fn: (row: InferSchema<S>, index: number) => boolean): Promise<DataFrame<S>>;

  /** Collect all data into a regular DataFrame (loads everything into memory) */
  collect(): Promise<DataFrame<S>>;

  /** Collect with row limit */
  collect(limit: number): Promise<DataFrame<S>>;

  /** Get column names */
  columns(): (keyof S)[];

  /** Print sample of data to console */
  print(): Promise<void>;

  /** Get DataFrame info */
  info(): { rows: number; columns: number; dtypes: Record<string, string>; cached: number };

  /** Clear cached data */
  clearCache(): void;

  /** File path */
  readonly path: string;
}
