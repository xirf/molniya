import type { DType, DTypeKind, InferDType, StorageType } from '../types';
import type { ISeries } from './interface';
import { TypeMismatchError } from '../../errors';
import { createStorageFrom } from './storage';
import { StringAccessor } from './string-accessor';

/**
 * Series - A typed 1D array with Pandas-like operations.
 *
 * Wraps TypedArrays (Float64Array, Int32Array) for numerics and
 * regular arrays for strings. Provides zero-copy slicing via views.
 *
 * @example
 * ```ts
 * const ages = Series.float64([25, 30, 22]);
 * const firstTwo = ages.head(2);
 * ages.print();
 * ```
 */
export class Series<T extends DTypeKind> implements ISeries<T> {
  readonly dtype: DType<T>;
  readonly length: number;

  private readonly _data: StorageType<T>;
  private readonly _offset: number;
  private readonly _len: number;

  /**
   * Private constructor - use factory methods instead.
   */
  private constructor(dtype: DType<T>, data: StorageType<T>, offset = 0, length?: number) {
    this.dtype = dtype;
    this._data = data;
    this._offset = offset;
    this._len = length ?? data.length - offset;
    this.length = this._len;
  }

  // ─────────────────────────────────────────────────────────────
  // Factory Methods
  // ─────────────────────────────────────────────────────────────

  /**
   * Creates a float64 Series from numbers.
   */
  static float64(data: number[] | Float64Array): Series<'float64'> {
    const storage = data instanceof Float64Array ? data : new Float64Array(data);
    return new Series<'float64'>({ kind: 'float64', nullable: false }, storage);
  }

  /**
   * Creates an int32 Series from numbers.
   */
  static int32(data: number[] | Int32Array): Series<'int32'> {
    const storage = data instanceof Int32Array ? data : new Int32Array(data);
    return new Series<'int32'>({ kind: 'int32', nullable: false }, storage);
  }

  /**
   * Creates a string Series.
   */
  static string(data: string[]): Series<'string'> {
    return new Series<'string'>({ kind: 'string', nullable: false }, [
      ...data,
    ] as StorageType<'string'>);
  }

  /**
   * Creates a boolean Series.
   */
  static bool(data: boolean[]): Series<'bool'> {
    const storage = createStorageFrom('bool', data);
    return new Series<'bool'>({ kind: 'bool', nullable: false }, storage);
  }

  /**
   * Creates a Series from raw storage (internal use).
   */
  static _fromStorage<T extends DTypeKind>(dtype: DType<T>, storage: StorageType<T>): Series<T> {
    return new Series<T>(dtype, storage);
  }

  // ─────────────────────────────────────────────────────────────
  // Element Access
  // ─────────────────────────────────────────────────────────────

  /**
   * Gets element at index.
   * Returns undefined for out-of-bounds access.
   */
  at(index: number): InferDType<DType<T>> | undefined {
    if (index < 0 || index >= this._len) {
      return undefined;
    }
    const value = this._data[this._offset + index];

    // Convert bool storage (0/1) to boolean
    if (this.dtype.kind === 'bool') {
      return (value === 1) as InferDType<DType<T>>;
    }

    return value as InferDType<DType<T>>;
  }

  // ─────────────────────────────────────────────────────────────
  // Slicing Operations (zero-copy views)
  // ─────────────────────────────────────────────────────────────

  /**
   * Returns first n elements (default: 5).
   * Creates a zero-copy view for TypedArrays.
   */
  head(n = 5): Series<T> {
    const len = Math.min(n, this._len);
    return new Series<T>(this.dtype, this._data, this._offset, len);
  }

  /**
   * Returns last n elements (default: 5).
   * Creates a zero-copy view for TypedArrays.
   */
  tail(n = 5): Series<T> {
    const len = Math.min(n, this._len);
    const offset = this._offset + this._len - len;
    return new Series<T>(this.dtype, this._data, offset, len);
  }

  /**
   * Slices elements from start to end.
   * Creates a zero-copy view for TypedArrays.
   */
  slice(start: number, end?: number): Series<T> {
    const resolvedEnd = end ?? this._len;
    const newOffset = this._offset + Math.max(0, start);
    const newLen = Math.max(0, resolvedEnd - Math.max(0, start));

    return new Series<T>(
      this.dtype,
      this._data,
      newOffset,
      Math.min(newLen, this._len - Math.max(0, start)),
    );
  }

  // ─────────────────────────────────────────────────────────────
  // Iteration
  // ─────────────────────────────────────────────────────────────

  *[Symbol.iterator](): Iterator<InferDType<DType<T>>> {
    for (let i = 0; i < this._len; i++) {
      yield this.at(i)!;
    }
  }

  *values(): IterableIterator<InferDType<DType<T>>> {
    yield* this;
  }

  // ─────────────────────────────────────────────────────────────
  // Statistical Operations (numeric only)
  // ─────────────────────────────────────────────────────────────

  /**
   * Sum of all values. Returns 0 for empty series.
   * Only valid for numeric types (float64, int32).
   */
  sum(): number {
    if (this.dtype.kind === 'string' || this.dtype.kind === 'bool') {
      throw new TypeMismatchError('sum', this.dtype.kind, ['float64', 'int32']);
    }

    let total = 0;
    const data = this._data as Float64Array | Int32Array;
    for (let i = this._offset; i < this._offset + this._len; i++) {
      total += data[i]!;
    }
    return total;
  }

  /**
   * Mean (average) of values. Returns NaN for empty series.
   * Only valid for numeric types.
   */
  mean(): number {
    if (this._len === 0) return Number.NaN;
    return this.sum() / this._len;
  }

  /**
   * Minimum value. Returns Infinity for empty series.
   * Only valid for numeric types.
   */
  min(): number {
    if (this.dtype.kind === 'string' || this.dtype.kind === 'bool') {
      throw new TypeMismatchError('min', this.dtype.kind, ['float64', 'int32']);
    }

    let minVal = Number.POSITIVE_INFINITY;
    const data = this._data as Float64Array | Int32Array;
    for (let i = this._offset; i < this._offset + this._len; i++) {
      if (data[i]! < minVal) minVal = data[i]!;
    }
    return minVal;
  }

  /**
   * Maximum value. Returns -Infinity for empty series.
   * Only valid for numeric types.
   */
  max(): number {
    if (this.dtype.kind === 'string' || this.dtype.kind === 'bool') {
      throw new TypeMismatchError('max', this.dtype.kind, ['float64', 'int32']);
    }

    let maxVal = Number.NEGATIVE_INFINITY;
    const data = this._data as Float64Array | Int32Array;
    for (let i = this._offset; i < this._offset + this._len; i++) {
      if (data[i]! > maxVal) maxVal = data[i]!;
    }
    return maxVal;
  }

  /**
   * Standard deviation of values.
   */
  std(): number {
    if (this._len === 0) return Number.NaN;
    const avg = this.mean();
    let sumSq = 0;
    for (const val of this) {
      const diff = (val as number) - avg;
      sumSq += diff * diff;
    }
    return Math.sqrt(sumSq / this._len);
  }

  /**
   * Variance of values.
   */
  var(): number {
    const s = this.std();
    return s * s;
  }

  // ─────────────────────────────────────────────────────────────
  // Transformation Operations
  // ─────────────────────────────────────────────────────────────

  /**
   * Filter values by predicate function.
   * Returns a new Series with only matching values.
   */
  filter(fn: (value: InferDType<DType<T>>, index: number) => boolean): Series<T> {
    const results: InferDType<DType<T>>[] = [];
    let idx = 0;
    for (const val of this) {
      if (fn(val, idx++)) {
        results.push(val);
      }
    }

    return this._createFromValues(results);
  }

  /**
   * Apply a function to each value.
   * Returns a new Series with transformed values.
   */
  map<R extends DTypeKind = T>(
    fn: (value: InferDType<DType<T>>, index: number) => InferDType<DType<R>>,
  ): Series<R> {
    const results: InferDType<DType<R>>[] = [];
    let idx = 0;
    for (const val of this) {
      results.push(fn(val, idx++));
    }

    // Detect result type from first value
    const firstVal = results[0];
    if (typeof firstVal === 'number') {
      if (Number.isInteger(firstVal) && this.dtype.kind === 'int32') {
        return Series.int32(results as number[]) as unknown as Series<R>;
      }
      return Series.float64(results as number[]) as unknown as Series<R>;
    }
    if (typeof firstVal === 'boolean') {
      return Series.bool(results as boolean[]) as unknown as Series<R>;
    }
    return Series.string(results as string[]) as unknown as Series<R>;
  }

  /**
   * Sort values in ascending or descending order.
   * Returns a new Series.
   */
  sort(ascending = true): Series<T> {
    const values = [...this];

    values.sort((a, b) => {
      if (typeof a === 'number' && typeof b === 'number') {
        return ascending ? a - b : b - a;
      }
      const strA = String(a);
      const strB = String(b);
      return ascending ? strA.localeCompare(strB) : strB.localeCompare(strA);
    });

    return this._createFromValues(values);
  }

  /**
   * Get unique values.
   * Returns a new Series with duplicates removed.
   */
  unique(): Series<T> {
    const seen = new Set<InferDType<DType<T>>>();
    const results: InferDType<DType<T>>[] = [];

    for (const val of this) {
      if (!seen.has(val)) {
        seen.add(val);
        results.push(val);
      }
    }

    return this._createFromValues(results);
  }

  /**
   * Count occurrences of each value.
   * Returns a Map of value -> count.
   */
  valueCounts(): Map<InferDType<DType<T>>, number> {
    const counts = new Map<InferDType<DType<T>>, number>();

    for (const val of this) {
      counts.set(val, (counts.get(val) ?? 0) + 1);
    }

    return counts;
  }

  // ─────────────────────────────────────────────────────────────
  // Type Conversion & Data Cleaning
  // ─────────────────────────────────────────────────────────────

  /**
   * Convert Series to a different dtype.
   * @param kind Target dtype: 'float64', 'int32', 'string', 'bool'
   */
  astype<K extends DTypeKind>(kind: K): Series<K> {
    const results: InferDType<DType<K>>[] = [];

    for (const val of this) {
      let converted: unknown;

      switch (kind) {
        case 'float64':
          if (typeof val === 'number') {
            converted = val;
          } else if (typeof val === 'boolean') {
            converted = val ? 1.0 : 0.0;
          } else {
            converted = parseFloat(String(val)) || 0;
          }
          break;
        case 'int32':
          if (typeof val === 'number') {
            converted = Math.trunc(val);
          } else if (typeof val === 'boolean') {
            converted = val ? 1 : 0;
          } else {
            converted = parseInt(String(val), 10) || 0;
          }
          break;
        case 'string':
          converted = String(val ?? '');
          break;
        case 'bool':
          if (typeof val === 'boolean') {
            converted = val;
          } else if (typeof val === 'number') {
            converted = val !== 0;
          } else {
            const s = String(val).toLowerCase();
            converted = s === 'true' || s === '1';
          }
          break;
        default:
          converted = val;
      }

      results.push(converted as InferDType<DType<K>>);
    }

    switch (kind) {
      case 'float64':
        return Series.float64(results as number[]) as unknown as Series<K>;
      case 'int32':
        return Series.int32(results as number[]) as unknown as Series<K>;
      case 'bool':
        return Series.bool(results as boolean[]) as unknown as Series<K>;
      default:
        return Series.string(results as string[]) as unknown as Series<K>;
    }
  }

  /**
   * Replace values in the Series.
   * @param oldValue Value to replace
   * @param newValue Replacement value
   */
  replace(
    oldValue: InferDType<DType<T>>,
    newValue: InferDType<DType<T>>,
  ): Series<T> {
    const results: InferDType<DType<T>>[] = [];

    for (const val of this) {
      if (val === oldValue || (Number.isNaN(val as number) && Number.isNaN(oldValue as number))) {
        results.push(newValue);
      } else {
        results.push(val);
      }
    }

    return this._createFromValues(results);
  }

  /**
   * Clip values to a range.
   * Only valid for numeric types.
   * @param min Minimum value (values below are set to min)
   * @param max Maximum value (values above are set to max)
   */
  clip(min?: number, max?: number): Series<T> {
    if (this.dtype.kind === 'string' || this.dtype.kind === 'bool') {
      throw new TypeMismatchError('clip', this.dtype.kind, ['float64', 'int32']);
    }

    const results: InferDType<DType<T>>[] = [];

    for (const val of this) {
      let clipped = val as number;
      if (min !== undefined && clipped < min) clipped = min;
      if (max !== undefined && clipped > max) clipped = max;
      results.push(clipped as InferDType<DType<T>>);
    }

    return this._createFromValues(results);
  }

  /**
   * Forward fill missing values.
   * Replaces NaN/null with the previous valid value.
   */
  ffill(): Series<T> {
    const results: InferDType<DType<T>>[] = [];
    let lastValid: InferDType<DType<T>> | null = null;

    for (const val of this) {
      const isMissing =
        val === null ||
        val === undefined ||
        (typeof val === 'number' && Number.isNaN(val));

      if (isMissing && lastValid !== null) {
        results.push(lastValid);
      } else {
        results.push(val);
        if (!isMissing) lastValid = val;
      }
    }

    return this._createFromValues(results);
  }

  /**
   * Backward fill missing values.
   * Replaces NaN/null with the next valid value.
   */
  bfill(): Series<T> {
    const values = [...this];
    const results: InferDType<DType<T>>[] = new Array(values.length);
    let nextValid: InferDType<DType<T>> | null = null;

    for (let i = values.length - 1; i >= 0; i--) {
      const val = values[i]!;
      const isMissing =
        val === null ||
        val === undefined ||
        (typeof val === 'number' && Number.isNaN(val));

      if (isMissing && nextValid !== null) {
        results[i] = nextValid;
      } else {
        results[i] = val;
        if (!isMissing) nextValid = val;
      }
    }

    return this._createFromValues(results);
  }

  /**
   * String accessor for string Series.
   * Provides string manipulation methods.
   * @throws TypeMismatchError if Series is not string type
   */
  get str(): StringAccessor {
    if (this.dtype.kind !== 'string') {
      throw new TypeMismatchError('str', this.dtype.kind, ['string']);
    }
    return new StringAccessor(this as unknown as Series<'string'>);
  }

  /**
   * Summary statistics for numeric Series.
   */
  describe(): { count: number; mean: number; std: number; min: number; max: number } {
    if (this.dtype.kind === 'string' || this.dtype.kind === 'bool') {
      throw new TypeMismatchError('describe', this.dtype.kind, ['float64', 'int32']);
    }

    return {
      count: this._len,
      mean: this.mean(),
      std: this.std(),
      min: this.min(),
      max: this.max(),
    };
  }

  /**
   * Convert Series to plain array.
   */
  toArray(): InferDType<DType<T>>[] {
    return [...this];
  }

  // ─────────────────────────────────────────────────────────────
  // Missing Value Operations
  // ─────────────────────────────────────────────────────────────

  /**
   * Detect missing values (null, undefined, NaN).
   * Returns a boolean Series.
   */
  isna(): Series<'bool'> {
    const results: boolean[] = [];
    for (const val of this) {
      const isMissing =
        val === null ||
        val === undefined ||
        (typeof val === 'number' && Number.isNaN(val));
      results.push(isMissing);
    }
    return Series.bool(results);
  }

  /**
   * Fill missing values with specified value.
   * Returns a new Series.
   */
  fillna(value: InferDType<DType<T>>): Series<T> {
    const results: InferDType<DType<T>>[] = [];
    for (const val of this) {
      const isMissing =
        val === null ||
        val === undefined ||
        (typeof val === 'number' && Number.isNaN(val));
      results.push(isMissing ? value : val);
    }
    return this._createFromValues(results);
  }

  /**
   * Create a deep copy of the Series.
   */
  copy(): Series<T> {
    return this._createFromValues([...this]);
  }

  // ─────────────────────────────────────────────────────────────
  // Display
  // ─────────────────────────────────────────────────────────────

  /**
   * Prints Series to console in formatted output.
   */
  print(): void {
    console.log(this.toString());
  }

  /**
   * Formats Series as string.
   */
  toString(): string {
    const maxShow = 10;
    const lines: string[] = [];

    if (this._len <= maxShow) {
      for (let i = 0; i < this._len; i++) {
        lines.push(`${i}    ${this._formatValue(this.at(i))}`);
      }
    } else {
      // Show first 5 and last 5
      for (let i = 0; i < 5; i++) {
        lines.push(`${i}    ${this._formatValue(this.at(i))}`);
      }
      lines.push('...');
      for (let i = this._len - 5; i < this._len; i++) {
        lines.push(`${i}    ${this._formatValue(this.at(i))}`);
      }
    }

    lines.push(`dtype: ${this.dtype.kind}, length: ${this._len}`);
    return lines.join('\n');
  }

  private _formatValue(value: unknown): string {
    if (value === null || value === undefined) {
      return 'null';
    }
    if (typeof value === 'number') {
      return Number.isInteger(value) ? String(value) : value.toFixed(4);
    }
    return String(value);
  }

  private _createFromValues(values: InferDType<DType<T>>[]): Series<T> {
    switch (this.dtype.kind) {
      case 'float64':
        return Series.float64(values as number[]) as unknown as Series<T>;
      case 'int32':
        return Series.int32(values as number[]) as unknown as Series<T>;
      case 'bool':
        return Series.bool(values as boolean[]) as unknown as Series<T>;
      default:
        return Series.string(values as string[]) as unknown as Series<T>;
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Internal
  // ─────────────────────────────────────────────────────────────

  /**
   * Returns underlying storage (for internal/advanced use).
   */
  _storage(): StorageType<T> {
    // Return a view if using offset, otherwise return the data directly
    if (this._offset === 0 && this._len === this._data.length) {
      return this._data;
    }

    // For TypedArrays, create a subarray view
    if (
      this._data instanceof Float64Array ||
      this._data instanceof Int32Array ||
      this._data instanceof Uint8Array
    ) {
      return this._data.subarray(this._offset, this._offset + this._len) as StorageType<T>;
    }

    // For string arrays, slice (creates copy)
    return (this._data as string[]).slice(this._offset, this._offset + this._len) as StorageType<T>;
  }
}
