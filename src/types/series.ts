import type { Column } from '../core/column';
import { getColumnValue } from '../core/column';
import type { StringDictionary } from '../memory/dictionary';
import { DType } from './dtypes';
import { SeriesStringMethods } from './series-string';

/**
 * Series represents a single column of typed data
 * Provides high-level operations for searching, sorting, and manipulation
 */
export class Series<T = unknown> {
  /** Internal column storage */
  private readonly column: Column;
  /** String dictionary (shared with DataFrame) */
  private readonly dictionary?: StringDictionary;
  /** Column name (optional) */
  readonly name?: string;

  constructor(column: Column, dictionary?: StringDictionary, name?: string) {
    this.column = column;
    this.dictionary = dictionary;
    this.name = name;
  }

  /**
   * Get the data type of the Series
   */
  get dtype(): DType {
    return this.column.dtype;
  }

  /**
   * Get the number of elements in the Series
   */
  get length(): number {
    return this.column.length;
  }

  /**
   * Convert Series to array
   * @returns Array of values
   */
  toArray(): T[] {
    const result: T[] = [];
    for (let i = 0; i < this.column.length; i++) {
      const value = getColumnValue(this.column, i);
      // For string columns, convert dictionary ID to string
      if (this.column.dtype === DType.String && this.dictionary && typeof value === 'number') {
        const str = this.dictionary.idToString.get(value);
        result.push((str ?? null) as T);
      } else {
        result.push(value as T);
      }
    }
    return result;
  }

  /**
   * Get value at specific index
   * @param index - Row index
   * @returns Value at index
   */
  get(index: number): T | null {
    if (index < 0 || index >= this.column.length) {
      return null;
    }
    const value = getColumnValue(this.column, index);
    // For string columns, convert dictionary ID to string
    if (this.column.dtype === DType.String && this.dictionary && typeof value === 'number') {
      return (this.dictionary.idToString.get(value) ?? null) as T;
    }
    return value as T;
  }

  // ============================================================================
  // Numeric Aggregations
  // ============================================================================

  /**
   * Sum of all values (numeric columns only)
   * @returns Sum or 0 for non-numeric columns
   */
  sum(): number {
    const { dtype, length, view } = this.column;
    if (dtype !== DType.Float64 && dtype !== DType.Int32) return 0;

    let total = 0;
    if (dtype === DType.Float64) {
      // Raw DataView access for Float64
      for (let i = 0; i < length; i++) {
        total += view.getFloat64(i * 8, true);
      }
    } else {
      // Raw DataView access for Int32
      for (let i = 0; i < length; i++) {
        total += view.getInt32(i * 4, true);
      }
    }
    return total;
  }

  /**
   * Mean (average) of all values (numeric columns only)
   * @returns Mean or 0 for non-numeric columns or empty series
   */
  mean(): number {
    if (this.column.length === 0) return 0;
    return this.sum() / this.column.length;
  }

  /**
   * Minimum value (numeric columns only)
   * @returns Minimum value or Infinity for non-numeric/empty columns
   */
  min(): number {
    const { dtype, length, view } = this.column;
    if (dtype !== DType.Float64 && dtype !== DType.Int32) {
      return Number.POSITIVE_INFINITY;
    }
    if (length === 0) return Number.POSITIVE_INFINITY;

    let minVal = Number.POSITIVE_INFINITY;
    if (dtype === DType.Float64) {
      // Raw DataView access for Float64
      for (let i = 0; i < length; i++) {
        const val = view.getFloat64(i * 8, true);
        if (val < minVal) minVal = val;
      }
    } else {
      // Raw DataView access for Int32
      for (let i = 0; i < length; i++) {
        const val = view.getInt32(i * 4, true);
        if (val < minVal) minVal = val;
      }
    }
    return minVal;
  }

  /**
   * Maximum value (numeric columns only)
   * @returns Maximum value or -Infinity for non-numeric/empty columns
   */
  max(): number {
    const { dtype, length, view } = this.column;
    if (dtype !== DType.Float64 && dtype !== DType.Int32) {
      return Number.NEGATIVE_INFINITY;
    }
    if (length === 0) return Number.NEGATIVE_INFINITY;

    let maxVal = Number.NEGATIVE_INFINITY;
    if (dtype === DType.Float64) {
      // Raw DataView access for Float64
      for (let i = 0; i < length; i++) {
        const val = view.getFloat64(i * 8, true);
        if (val > maxVal) maxVal = val;
      }
    } else {
      // Raw DataView access for Int32
      for (let i = 0; i < length; i++) {
        const val = view.getInt32(i * 4, true);
        if (val > maxVal) maxVal = val;
      }
    }
    return maxVal;
  }

  /**
   * Median value (numeric columns only)
   * @returns Median value or 0 for non-numeric/empty columns
   */
  median(): number {
    const { dtype, length, view } = this.column;
    if (dtype !== DType.Float64 && dtype !== DType.Int32) return 0;
    if (length === 0) return 0;

    // Collect values
    const values: number[] = [];
    if (dtype === DType.Float64) {
      for (let i = 0; i < length; i++) {
        values.push(view.getFloat64(i * 8, true));
      }
    } else {
      for (let i = 0; i < length; i++) {
        values.push(view.getInt32(i * 4, true));
      }
    }

    // Sort and find median
    values.sort((a, b) => a - b);
    const mid = Math.floor(values.length / 2);
    if (values.length % 2 === 0) {
      return (values[mid - 1]! + values[mid]!) / 2;
    }
    return values[mid]!;
  }

  /**
   * Mode (most frequent value) for numeric columns
   * @returns Most frequent value or 0 for non-numeric/empty columns
   */
  mode(): number {
    const { dtype, length, view } = this.column;
    if (dtype !== DType.Float64 && dtype !== DType.Int32) return 0;
    if (length === 0) return 0;

    // Count frequency
    const freq = new Map<number, number>();
    if (dtype === DType.Float64) {
      for (let i = 0; i < length; i++) {
        const val = view.getFloat64(i * 8, true);
        freq.set(val, (freq.get(val) || 0) + 1);
      }
    } else {
      for (let i = 0; i < length; i++) {
        const val = view.getInt32(i * 4, true);
        freq.set(val, (freq.get(val) || 0) + 1);
      }
    }

    // Find mode
    let modeVal = 0;
    let maxFreq = 0;
    for (const [val, count] of freq) {
      if (count > maxFreq) {
        maxFreq = count;
        modeVal = val;
      }
    }
    return modeVal;
  }

  /**
   * Cumulative sum (numeric columns only)
   * @returns Array of cumulative sums
   */
  cumsum(): number[] {
    const { dtype, length, view } = this.column;
    if (dtype !== DType.Float64 && dtype !== DType.Int32) {
      return new Array(length).fill(0);
    }

    const result: number[] = new Array(length);
    let cumulative = 0;

    if (dtype === DType.Float64) {
      for (let i = 0; i < length; i++) {
        cumulative += view.getFloat64(i * 8, true);
        result[i] = cumulative;
      }
    } else {
      for (let i = 0; i < length; i++) {
        cumulative += view.getInt32(i * 4, true);
        result[i] = cumulative;
      }
    }

    return result;
  }

  /**
   * Cumulative maximum (numeric columns only)
   * @returns Array of cumulative maximums
   */
  cummax(): number[] {
    const { dtype, length, view } = this.column;
    if (dtype !== DType.Float64 && dtype !== DType.Int32) {
      return new Array(length).fill(0);
    }

    const result: number[] = new Array(length);
    let cumMax = Number.NEGATIVE_INFINITY;

    if (dtype === DType.Float64) {
      for (let i = 0; i < length; i++) {
        const val = view.getFloat64(i * 8, true);
        cumMax = Math.max(cumMax, val);
        result[i] = cumMax;
      }
    } else {
      for (let i = 0; i < length; i++) {
        const val = view.getInt32(i * 4, true);
        cumMax = Math.max(cumMax, val);
        result[i] = cumMax;
      }
    }

    return result;
  }

  /**
   * Cumulative minimum (numeric columns only)
   * @returns Array of cumulative minimums
   */
  cummin(): number[] {
    const { dtype, length, view } = this.column;
    if (dtype !== DType.Float64 && dtype !== DType.Int32) {
      return new Array(length).fill(0);
    }

    const result: number[] = new Array(length);
    let cumMin = Number.POSITIVE_INFINITY;

    if (dtype === DType.Float64) {
      for (let i = 0; i < length; i++) {
        const val = view.getFloat64(i * 8, true);
        cumMin = Math.min(cumMin, val);
        result[i] = cumMin;
      }
    } else {
      for (let i = 0; i < length; i++) {
        const val = view.getInt32(i * 4, true);
        cumMin = Math.min(cumMin, val);
        result[i] = cumMin;
      }
    }

    return result;
  }

  /**
   * Get unique values in the Series
   * @returns Array of unique values
   */
  unique(): T[] {
    const seen = new Set<number | bigint | string | boolean>();
    const result: T[] = [];

    for (let i = 0; i < this.column.length; i++) {
      const value = getColumnValue(this.column, i);
      if (value === null || value === undefined) continue;

      // For string columns, we work with dictionary IDs
      if (this.column.dtype === DType.String && typeof value === 'number') {
        if (!seen.has(value)) {
          seen.add(value);
          const str = this.dictionary?.idToString.get(value);
          if (str !== undefined) {
            result.push(str as T);
          }
        }
      } else if (
        typeof value === 'number' ||
        typeof value === 'bigint' ||
        typeof value === 'string' ||
        typeof value === 'boolean'
      ) {
        if (!seen.has(value)) {
          seen.add(value);
          result.push(value as T);
        }
      }
    }

    return result;
  }

  /**
   * Count non-null values
   * For simplicity, returns length (null bitmap check to be added)
   * @returns Count of non-null values
   */
  count(): number {
    return this.column.length;
  }

  // ============================================================================
  // String Operations
  // ============================================================================

  /**
   * String operations namespace (for String dtype columns)
   */
  get str(): SeriesStringMethods {
    return new SeriesStringMethods(this.column, this.dictionary);
  }
}

export { SeriesStringMethods } from './series-string';
export { createSeries } from './series-factory';
