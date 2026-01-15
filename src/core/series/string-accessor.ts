import type { DType, InferDType } from '../types';
import { Series } from './series';

/**
 * String accessor for string Series.
 * Provides string manipulation methods similar to Pandas str accessor.
 *
 * @example
 * ```ts
 * const names = Series.string(['Alice', 'Bob', 'Carol']);
 * names.str.lower();  // ['alice', 'bob', 'carol']
 * names.str.contains('a');  // [false, false, true]
 * ```
 */
export class StringAccessor {
  constructor(private readonly series: Series<'string'>) {}

  /**
   * Convert all strings to lowercase.
   */
  lower(): Series<'string'> {
    const results: string[] = [];
    for (const val of this.series) {
      results.push(val?.toLowerCase() ?? '');
    }
    return Series.string(results);
  }

  /**
   * Convert all strings to uppercase.
   */
  upper(): Series<'string'> {
    const results: string[] = [];
    for (const val of this.series) {
      results.push(val?.toUpperCase() ?? '');
    }
    return Series.string(results);
  }

  /**
   * Trim whitespace from both ends.
   */
  trim(): Series<'string'> {
    const results: string[] = [];
    for (const val of this.series) {
      results.push(val?.trim() ?? '');
    }
    return Series.string(results);
  }

  /**
   * Trim whitespace from the left.
   */
  trimStart(): Series<'string'> {
    const results: string[] = [];
    for (const val of this.series) {
      results.push(val?.trimStart() ?? '');
    }
    return Series.string(results);
  }

  /**
   * Trim whitespace from the right.
   */
  trimEnd(): Series<'string'> {
    const results: string[] = [];
    for (const val of this.series) {
      results.push(val?.trimEnd() ?? '');
    }
    return Series.string(results);
  }

  /**
   * Check if each string contains the pattern.
   * @param pattern String or RegExp to search for
   */
  contains(pattern: string | RegExp): Series<'bool'> {
    const results: boolean[] = [];
    const regex = typeof pattern === 'string' ? new RegExp(pattern) : pattern;
    
    for (const val of this.series) {
      results.push(val ? regex.test(val) : false);
    }
    return Series.bool(results);
  }

  /**
   * Check if each string starts with the prefix.
   */
  startsWith(prefix: string): Series<'bool'> {
    const results: boolean[] = [];
    for (const val of this.series) {
      results.push(val?.startsWith(prefix) ?? false);
    }
    return Series.bool(results);
  }

  /**
   * Check if each string ends with the suffix.
   */
  endsWith(suffix: string): Series<'bool'> {
    const results: boolean[] = [];
    for (const val of this.series) {
      results.push(val?.endsWith(suffix) ?? false);
    }
    return Series.bool(results);
  }

  /**
   * Replace occurrences of pattern with replacement.
   * @param pattern String or RegExp to replace
   * @param replacement Replacement string
   */
  replace(pattern: string | RegExp, replacement: string): Series<'string'> {
    const results: string[] = [];
    const regex = typeof pattern === 'string' 
      ? new RegExp(pattern, 'g') 
      : new RegExp(pattern.source, pattern.flags.includes('g') ? pattern.flags : pattern.flags + 'g');
    
    for (const val of this.series) {
      results.push(val?.replace(regex, replacement) ?? '');
    }
    return Series.string(results);
  }

  /**
   * Split each string by delimiter.
   * Returns an array of arrays.
   */
  split(delimiter: string | RegExp): string[][] {
    const results: string[][] = [];
    for (const val of this.series) {
      results.push(val?.split(delimiter) ?? []);
    }
    return results;
  }

  /**
   * Get the length of each string.
   */
  len(): Series<'int32'> {
    const results: number[] = [];
    for (const val of this.series) {
      results.push(val?.length ?? 0);
    }
    return Series.int32(results);
  }

  /**
   * Extract substring from each string.
   */
  slice(start: number, end?: number): Series<'string'> {
    const results: string[] = [];
    for (const val of this.series) {
      results.push(val?.slice(start, end) ?? '');
    }
    return Series.string(results);
  }

  /**
   * Pad strings on the left to reach specified width.
   */
  padStart(width: number, fillChar = ' '): Series<'string'> {
    const results: string[] = [];
    for (const val of this.series) {
      results.push(val?.padStart(width, fillChar) ?? ''.padStart(width, fillChar));
    }
    return Series.string(results);
  }

  /**
   * Pad strings on the right to reach specified width.
   */
  padEnd(width: number, fillChar = ' '): Series<'string'> {
    const results: string[] = [];
    for (const val of this.series) {
      results.push(val?.padEnd(width, fillChar) ?? ''.padEnd(width, fillChar));
    }
    return Series.string(results);
  }

  /**
   * Repeat each string n times.
   */
  repeat(n: number): Series<'string'> {
    const results: string[] = [];
    for (const val of this.series) {
      results.push(val?.repeat(n) ?? '');
    }
    return Series.string(results);
  }

  /**
   * Capitalize the first character of each string.
   */
  capitalize(): Series<'string'> {
    const results: string[] = [];
    for (const val of this.series) {
      if (!val) {
        results.push('');
      } else {
        results.push(val.charAt(0).toUpperCase() + val.slice(1).toLowerCase());
      }
    }
    return Series.string(results);
  }

  /**
   * Title case each string (capitalize first letter of each word).
   */
  title(): Series<'string'> {
    const results: string[] = [];
    for (const val of this.series) {
      if (!val) {
        results.push('');
      } else {
        results.push(
          val.replace(/\b\w/g, (char) => char.toUpperCase())
        );
      }
    }
    return Series.string(results);
  }
}
