import type { Column } from '../core/column';
import { getColumnValue } from '../core/column';
import type { StringDictionary } from '../memory/dictionary';
import { DType } from './dtypes';

/**
 * String operations for Series
 * Only available for String dtype columns
 */
export class SeriesStringMethods {
  constructor(
    private readonly column: Column,
    private readonly dictionary?: StringDictionary,
  ) {
    if (column.dtype !== DType.String) {
      throw new Error('String operations are only available for String dtype columns');
    }
  }

  /**
   * Convert all strings to lowercase
   * @returns Array of lowercase strings
   */
  toLowerCase(): string[] {
    const result: string[] = [];
    for (let i = 0; i < this.column.length; i++) {
      const value = getColumnValue(this.column, i);
      if (typeof value === 'number' && this.dictionary) {
        const str = this.dictionary.idToString.get(value);
        result.push(str ? str.toLowerCase() : '');
      } else {
        result.push('');
      }
    }
    return result;
  }

  /**
   * Convert all strings to uppercase
   * @returns Array of uppercase strings
   */
  toUpperCase(): string[] {
    const result: string[] = [];
    for (let i = 0; i < this.column.length; i++) {
      const value = getColumnValue(this.column, i);
      if (typeof value === 'number' && this.dictionary) {
        const str = this.dictionary.idToString.get(value);
        result.push(str ? str.toUpperCase() : '');
      } else {
        result.push('');
      }
    }
    return result;
  }

  /**
   * Check if strings contain a substring
   * @param substring - Substring to search for
   * @returns Array of booleans
   */
  contains(substring: string): boolean[] {
    const result: boolean[] = [];
    for (let i = 0; i < this.column.length; i++) {
      const value = getColumnValue(this.column, i);
      if (typeof value === 'number' && this.dictionary) {
        const str = this.dictionary.idToString.get(value);
        result.push(str ? str.includes(substring) : false);
      } else {
        result.push(false);
      }
    }
    return result;
  }

  /**
   * Check if strings start with a prefix
   * @param prefix - Prefix to check
   * @returns Array of booleans
   */
  startsWith(prefix: string): boolean[] {
    const result: boolean[] = [];
    for (let i = 0; i < this.column.length; i++) {
      const value = getColumnValue(this.column, i);
      if (typeof value === 'number' && this.dictionary) {
        const str = this.dictionary.idToString.get(value);
        result.push(str ? str.startsWith(prefix) : false);
      } else {
        result.push(false);
      }
    }
    return result;
  }

  /**
   * Check if strings end with a suffix
   * @param suffix - Suffix to check
   * @returns Array of booleans
   */
  endsWith(suffix: string): boolean[] {
    const result: boolean[] = [];
    for (let i = 0; i < this.column.length; i++) {
      const value = getColumnValue(this.column, i);
      if (typeof value === 'number' && this.dictionary) {
        const str = this.dictionary.idToString.get(value);
        result.push(str ? str.endsWith(suffix) : false);
      } else {
        result.push(false);
      }
    }
    return result;
  }

  /**
   * Get length of each string
   * @returns Array of string lengths
   */
  length(): number[] {
    const result: number[] = [];
    for (let i = 0; i < this.column.length; i++) {
      const value = getColumnValue(this.column, i);
      if (typeof value === 'number' && this.dictionary) {
        const str = this.dictionary.idToString.get(value);
        result.push(str ? str.length : 0);
      } else {
        result.push(0);
      }
    }
    return result;
  }
}
