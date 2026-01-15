import type { DTypeKind, StorageType } from '../types';
import { SchemaError } from '../../errors';

/**
 * Maps DTypeKind to its TypedArray constructor.
 */
export const STORAGE_CONSTRUCTORS = {
  float64: Float64Array,
  int32: Int32Array,
  string: Array,
  bool: Uint8Array,
} as const;

/**
 * Sentinel values for nullable types.
 */
export const NULL_SENTINELS = {
  float64: Number.NaN,
  int32: -2147483648, // INT32_MIN
  string: null,
  bool: 255,
} as const;

/**
 * Creates storage array for given dtype and length.
 */
export function createStorage<T extends DTypeKind>(kind: T, length: number): StorageType<T> {
  switch (kind) {
    case 'float64':
      return new Float64Array(length) as StorageType<T>;
    case 'int32':
      return new Int32Array(length) as StorageType<T>;
    case 'string':
      return new Array<string>(length) as StorageType<T>;
    case 'bool':
      return new Uint8Array(length) as StorageType<T>;
    default:
      throw new SchemaError(`unknown dtype '${kind}'`, `supported types: float64, int32, string, bool`);
  }
}

/**
 * Creates storage from existing data.
 */
export function createStorageFrom<T extends DTypeKind>(
  kind: T,
  data: ArrayLike<number> | string[] | boolean[],
): StorageType<T> {
  switch (kind) {
    case 'float64':
      return new Float64Array(data as ArrayLike<number>) as StorageType<T>;
    case 'int32':
      return new Int32Array(data as ArrayLike<number>) as StorageType<T>;
    case 'string':
      return Array.from(data as string[]) as StorageType<T>;
    case 'bool': {
      const boolData = data as boolean[];
      const arr = new Uint8Array(boolData.length);
      for (let i = 0; i < boolData.length; i++) {
        arr[i] = boolData[i] ? 1 : 0;
      }
      return arr as StorageType<T>;
    }
    default:
      throw new SchemaError(`unknown dtype '${kind}'`, `supported types: float64, int32, string, bool`);
  }
}
