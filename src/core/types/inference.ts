import type { DType, DTypeKind } from './dtype';

/**
 * Maps DType to its corresponding TypeScript type.
 * Used for compile-time type inference.
 */
export type InferDType<D extends DType<DTypeKind>> = D extends DType<'float64'>
  ? number
  : D extends DType<'int32'>
    ? number
    : D extends DType<'string'>
      ? string
      : D extends DType<'bool'>
        ? boolean
        : never;

/**
 * Maps DType to its underlying storage type.
 * Used for internal buffer representations.
 */
export type StorageType<T extends DTypeKind> = T extends 'float64'
  ? Float64Array
  : T extends 'int32'
    ? Int32Array
    : T extends 'string'
      ? string[]
      : T extends 'bool'
        ? Uint8Array
        : never;
