import type { DType, DTypeKind } from './dtype';

/**
 * Type builder (Elysia-style).
 * Creates DType descriptors with compile-time type inference.
 *
 * @example
 * const schema = {
 *   age: m.int32(),
 *   name: m.string(),
 *   score: m.nullable(m.float64()),
 * } as const;
 */
export const m = {
  /**
   * 64-bit floating point number.
   * Backed by Float64Array for optimal performance.
   */
  float64: (): DType<'float64'> => ({ kind: 'float64', nullable: false }),

  /**
   * 32-bit signed integer.
   * Backed by Int32Array for optimal performance.
   */
  int32: (): DType<'int32'> => ({ kind: 'int32', nullable: false }),

  /**
   * String type.
   * Backed by Array<string>.
   */
  string: (): DType<'string'> => ({ kind: 'string', nullable: false }),

  /**
   * Boolean type.
   * Backed by Uint8Array (0 = false, 1 = true).
   */
  bool: (): DType<'bool'> => ({ kind: 'bool', nullable: false }),

  /**
   * Makes a DType nullable.
   * - Numerics use NaN / sentinel values
   * - Strings use null
   * - Bools use 255 as sentinel
   */
  nullable: <T extends DType<DTypeKind>>(dtype: T): T & { nullable: true } =>
    ({ ...dtype, nullable: true }) as T & { nullable: true },
} as const;
