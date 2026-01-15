/**
 * DType kind identifiers.
 * These are the string literal types that identify each data type.
 */
export type DTypeKind = 'float64' | 'int32' | 'string' | 'bool';

/**
 * DType descriptor interface.
 * Carries type information at both runtime and compile-time.
 */
export interface DType<T extends DTypeKind = DTypeKind> {
  readonly kind: T;
  readonly nullable: boolean;
}
