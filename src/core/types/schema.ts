import type { DType, DTypeKind } from './dtype';
import type { InferDType } from './inference';

/**
 * Schema definition type.
 * Maps column names to their DType descriptors.
 */
export type Schema = Record<string, DType<DTypeKind>>;

/**
 * Infers the row type from a Schema.
 * Each key maps to its corresponding TypeScript type.
 *
 * @example
 * const schema = { age: m.int32(), name: m.string() } as const;
 * type Row = InferSchema<typeof schema>; // { age: number, name: string }
 */
export type InferSchema<S extends Schema> = {
  [K in keyof S]: InferDType<S[K]>;
};
