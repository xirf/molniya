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

/**
 * Forces TypeScript to expand/simplify a type in IDE hovers.
 * This makes complex mapped types show their resolved structure.
 */
export type Prettify<T> = { [K in keyof T]: T[K] };

/**
 * Internal type that performs the key renaming.
 * @internal
 */
type RenameSchemaRaw<S extends Schema, M extends { [P in keyof S]?: string }> = {
  [K in keyof S as K extends keyof M ? (M[K] extends string ? M[K] : K) : K]: S[K];
};

/**
 * Renames schema keys based on a mapping.
 * Keys present in M are replaced with their mapped values.
 * Keys not in M are preserved as-is.
 *
 * @example
 * type Original = { age: DType<'int32'>, name: DType<'string'> };
 * type Mapping = { age: 'years' };
 * type Result = RenameSchema<Original, Mapping>; // { years: DType<'int32'>, name: DType<'string'> }
 */
export type RenameSchema<S extends Schema, M extends { [P in keyof S]?: string }> = Prettify<
  RenameSchemaRaw<S, M>
> extends infer R
  ? R extends Schema
    ? R
    : never
  : never;
