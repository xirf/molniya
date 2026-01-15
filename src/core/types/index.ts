/**
 * Core type system module.
 * Provides Elysia-style type builder and inference utilities.
 */

// Type definitions
export type { DType, DTypeKind } from './dtype';
export type { InferDType, StorageType } from './inference';
export type { Schema, InferSchema } from './schema';

// Type builder
export { m } from './builder';
