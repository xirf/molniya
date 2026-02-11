/**
 * Compiled expression types.
 *
 * These types are shared between compiler and apply modules.
 */

import type { Chunk } from "../buffer/chunk.ts";

/**
 * Compiled predicate function (scalar per-row evaluation).
 * Returns true if the row at the given index matches the predicate.
 */
export type CompiledPredicate = (chunk: Chunk, rowIndex: number) => boolean;

/**
 * Vectorized predicate function (batch evaluation).
 * Scans the entire chunk in a tight loop, writing matching indices to selectionOut.
 * Returns the number of matching rows.
 *
 * This is much faster than CompiledPredicate because:
 * - No per-row function call overhead
 * - Direct TypedArray access (auto-vectorizable by JIT)
 * - No physicalIndex indirection in the inner loop
 */
export type VectorizedPredicate = (
	chunk: Chunk,
	selectionOut: Uint32Array,
) => number;

/**
 * A compiled predicate that may have an optional vectorized fast path.
 * The scalar predicate is always available as a fallback.
 */
export interface CompiledPredicateWithVec {
	/** Scalar per-row predicate (always available) */
	scalar: CompiledPredicate;
	/** Optional vectorized batch predicate */
	vectorized?: VectorizedPredicate;
}

/**
 * Compiled value function.
 * Returns the computed value for the row at the given index.
 */
export type CompiledValue = (
	chunk: Chunk,
	rowIndex: number,
) => number | bigint | boolean | string | null;
