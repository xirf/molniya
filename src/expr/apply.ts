/**
 * Expression application utilities.
 *
 * Functions for applying compiled expressions to chunks.
 */

import type { Chunk } from "../buffer/chunk.ts";
import type { CompiledPredicate, CompiledValue, VectorizedPredicate } from "./compile-types.ts";

/**
 * Apply a predicate and generate a selection vector.
 * This is the primary way predicates are used in filtering.
 *
 * If a vectorized predicate is provided, it is used for batch evaluation
 * which is significantly faster (10-50x) for simple predicates.
 *
 * @param predicate - Compiled predicate function (scalar per-row)
 * @param chunk - Input chunk to filter
 * @param selectionOut - Output array to write selected row indices
 * @param vectorized - Optional vectorized fast path
 * @returns Number of rows that matched the predicate
 */
export function applyPredicate(
	predicate: CompiledPredicate,
	chunk: Chunk,
	selectionOut: Uint32Array,
	vectorized?: VectorizedPredicate,
): number {
	// Use vectorized path if available
	if (vectorized) {
		return vectorized(chunk, selectionOut);
	}

	// Fallback: scalar per-row evaluation
	const rowCount = chunk.rowCount;
	let selected = 0;

	for (let i = 0; i < rowCount; i++) {
		if (predicate(chunk, i)) {
			selectionOut[selected++] = i;
		}
	}

	return selected;
}

/**
 * Apply a value expression and evaluate it for all rows.
 * Writes results to the output array.
 *
 * @param value - Compiled value function
 * @param chunk - Input chunk
 * @param outputF64 - Output array for results (uses NaN for null)
 */
export function applyValue(
	value: CompiledValue,
	chunk: Chunk,
	outputF64: Float64Array,
): void {
	const rowCount = chunk.rowCount;

	for (let i = 0; i < rowCount; i++) {
		const v = value(chunk, i);
		if (v === null) {
			outputF64[i] = NaN; // Use NaN for null
		} else if (typeof v === "bigint") {
			outputF64[i] = Number(v);
		} else if (typeof v === "number") {
			outputF64[i] = v;
		} else if (typeof v === "boolean") {
			outputF64[i] = v ? 1 : 0;
		} else {
			outputF64[i] = NaN;
		}
	}
}

/**
 * Apply a predicate to filter rows and return matching count.
 * Does not write to selection buffer.
 */
export function countMatching(
	predicate: CompiledPredicate,
	chunk: Chunk,
): number {
	const rowCount = chunk.rowCount;
	let count = 0;

	for (let i = 0; i < rowCount; i++) {
		if (predicate(chunk, i)) {
			count++;
		}
	}

	return count;
}
