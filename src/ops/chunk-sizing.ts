/**
 * Adaptive chunk sizing.
 *
 * Computes an optimal number of rows per chunk based on schema column widths
 * and a target memory budget per chunk. Wide schemas (many columns) use smaller
 * chunks to keep memory bounded; narrow schemas use larger chunks to amortize
 * per-chunk overhead.
 */

import type { Schema } from "../types/schema.ts";

/** Default target bytes per chunk (~1 MB) */
const DEFAULT_TARGET_CHUNK_BYTES = 1 << 20; // 1 MB

/** Minimum rows per chunk to avoid excessive per-chunk overhead */
const MIN_CHUNK_SIZE = 1024;

/** Maximum rows per chunk to keep latency bounded */
const MAX_CHUNK_SIZE = 65536;

/** Additional overhead per nullable column per row (null bitmap byte) */
const NULL_BITMAP_BYTES_PER_ROW = 1;

/** Per-row base overhead for selection vectors (4 bytes Uint32) */
const SELECTION_OVERHEAD_PER_ROW = 4;

export interface ChunkSizingOptions {
	/** Target bytes per chunk (default: 1 MB) */
	targetChunkBytes?: number;
	/** Minimum rows per chunk (default: 1024) */
	minChunkSize?: number;
	/** Maximum rows per chunk (default: 65536) */
	maxChunkSize?: number;
}

/**
 * Compute the optimal chunk size (rows) for a given schema.
 *
 * Formula:
 *   bytesPerRow = schema.rowSize + nullableCols * 1 + selectionOverhead
 *   chunkSize = clamp(floor(targetBytes / bytesPerRow), min, max)
 *   chunkSize = floorPow2(chunkSize) for TypedArray alignment
 *
 * Examples:
 *   - 3-column Float64 schema (24 bytes/row): ~43K rows → clamped to 32768
 *   - 100-column Float64 schema (800 bytes/row): ~1310 rows → 1024
 *   - 10-column mixed schema (52 bytes/row): ~20K rows → 16384
 */
export function computeChunkSize(
	schema: Schema,
	options?: ChunkSizingOptions,
): number {
	const targetBytes = options?.targetChunkBytes ?? DEFAULT_TARGET_CHUNK_BYTES;
	const minSize = options?.minChunkSize ?? MIN_CHUNK_SIZE;
	const maxSize = options?.maxChunkSize ?? MAX_CHUNK_SIZE;

	if (schema.columnCount === 0) {
		return minSize;
	}

	// Compute effective bytes per row including overhead
	let bytesPerRow = schema.rowSize;

	// Add null bitmap overhead for nullable columns
	for (const col of schema.columns) {
		if (col.dtype.nullable) {
			bytesPerRow += NULL_BITMAP_BYTES_PER_ROW;
		}
	}

	// Add selection vector overhead (operators that use selection buffers)
	bytesPerRow += SELECTION_OVERHEAD_PER_ROW;

	// Ensure at least 1 byte per row to avoid division by zero
	if (bytesPerRow < 1) bytesPerRow = 1;

	// Compute raw chunk size
	let chunkSize = Math.floor(targetBytes / bytesPerRow);

	// Clamp to bounds
	chunkSize = Math.max(minSize, Math.min(maxSize, chunkSize));

	// Round down to nearest power of 2 for TypedArray alignment
	chunkSize = floorPow2(chunkSize);

	return chunkSize;
}

/**
 * Round down to the nearest power of 2.
 * e.g., 50000 → 32768, 10000 → 8192, 1500 → 1024
 */
function floorPow2(n: number): number {
	if (n <= 0) return 1;
	// Bit trick: fill all bits below highest set bit, then shift right + 1
	let v = n;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	return (v + 1) >>> 1;
}
