/**
 * Buffer pool for reusing ColumnBuffers to reduce GC pressure.
 */

import { DTypeKind } from "../types/dtypes.ts";
import type { Chunk } from "./chunk.ts";
import { ColumnBuffer } from "./column-buffer.ts";
import { StringViewColumnBuffer } from "./string-view-column.ts";

export class ColumnBufferPool {
	private static instance: ColumnBufferPool;

	// Map<DTypeKind, Array<AvailableBuffers>>
	// We bucket by capacity? Or just keep "at least capacity"?
	// For simplicity, we assume fixed chunk size mostly.
	// If request capacity > buffer capacity, we discard and create new?
	// Or we just bucket by exact capacity or power of 2?
	// Let's assume standard chunk size (e.g. 8192) for now.
	// Map<Key, ColumnBuffer[]>
	// Key = kind + "_" + capacity + "_" + nullable
	private pools = new Map<string, ColumnBuffer[]>();

	private constructor() { }

	static getInstance(): ColumnBufferPool {
		if (!ColumnBufferPool.instance) {
			ColumnBufferPool.instance = new ColumnBufferPool();
		}
		return ColumnBufferPool.instance;
	}

	acquire(kind: DTypeKind, capacity: number, nullable: boolean): ColumnBuffer {
		const key = this.getKey(kind, capacity, nullable);
		const pool = this.pools.get(key);

		if (pool && pool.length > 0) {
			return pool.pop() as ColumnBuffer;
		}

		if (kind === DTypeKind.StringView) {
			// Fast path zero-copy string buffer wrapper
			return new StringViewColumnBuffer(capacity, nullable) as any;
		}

		return new ColumnBuffer(kind, capacity, nullable);
	}

	release(buffer: ColumnBuffer): void {
		// Reset buffer before storing
		buffer.clear();

		// For StringViewColumnBuffer the base `capacity` is 0 (we zero it to avoid
		// allocating a wasted Uint8Array). Use the actual string capacity instead.
		const effectiveCap = buffer.kind === DTypeKind.StringView
			? (buffer as unknown as StringViewColumnBuffer).stringView.capacity
			: buffer.capacity;
		const key = this.getKey(buffer.kind, effectiveCap, buffer.isNullable);
		let pool = this.pools.get(key);
		if (!pool) {
			pool = [];
			this.pools.set(key, pool);
		}

		// Limit pool size?
		if (pool.length < 50) {
			// Arbitrary limit per type
			pool.push(buffer);
		}
	}

	private getKey(kind: DTypeKind, capacity: number, nullable: boolean): string {
		return `${kind}_${capacity}_${nullable}`;
	}
}

export const bufferPool = ColumnBufferPool.getInstance();

/**
 * Recycle a chunk by returning its column buffers to the pool.
 * Calls chunk.dispose() which also releases any borrowed selection buffers.
 */
export function recycleChunk(chunk: Chunk): void {
	const cols = chunk.dispose();
	for (const col of cols) {
		bufferPool.release(col);
	}
}

/**
 * Pool for reusing entire Chunk objects.
 * Pools by schema signature so chunks with matching column layouts can be reused.
 * This avoids repeated allocation of Chunk + ColumnBuffer arrays for
 * scenarios where many chunks flow through a pipeline.
 */
export class ChunkPool {
	private static instance: ChunkPool;
	private pools = new Map<string, Chunk[]>();
	private readonly maxPerKey: number;

	private constructor(maxPerKey: number = 20) {
		this.maxPerKey = maxPerKey;
	}

	static getInstance(): ChunkPool {
		if (!ChunkPool.instance) {
			ChunkPool.instance = new ChunkPool();
		}
		return ChunkPool.instance;
	}

	/**
	 * Return a chunk to the pool for reuse.
	 * Disposes column buffers back to the ColumnBufferPool.
	 */
	release(chunk: Chunk): void {
		// Recycle column buffers first
		recycleChunk(chunk);

		// Pool the chunk shell by its schema signature
		const key = this.schemaKey(chunk);
		let pool = this.pools.get(key);
		if (!pool) {
			pool = [];
			this.pools.set(key, pool);
		}
		if (pool.length < this.maxPerKey) {
			pool.push(chunk);
		}
	}

	/**
	 * Get pool statistics.
	 */
	getStats(): { totalChunks: number; keys: number } {
		let totalChunks = 0;
		for (const pool of this.pools.values()) {
			totalChunks += pool.length;
		}
		return { totalChunks, keys: this.pools.size };
	}

	/**
	 * Clear all pooled chunks.
	 */
	clear(): void {
		this.pools.clear();
	}

	private schemaKey(chunk: Chunk): string {
		return `${chunk.schema.columnCount}_${chunk.columnCount}`;
	}
}

export const chunkPool = ChunkPool.getInstance();
