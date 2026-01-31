/**
 * Selection Buffer Pool for zero-copy filtering.
 *
 * Reuses Uint32Array buffers to avoid GC pressure during filtering operations.
 * Each operator can acquire a buffer, use it for selection vectors, and release it back.
 */

const DEFAULT_POOL_SIZE = 50;

/**
 * Pool for reusing selection buffer arrays.
 * Keyed by buffer size (capacity) to ensure compatible reuse.
 */
export class SelectionBufferPool {
	private static instance: SelectionBufferPool;
	private pools = new Map<number, Uint32Array[]>();
	private maxPoolSize: number;

	constructor(maxPoolSize: number = DEFAULT_POOL_SIZE) {
		this.maxPoolSize = maxPoolSize;
	}

	static getInstance(): SelectionBufferPool {
		if (!SelectionBufferPool.instance) {
			SelectionBufferPool.instance = new SelectionBufferPool();
		}
		return SelectionBufferPool.instance;
	}

	/**
	 * Acquire a buffer of at least the requested size.
	 * Returns a zeroed buffer ready for use.
	 */
	acquire(size: number): Uint32Array {
		const bucketSize = this.roundUpSize(size);

		const pool = this.pools.get(bucketSize);
		if (pool && pool.length > 0) {
			const buffer = pool.pop() as Uint32Array;
			buffer.fill(0);
			return buffer.subarray(0, size);
		}

		// No buffer available, create new
		return new Uint32Array(bucketSize);
	}

	/**
	 * Release a buffer back to the pool for reuse.
	 * The buffer will be zeroed before reuse.
	 */
	release(buffer: Uint32Array): void {
		const bucketSize = buffer.buffer.byteLength / 4;

		let pool = this.pools.get(bucketSize);
		if (!pool) {
			pool = [];
			this.pools.set(bucketSize, pool);
		}

		if (pool.length < this.maxPoolSize) {
			pool.push(buffer);
		}
	}

	/**
	 * Clear all pooled buffers.
	 */
	clear(): void {
		this.pools.clear();
	}

	/**
	 * Get statistics about the pool.
	 */
	getStats(): { totalBuffers: number; totalBytes: number } {
		let totalBuffers = 0;
		let totalBytes = 0;

		for (const [size, pool] of this.pools) {
			totalBuffers += pool.length;
			totalBytes += pool.length * size * 4; // 4 bytes per uint32
		}

		return { totalBuffers, totalBytes };
	}

	/**
	 * Round up size to nearest power of 2 for better buffer reuse.
	 * This reduces fragmentation and increases hit rate.
	 */
	private roundUpSize(size: number): number {
		if (size <= 64) return 64;
		return 2 ** Math.ceil(Math.log2(size));
	}
}

/**
 * Global singleton instance for convenience.
 */
export const selectionPool = SelectionBufferPool.getInstance();
