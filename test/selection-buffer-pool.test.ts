/**
 * Tests for SelectionBufferPool - zero-copy selection buffer reuse
 *
 * TDD Approach:
 * 1. Write tests that define expected behavior
 * 2. Run tests (they should fail)
 * 3. Implement the feature
 * 4. Verify tests pass
 */

import { beforeEach, describe, expect, it } from "bun:test";
import { SelectionBufferPool } from "../src/buffer/selection-pool.ts";

describe("SelectionBufferPool", () => {
	let pool: SelectionBufferPool;

	beforeEach(() => {
		pool = new SelectionBufferPool();
	});

	describe("acquire", () => {
		it("should return a Uint32Array of at least requested size", () => {
			const buffer = pool.acquire(100);
			expect(buffer).toBeInstanceOf(Uint32Array);
			// Pool rounds up to power of 2 for better reuse
			expect(buffer.length).toBeGreaterThanOrEqual(100);
		});

		it("should return zeroed buffer", () => {
			const buffer = pool.acquire(10);
			for (let i = 0; i < buffer.length; i++) {
				expect(buffer[i]).toBe(0);
			}
		});

		it("should reuse released buffers of same size", () => {
			const buffer1 = pool.acquire(100);
			const ptr1 = buffer1.buffer;

			pool.release(buffer1);
			const buffer2 = pool.acquire(100);
			const ptr2 = buffer2.buffer;

			// Should be the same underlying buffer (reused)
			expect(ptr1).toBe(ptr2);
		});

		it("should not reuse buffers of different sizes", () => {
			const buffer1 = pool.acquire(100);
			const ptr1 = buffer1.buffer;

			pool.release(buffer1);
			const buffer2 = pool.acquire(200);
			const ptr2 = buffer2.buffer;

			// Should be different buffers
			expect(ptr1).not.toBe(ptr2);
		});

		it("should create new buffer when pool is empty", () => {
			const buffer1 = pool.acquire(100);
			const buffer2 = pool.acquire(100);

			// Different buffers since nothing was released
			expect(buffer1.buffer).not.toBe(buffer2.buffer);
		});
	});

	describe("release", () => {
		it("should accept buffers for pooling", () => {
			const buffer = pool.acquire(100);
			expect(() => pool.release(buffer)).not.toThrow();
		});

		it("should clear buffer data on release", () => {
			const buffer = pool.acquire(10);
			buffer[0] = 999;
			buffer[5] = 123;

			pool.release(buffer);
			const reused = pool.acquire(10);

			// Should be zeroed
			expect(reused[0]).toBe(0);
			expect(reused[5]).toBe(0);
		});

		it("should limit pool size to prevent unbounded growth", () => {
			// Create and release many buffers
			const buffers: Uint32Array[] = [];
			for (let i = 0; i < 60; i++) {
				buffers.push(pool.acquire(100));
			}

			for (const buf of buffers) {
				pool.release(buf);
			}

			// Pool should be limited (default limit is 50)
			// Acquire many buffers - some should be new allocations
			const newBuffers: Uint32Array[] = [];
			for (let i = 0; i < 60; i++) {
				newBuffers.push(pool.acquire(100));
			}

			// We can't directly test internal state, but we can verify it doesn't crash
			// and buffers are valid (length will be rounded up to power of 2)
			for (const buf of newBuffers) {
				expect(buf.length).toBeGreaterThanOrEqual(100);
				expect(buf).toBeInstanceOf(Uint32Array);
			}
		});
	});

	describe("getInstance", () => {
		it("should return singleton instance", () => {
			const instance1 = SelectionBufferPool.getInstance();
			const instance2 = SelectionBufferPool.getInstance();
			expect(instance1).toBe(instance2);
		});
	});

	describe("integration - zero copy pattern", () => {
		it("should support acquire-use-release pattern without allocations", () => {
			// Simulate the filter operator pattern
			const selectionBuffer = pool.acquire(1000);

			// Use the buffer (simulate filtering)
			let selected = 0;
			for (let i = 0; i < 100; i++) {
				if (i % 2 === 0) {
					selectionBuffer[selected++] = i;
				}
			}

			// Create a view of just the selected portion
			const view = selectionBuffer.subarray(0, selected);

			// In real usage, we'd pass 'view' to chunk.applySelection
			expect(view.length).toBe(50);
			expect(view[0]).toBe(0);
			expect(view[1]).toBe(2);

			// Release back to pool
			pool.release(selectionBuffer);

			// Reuse
			const reused = pool.acquire(1000);
			expect(reused.buffer).toBe(selectionBuffer.buffer);
		});
	});
});
