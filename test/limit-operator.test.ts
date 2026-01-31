/**
 * Tests for LimitOperator with SelectionBufferPool integration
 *
 * TDD: Verify that LimitOperator properly uses pooled selection buffers
 */
/** biome-ignore-all lint/style/noNonNullAssertion: Not null */

import { describe, expect, it } from "bun:test";
import { Chunk } from "../src/buffer/chunk.ts";
import { ColumnBuffer } from "../src/buffer/column-buffer.ts";
import { selectionPool } from "../src/buffer/selection-pool.ts";
import { LimitOperator } from "../src/ops/limit.ts";
import { DType, DTypeKind } from "../src/types/dtypes.ts";
import { unwrap } from "../src/types/error.ts";
import { createSchema } from "../src/types/schema.ts";

describe("LimitOperator", () => {
	// Helper to create a simple test chunk
	function createTestChunk(values: number[]): Chunk {
		const schema = unwrap(createSchema({ value: DType.float64 }));
		const colBuffer = new ColumnBuffer(DTypeKind.Float64, values.length, false);

		for (let i = 0; i < values.length; i++) {
			colBuffer.set(i, values[i]!);
		}
		colBuffer.setLength(values.length);

		return new Chunk(schema, [colBuffer]);
	}

	describe("with selection pool", () => {
		it("should limit rows without allocating new arrays per chunk", () => {
			// Get initial pool stats
			const statsBefore = selectionPool.getStats();

			const schema = unwrap(createSchema({ value: DType.float64 }));
			const limitOp = new LimitOperator(schema, 5); // Limit to 5 rows

			// Process multiple chunks
			for (let i = 0; i < 10; i++) {
				const chunk = createTestChunk([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
				const result = limitOp.process(chunk);
				expect(result.error).toBe(0);

				// First chunk should be limited to 5 rows
				if (i === 0) {
					expect(result.value!.chunk!.rowCount).toBe(5);
				}
			}

			// Pool should have been used
			const statsAfter = selectionPool.getStats();
			expect(statsAfter.totalBuffers).toBeGreaterThanOrEqual(
				statsBefore.totalBuffers,
			);
		});

		it("should handle offset without allocating new arrays", () => {
			const schema = unwrap(createSchema({ value: DType.float64 }));
			const limitOp = new LimitOperator(schema, 5, 3); // Skip 3, limit 5

			const chunk = createTestChunk([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
			const result = limitOp.process(chunk);

			expect(result.error).toBe(0);
			// Should skip 3 (1,2,3), then take 5 (4,5,6,7,8)
			expect(result.value!.chunk!.rowCount).toBe(5);
		});

		it("should handle empty chunks", () => {
			const schema = unwrap(createSchema({ value: DType.float64 }));
			const limitOp = new LimitOperator(schema, 5);

			const chunk = createTestChunk([]);
			const result = limitOp.process(chunk);

			expect(result.error).toBe(0);
			expect(result.value!.chunk).toBeNull(); // Empty result
		});

		it("should signal done when limit is reached", () => {
			const schema = unwrap(createSchema({ value: DType.float64 }));
			const limitOp = new LimitOperator(schema, 5);

			// First chunk with 10 rows - should limit to 5 and signal done
			const chunk = createTestChunk([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
			const result = limitOp.process(chunk);

			expect(result.error).toBe(0);
			expect(result.value!.done).toBe(true);
			expect(result.value!.chunk!.rowCount).toBe(5);
		});

		it("should pass through chunks that fit within limit", () => {
			const schema = unwrap(createSchema({ value: DType.float64 }));
			const limitOp = new LimitOperator(schema, 10);

			// Chunk with 5 rows - should pass through entirely
			const chunk = createTestChunk([1, 2, 3, 4, 5]);
			const result = limitOp.process(chunk);

			expect(result.error).toBe(0);
			expect(result.value!.chunk).toBe(chunk); // Same chunk
			expect(result.value!.chunk!.rowCount).toBe(5);
			expect(result.value!.done).toBe(false);
		});

		it("should handle partial offset within a chunk", () => {
			const schema = unwrap(createSchema({ value: DType.float64 }));
			const limitOp = new LimitOperator(schema, 10, 5); // Skip 5, limit 10

			// First chunk: skip 5 of 10, leaving 5
			const chunk1 = createTestChunk([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
			const result1 = limitOp.process(chunk1);

			expect(result1.error).toBe(0);
			expect(result1.value!.chunk!.rowCount).toBe(5); // 10 - 5 = 5 remaining

			// Second chunk: need 5 more to reach limit of 10
			const chunk2 = createTestChunk([11, 12, 13, 14, 15, 16, 17]);
			const result2 = limitOp.process(chunk2);

			expect(result2.error).toBe(0);
			expect(result2.value!.chunk!.rowCount).toBe(5); // Only take 5
			expect(result2.value!.done).toBe(true);
		});
	});
});
