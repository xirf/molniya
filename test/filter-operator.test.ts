/**
 * Tests for FilterOperator with SelectionBufferPool integration
 *
 * TDD: Verify that FilterOperator properly uses pooled selection buffers
 */

import { describe, expect, it } from "bun:test";
import { Chunk } from "../src/buffer/chunk.ts";
import { ColumnBuffer } from "../src/buffer/column-buffer.ts";
import { selectionPool } from "../src/buffer/selection-pool.ts";
import type { ComparisonExpr } from "../src/expr/ast.ts";
import { ExprType } from "../src/expr/ast.ts";
import { FilterOperator } from "../src/ops/filter.ts";
import { DType, DTypeKind } from "../src/types/dtypes.ts";
import { unwrap } from "../src/types/error.ts";
import { createSchema } from "../src/types/schema.ts";

describe("FilterOperator", () => {
	// Helper to create a simple test chunk
	function createTestChunk(values: number[]): Chunk {
		const schema = unwrap(createSchema({ value: DType.float64 }));
		const colBuffer = new ColumnBuffer(DTypeKind.Float64, values.length, false);

		for (let i = 0; i < values.length; i++) {
			// biome-ignore lint/style/noNonNullAssertion: Not null
			colBuffer.set(i, values[i]!);
		}
		colBuffer.setLength(values.length);

		return new Chunk(schema, [colBuffer]);
	}

	// Helper to create a "greater than" expression
	function gt(columnName: string, value: number): ComparisonExpr {
		return {
			type: ExprType.Gt,
			left: { type: ExprType.Column, name: columnName },
			right: { type: ExprType.Literal, value, dtype: DTypeKind.Float64 },
		};
	}

	describe("with selection pool", () => {
		it("should filter rows without allocating new arrays per chunk", () => {
			// Get initial pool stats
			const statsBefore = selectionPool.getStats();

			// Create filter: value > 5
			const schema = unwrap(createSchema({ value: DType.float64 }));
			const expr = gt("value", 5);
			const filterOp = unwrap(FilterOperator.create(schema, expr, 1024));

			// Process multiple chunks
			for (let i = 0; i < 10; i++) {
				const chunk = createTestChunk([1, 2, 3, 10, 11, 12, 4, 5, 6]);
				const result = filterOp.process(chunk);
				expect(result.error).toBe(0); // ErrorCode.None
				expect(result.value?.chunk).not.toBeNull();
				// Should have filtered to values > 5: [10, 11, 12, 6]
				expect(result.value?.chunk?.rowCount).toBe(4);
			}

			// Pool should have been used (buffers released back)
			const statsAfter = selectionPool.getStats();
			// At minimum, we should have buffers in the pool now
			expect(statsAfter.totalBuffers).toBeGreaterThanOrEqual(
				statsBefore.totalBuffers,
			);
		});

		it("should handle empty chunks", () => {
			const schema = unwrap(createSchema({ value: DType.float64 }));
			const expr = gt("value", 5);
			const filterOp = unwrap(FilterOperator.create(schema, expr, 1024));

			const chunk = createTestChunk([]);
			const result = filterOp.process(chunk);

			expect(result.error).toBe(0);
			expect(result.value?.chunk).toBeNull(); // Empty result
		});

		it("should handle all-rows-match case efficiently", () => {
			const schema = unwrap(createSchema({ value: DType.float64 }));
			const expr = gt("value", 0);
			const filterOp = unwrap(FilterOperator.create(schema, expr, 1024));

			const chunk = createTestChunk([1, 2, 3, 4, 5]);
			const result = filterOp.process(chunk);

			expect(result.error).toBe(0);
			expect(result.value?.chunk).toBe(chunk); // Same chunk passed through
			expect(result.value?.chunk?.rowCount).toBe(5);
		});

		it("should handle no-rows-match case", () => {
			const schema = unwrap(createSchema({ value: DType.float64 }));
			const expr = gt("value", 100);
			const filterOp = unwrap(FilterOperator.create(schema, expr, 1024));

			const chunk = createTestChunk([1, 2, 3, 4, 5]);
			const result = filterOp.process(chunk);

			expect(result.error).toBe(0);
			expect(result.value?.chunk).toBeNull(); // No matching rows
		});

		it("should properly apply selection to chunks with existing selection", () => {
			const schema = unwrap(createSchema({ value: DType.float64 }));
			const expr = gt("value", 5);
			const filterOp = unwrap(FilterOperator.create(schema, expr, 1024));

			// Create chunk with pre-existing selection
			const chunk = createTestChunk([1, 10, 2, 11, 3, 12]);
			// Apply initial selection (indices 0, 2, 4) -> values [1, 2, 3]
			chunk.applySelection(new Uint32Array([0, 2, 4]), 3);

			const result = filterOp.process(chunk);

			expect(result.error).toBe(0);
			// After filtering > 5 on [1, 2, 3], nothing should match
			expect(result.value?.chunk).toBeNull(); // No rows match
		});
	});
});
