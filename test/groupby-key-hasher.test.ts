/**
 * Tests for GroupBy key hashing - numeric hash instead of string serialization
 *
 * TDD: Verify that GroupBy uses numeric hashing to avoid GC pressure
 */
/** biome-ignore-all lint/style/noNonNullAssertion: Fix non null later */

import { describe, expect, it } from "bun:test";
import { Chunk } from "../src/buffer/chunk.ts";
import { ColumnBuffer } from "../src/buffer/column-buffer.ts";
import { ExprType } from "../src/expr/ast.ts";
import type { AggSpec } from "../src/ops/aggregate.ts";
import { GroupByOperator } from "../src/ops/groupby.ts";
import { DType, DTypeKind } from "../src/types/dtypes.ts";
import { unwrap } from "../src/types/error.ts";
import { createSchema } from "../src/types/schema.ts";

describe("GroupByOperator Key Hashing", () => {
	// Helper to create a test chunk with multiple columns
	function createTestChunk(categories: number[], values: number[]): Chunk {
		const schema = unwrap(
			createSchema({
				category: DType.int32,
				value: DType.float64,
			}),
		);
		const catBuffer = new ColumnBuffer(
			DTypeKind.Int32,
			categories.length,
			false,
		);
		const valBuffer = new ColumnBuffer(DTypeKind.Float64, values.length, false);

		for (let i = 0; i < categories.length; i++) {
			catBuffer.set(i, categories[i]!);
			valBuffer.set(i, values[i]!);
		}
		catBuffer.setLength(categories.length);
		valBuffer.setLength(values.length);

		return new Chunk(schema, [catBuffer, valBuffer]);
	}

	// Helper to create aggregation specs
	function createSumAggSpec(column: string): AggSpec {
		return {
			name: "sum_value",
			expr: {
				type: ExprType.Sum,
				expr: { type: ExprType.Column, name: column },
			},
		};
	}

	describe("numeric key hashing", () => {
		it("should group by single column correctly", () => {
			const schema = unwrap(
				createSchema({
					category: DType.int32,
					value: DType.float64,
				}),
			);

			const groupByOp = unwrap(
				GroupByOperator.create(
					schema,
					["category"],
					[createSumAggSpec("value")],
				),
			);

			// Process chunk: [A:1, B:2, A:3, B:4, C:5] where A=1, B=2, C=3
			const chunk = createTestChunk([1, 2, 1, 2, 3], [10, 20, 30, 40, 50]);
			const result = groupByOp.process(chunk);
			expect(result.error).toBe(0);

			const finishResult = groupByOp.finish();
			expect(finishResult.error).toBe(0);
			expect(finishResult.value!.chunk).not.toBeNull();

			const resultChunk = finishResult.value!.chunk!;
			// Should have 3 groups: A(1), B(2), C(3)
			expect(resultChunk.rowCount).toBe(3);
		});

		it("should handle multiple groups efficiently", () => {
			const schema = unwrap(
				createSchema({
					category: DType.int32,
					value: DType.float64,
				}),
			);

			const groupByOp = unwrap(
				GroupByOperator.create(
					schema,
					["category"],
					[createSumAggSpec("value")],
				),
			);

			// Process many chunks with many groups
			for (let i = 0; i < 10; i++) {
				const categories: number[] = [];
				const values: number[] = [];
				for (let j = 0; j < 100; j++) {
					categories.push(j % 20); // 20 unique groups
					values.push(j);
				}
				const chunk = createTestChunk(categories, values);
				const result = groupByOp.process(chunk);
				expect(result.error).toBe(0);
			}

			const finishResult = groupByOp.finish();
			expect(finishResult.error).toBe(0);
			expect(finishResult.value!.chunk!.rowCount).toBe(20); // 20 unique groups
		});

		it("should handle null values in keys", () => {
			const schema = unwrap(
				createSchema({
					category: DType.int32,
					value: DType.float64,
				}),
			);

			const groupByOp = unwrap(
				GroupByOperator.create(
					schema,
					["category"],
					[createSumAggSpec("value")],
				),
			);

			// Create chunk with nullable column
			const catBuffer = new ColumnBuffer(DTypeKind.Int32, 5, true); // nullable
			const valBuffer = new ColumnBuffer(DTypeKind.Float64, 5, false);

			catBuffer.set(0, 1);
			catBuffer.set(1, 2);
			catBuffer.setNull(2, true); // null
			catBuffer.set(3, 1);
			catBuffer.set(4, 2);

			valBuffer.set(0, 10);
			valBuffer.set(1, 20);
			valBuffer.set(2, 30);
			valBuffer.set(3, 40);
			valBuffer.set(4, 50);

			catBuffer.setLength(5);
			valBuffer.setLength(5);

			const chunk = new Chunk(schema, [catBuffer, valBuffer]);
			const result = groupByOp.process(chunk);
			expect(result.error).toBe(0);

			const finishResult = groupByOp.finish();
			expect(finishResult.error).toBe(0);
			// Should have 3 groups: 1, 2, and null
			expect(finishResult.value!.chunk!.rowCount).toBe(3);
		});

		it("should correctly aggregate sum values", () => {
			const schema = unwrap(
				createSchema({
					category: DType.int32,
					value: DType.float64,
				}),
			);

			const groupByOp = unwrap(
				GroupByOperator.create(
					schema,
					["category"],
					[createSumAggSpec("value")],
				),
			);

			// Process: A=1: 10+30=40, B=2: 20+40=60
			const chunk = createTestChunk([1, 2, 1, 2], [10, 20, 30, 40]);
			const result = groupByOp.process(chunk);
			expect(result.error).toBe(0);

			const finishResult = groupByOp.finish();
			expect(finishResult.error).toBe(0);

			const resultChunk = finishResult.value!.chunk!;
			expect(resultChunk.rowCount).toBe(2);

			// Get the sum column (index 1, after key column)
			const sumCol = resultChunk.getColumn(1);
			expect(sumCol).not.toBeUndefined();

			// Sums should be 40 and 60 (order may vary)
			const sums = [sumCol!.get(0), sumCol!.get(1)].sort(
				(a, b) => Number(a) - Number(b),
			);
			expect(sums[0]).toBe(40);
			expect(sums[1]).toBe(60);
		});

		it("should handle bigint key values", () => {
			const schema = unwrap(
				createSchema({
					id: DType.int64,
					value: DType.float64,
				}),
			);

			const groupByOp = unwrap(
				GroupByOperator.create(schema, ["id"], [createSumAggSpec("value")]),
			);

			// Create chunk with bigint column
			const idBuffer = new ColumnBuffer(DTypeKind.Int64, 4, false);
			const valBuffer = new ColumnBuffer(DTypeKind.Float64, 4, false);

			idBuffer.set(0, 9007199254740993n); // Large bigint
			idBuffer.set(1, 9007199254740994n);
			idBuffer.set(2, 9007199254740993n); // Same as first
			idBuffer.set(3, 9007199254740994n); // Same as second

			valBuffer.set(0, 10);
			valBuffer.set(1, 20);
			valBuffer.set(2, 30);
			valBuffer.set(3, 40);

			idBuffer.setLength(4);
			valBuffer.setLength(4);

			const chunk = new Chunk(schema, [idBuffer, valBuffer]);
			const result = groupByOp.process(chunk);
			expect(result.error).toBe(0);

			const finishResult = groupByOp.finish();
			expect(finishResult.error).toBe(0);
			// Should have 2 groups
			expect(finishResult.value!.chunk!.rowCount).toBe(2);
		});
	});
});
