/**
 * Tests for additional features: between() and tail()
 */

import { describe, expect, it } from "bun:test";
import { between, col, DType, fromColumns } from "../src/index.ts";

describe("Additional Features", () => {
	describe("between() function", () => {
		it("should filter values between range", async () => {
			const df = fromColumns(
				{ value: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] },
				{ value: DType.int32 },
			);

			const result = await df.filter(between("value", 3, 7)).toArray();

			expect(result).toHaveLength(5);
			const values = result.map((r) => r.value).sort((a, b) => a - b);
			expect(values).toEqual([3, 4, 5, 6, 7]);
		});

		it("should work with col() expression", async () => {
			const df = fromColumns(
				{ value: [1, 5, 10, 15, 20] },
				{ value: DType.int32 },
			);

			const result = await df.filter(between(col("value"), 5, 15)).toArray();

			expect(result).toHaveLength(3);
			const values = result.map((r) => r.value).sort((a, b) => a - b);
			expect(values).toEqual([5, 10, 15]);
		});

		it("should handle edge cases", async () => {
			function getDf() {
				return fromColumns(
					{ value: [1, 2, 3, 4, 5] },
					{ value: DType.int32 },
				);
			}

			// All values in range
			const result1 = await getDf().filter(between("value", 1, 5)).toArray();
			expect(result1).toHaveLength(5);

			// No values in range
			const result2 = await getDf().filter(between("value", 10, 20)).toArray();
			expect(result2).toHaveLength(0);

			// Single value in range
			const result3 = await getDf().filter(between("value", 3, 3)).toArray();
			expect(result3).toHaveLength(1);
			expect(result3[0].value).toBe(3);
		});
	});

	describe("tail() method", () => {
		it("should return last n rows", async () => {
			const df = fromColumns(
				{ value: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] },
				{ value: DType.int32 },
			);

			const result = await (await df.tail(3)).toArray();

			expect(result).toHaveLength(3);
			expect(result[0].value).toBe(8);
			expect(result[1].value).toBe(9);
			expect(result[2].value).toBe(10);
		});

		it("should default to 5 rows", async () => {
			const df = fromColumns(
				{ value: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] },
				{ value: DType.int32 },
			);

			const result = await (await df.tail()).toArray();

			expect(result).toHaveLength(5);
			expect(result[0].value).toBe(6);
			expect(result[4].value).toBe(10);
		});

		it("should handle fewer rows than requested", async () => {
			const df = fromColumns({ value: [1, 2, 3] }, { value: DType.int32 });

			const result = await (await df.tail(10)).toArray();

			expect(result).toHaveLength(3);
			expect(result[0].value).toBe(1);
			expect(result[2].value).toBe(3);
		});

		it("should work with empty dataframe", async () => {
			const df = fromColumns({ value: [] }, { value: DType.int32 });

			const result = await (await df.tail(5)).toArray();

			expect(result).toHaveLength(0);
		});
	});
});
