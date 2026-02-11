/**
 * Tests for Phase 6: Query Optimizer.
 * Tests filter fusion, predicate pushdown, and limit pushdown.
 */
import { describe, expect, test } from "bun:test";
import { fromColumns, col } from "../src/dataframe/index.ts";
import { DType } from "../src/types/dtypes.ts";
import { and } from "../src/expr/builders.ts";

describe("Phase 6: Query Optimizer", () => {
	function createTestDf(n: number = 1000) {
		const countries = ["USA", "Brazil", "Germany", "Japan", "India"];
		const ids = new Float64Array(n);
		const values = new Float64Array(n);
		const countryCol = new Array<string>(n);
		for (let i = 0; i < n; i++) {
			ids[i] = i;
			values[i] = i * 1.5;
			countryCol[i] = countries[i % countries.length]!;
		}
		return fromColumns(
			{ id: ids, value: values, country: countryCol },
			{ id: DType.float64, value: DType.float64, country: DType.string },
		);
	}

	describe("Filter fusion", () => {
		test("two chained filters should produce correct results", async () => {
			const df = createTestDf(1000);
			// Two chained filters = same as AND
			const chainedCount = await df
				.filter(col("value").gt(100))
				.filter(col("value").lt(500))
				.count();
			const andCount = await df
				.filter(and(col("value").gt(100), col("value").lt(500)))
				.count();
			expect(chainedCount).toBe(andCount);
		});

		test("three chained filters should fuse correctly", async () => {
			const df = createTestDf(1000);
			const count = await df
				.filter(col("value").gt(100))
				.filter(col("value").lt(500))
				.filter(col("country").eq("Brazil"))
				.count();
			const andCount = await df
				.filter(and(col("value").gt(100), col("value").lt(500), col("country").eq("Brazil")))
				.count();
			expect(count).toBe(andCount);
		});

		test("filter fusion should work with mixed numeric and string", async () => {
			const df = createTestDf(1000);
			const count = await df
				.filter(col("country").eq("Japan"))
				.filter(col("value").gt(300))
				.count();
			// Japan is every 5th row starting from i=3: value = i*1.5 > 300 → i > 200
			// i%5==3 and i>200: 203, 208, 213, ..., 998
			// (998-203)/5 + 1 = 795/5 + 1 = 159 + 1 = 160
			expect(count).toBe(160);
		});

		test("filter + select should work correctly", async () => {
			const df = createTestDf(100);
			const rows = await df
				.filter(col("value").gt(50))
				.select("country", "value")
				.toArray();
			// value > 50 → i*1.5 > 50 → i > 33 → 34..99 = 66 rows
			expect(rows.length).toBe(66);
			for (const row of rows) {
				expect(row.value as number).toBeGreaterThan(50);
				expect(row).not.toHaveProperty("id"); // id should be dropped
			}
		});
	});

	describe("Limit pushdown", () => {
		test("filter + limit should stop early", async () => {
			const df = createTestDf(1000);
			const rows = await df
				.filter(col("value").gt(100))
				.limit(5)
				.toArray();
			expect(rows.length).toBe(5);
			for (const row of rows) {
				expect(row.value as number).toBeGreaterThan(100);
			}
		});

		test("filter + limit + count should respect limit", async () => {
			const df = createTestDf(1000);
			const count = await df
				.filter(col("value").gt(0))
				.limit(10)
				.count();
			expect(count).toBe(10);
		});
	});

	describe("Optimizer correctness edge cases", () => {
		test("single filter should not be affected by optimizer", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("value").gt(500)).count();
			// i*1.5 > 500 → i > 333 → 334..999 = 666 rows
			expect(count).toBe(666);
		});

		test("filter after orderBy should work", async () => {
			const df = createTestDf(100);
			const rows = await df
				.orderBy("value") // asc
				.filter(col("value").lt(15))
				.limit(5)
				.toArray();
			expect(rows.length).toBe(5);
			for (const row of rows) {
				expect(row.value as number).toBeLessThan(15);
			}
		});

		test("empty result from fused filter should work", async () => {
			const df = createTestDf(1000);
			const count = await df
				.filter(col("value").gt(999999))
				.filter(col("country").eq("Brazil"))
				.count();
			expect(count).toBe(0);
		});
	});
});
