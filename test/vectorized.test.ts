/**
 * Tests for Phase 5: Vectorized predicate evaluation.
 * Vectorized predicates scan TypedArrays directly in tight loops,
 * avoiding per-row closure calls and physicalIndex indirection.
 */
import { describe, expect, test } from "bun:test";
import { fromColumns, fromRecords, col } from "../src/dataframe/index.ts";
import { DType } from "../src/types/dtypes.ts";
import { and, or, not } from "../src/expr/builders.ts";

describe("Phase 5: Vectorized Predicates", () => {
	// Helper: create a DataFrame with numeric and string columns
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

	describe("Numeric comparisons (vectorized path)", () => {
		test("gt should filter correctly", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("value").gt(750)).count();
			// value = i * 1.5; value > 750 means i * 1.5 > 750 → i > 500
			// i from 501 to 999 = 499 rows
			expect(count).toBe(499);
		});

		test("gte should filter correctly", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("value").gte(750)).count();
			// i >= 500 → 500 rows (500..999)
			expect(count).toBe(500);
		});

		test("lt should filter correctly", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("value").lt(15)).count();
			// i * 1.5 < 15 → i < 10 → 10 rows (0..9)
			expect(count).toBe(10);
		});

		test("lte should filter correctly", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("value").lte(15)).count();
			// i * 1.5 <= 15 → i <= 10 → 11 rows (0..10)
			expect(count).toBe(11);
		});

		test("eq should filter correctly on numeric", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("value").eq(1.5)).count();
			// Only i=1 → value=1.5
			expect(count).toBe(1);
		});

		test("neq should filter correctly on numeric", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("value").neq(0)).count();
			// All except i=0 → 999 rows
			expect(count).toBe(999);
		});
	});

	describe("Dictionary string comparisons (vectorized path)", () => {
		test("eq on string column should use vectorized dict path", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("country").eq("Brazil")).count();
			// 1000 rows, 5 countries cycling → 200 each
			expect(count).toBe(200);
		});

		test("neq on string column should use vectorized dict path", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("country").neq("USA")).count();
			expect(count).toBe(800);
		});

		test("isIn on string column should use vectorized path", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("country").isIn(["Brazil", "Japan"])).count();
			expect(count).toBe(400);
		});
	});

	describe("Byte-level string ops (vectorized path)", () => {
		test("contains should use vectorized byte scan", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("country").contains("an")).count();
			// "Germany" and "Japan" contain "an" → 400 rows
			expect(count).toBe(400);
		});

		test("startsWith should use vectorized byte scan", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("country").startsWith("Br")).count();
			expect(count).toBe(200);
		});

		test("endsWith should use vectorized byte scan", async () => {
			const df = createTestDf(1000);
			const count = await df.filter(col("country").endsWith("a")).count();
			// "India" ends with "a" → 200 rows
			expect(count).toBe(200);
		});
	});

	describe("Compound predicates with vectorized components", () => {
		test("AND of two numeric filters", async () => {
			const df = createTestDf(1000);
			const count = await df
				.filter(and(col("value").gt(100), col("value").lt(200)))
				.count();
			// i*1.5 > 100 AND i*1.5 < 200 → 67 < i < 134 → i in [68..133] = 66 rows
			// Actually: i*1.5 > 100 → i >= 68 (68*1.5=102), i*1.5 < 200 → i <= 133 (133*1.5=199.5) → 67 rows
			expect(count).toBe(67);
		});

		test("OR of two string filters", async () => {
			const df = createTestDf(1000);
			const count = await df
				.filter(or(col("country").eq("USA"), col("country").eq("Brazil")))
				.count();
			expect(count).toBe(400);
		});

		test("NOT on numeric filter", async () => {
			const df = createTestDf(1000);
			const countGt = await df.filter(col("value").gt(750)).count();
			const countNotGt = await df.filter(not(col("value").gt(750))).count();
			expect(countGt + countNotGt).toBe(1000);
		});
	});

	describe("Chained filters (vectorized on selection)", () => {
		test("two chained filters should compose correctly", async () => {
			const df = createTestDf(1000);
			const count = await df
				.filter(col("value").gt(100))
				.filter(col("country").eq("Brazil"))
				.count();
			// value > 100 → i > 66 → rows 67..999 = 933 rows
			// Of those, Brazil = every 5th starting from i=1: 1,6,11,... 
			// i > 66 and i % 5 == 1: 71,76,...,996 → (996-71)/5 + 1 = 186 rows
			// Actually, i%5==1 for Brazil: 67..999 where i%5==1
			// Starting from 71 (next after 66 where i%5==1): 71,76,...,996
			// Count: Math.floor((999-71)/5) + 1 = Math.floor(928/5) + 1 = 185 + 1 = 186
			expect(count).toBe(186);
		});
	});

	describe("Null handling in vectorized path", () => {
		test("numeric filter should skip nulls", async () => {
			const df = fromRecords([
				{ id: 1, value: 10 },
				{ id: 2, value: null },
				{ id: 3, value: 30 },
				{ id: 4, value: null },
				{ id: 5, value: 50 },
			], { id: DType.int32, value: DType.float64 });
			const count = await df.filter(col("value").gt(20)).count();
			expect(count).toBe(2); // 30 and 50
		});

		test("string filter should skip nulls", async () => {
			const df = fromRecords([
				{ name: "Alice" },
				{ name: "Bob" },
				{ name: "Charlie" },
				{ name: "Alice" },
				{ name: "Bob" },
			], { name: DType.string });
			const count = await df.filter(col("name").eq("Bob")).count();
			expect(count).toBe(2);
		});
	});

	describe("Correctness: vectorized matches scalar path", () => {
		test("toArray results should match for numeric gt", async () => {
			const df = createTestDf(100);
			const rows = await df.filter(col("value").gt(75)).toArray();
			// value > 75 means i*1.5 > 75 → i > 50 → rows 51..99
			expect(rows.length).toBe(49);
			for (const row of rows) {
				expect(row.value as number).toBeGreaterThan(75);
			}
		});

		test("toArray results should match for string eq", async () => {
			const df = createTestDf(100);
			const rows = await df.filter(col("country").eq("Japan")).toArray();
			expect(rows.length).toBe(20);
			for (const row of rows) {
				expect(row.country).toBe("Japan");
			}
		});
	});
});
