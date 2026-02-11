import { describe, test, expect } from "bun:test";
import { fromColumns, fromRecords, col } from "../src/dataframe/index.ts";
import { DType } from "../src/types/dtypes.ts";

describe("Phase 1-3 Optimized APIs", () => {
	const df = fromColumns(
		{
			name: ["Alice", "Bob", "Charlie", "Diana", "Eve"],
			age: [30, 25, 35, 28, 32],
			active: [true, false, true, true, false],
		},
		{ name: DType.string, age: DType.float64, active: DType.boolean },
	);

	describe("toColumns()", () => {
		test("should return columnar data", async () => {
			const cols = await df.toColumns();
			expect(cols.name).toEqual(["Alice", "Bob", "Charlie", "Diana", "Eve"]);
			expect(cols.active).toEqual([true, false, true, true, false]);
		});

		test("should return typed arrays for numeric columns", async () => {
			const cols = await df.toColumns();
			expect(cols.age).toBeInstanceOf(Float64Array);
			expect(Array.from(cols.age as Float64Array)).toEqual([30, 25, 35, 28, 32]);
		});

		test("should work with filtered data", async () => {
			const cols = await df.filter(col("age").gt(30)).toColumns();
			expect(cols.name).toEqual(["Charlie", "Eve"]);
		});
	});

	describe("forEach()", () => {
		test("should iterate all rows", async () => {
			const names: string[] = [];
			await df.forEach((row: any) => {
				names.push(row.name);
			});
			expect(names).toEqual(["Alice", "Bob", "Charlie", "Diana", "Eve"]);
		});

		test("should work with filtered data", async () => {
			const names: string[] = [];
			await df.filter(col("active").eq(true)).forEach((row: any) => {
				names.push(row.name);
			});
			expect(names).toEqual(["Alice", "Charlie", "Diana"]);
		});
	});

	describe("reduce()", () => {
		test("should accumulate a result", async () => {
			const totalAge = await df.reduce((acc: number, row: any) => acc + row.age, 0);
			expect(totalAge).toBe(150);
		});

		test("should work with filtered data", async () => {
			const totalAge = await df
				.filter(col("active").eq(true))
				.reduce((acc: number, row: any) => acc + row.age, 0);
			expect(totalAge).toBe(93); // 30 + 35 + 28
		});
	});

	describe("Dictionary-aware optimizations", () => {
		test("string eq/neq should work with dictionary indices", async () => {
			const result = await df.filter(col("name").eq("Alice")).toArray();
			expect(result.length).toBe(1);
			expect(result[0]?.name).toBe("Alice");

			const neqResult = await df.filter(col("name").neq("Alice")).toArray();
			expect(neqResult.length).toBe(4);
		});

		test("string isIn should work with dictionary indices", async () => {
			const result = await df.filter(col("name").isIn(["Alice", "Eve"])).toArray();
			expect(result.length).toBe(2);
		});

		test("byte-level contains should work", async () => {
			const result = await df.filter(col("name").contains("li")).toArray();
			expect(result.length).toBe(2); // Alice, Charlie
		});

		test("byte-level startsWith should work", async () => {
			const result = await df.filter(col("name").startsWith("D")).toArray();
			expect(result.length).toBe(1);
			expect(result[0]?.name).toBe("Diana");
		});

		test("byte-level endsWith should work", async () => {
			const result = await df.filter(col("name").endsWith("e")).toArray();
			expect(result.length).toBe(3); // Alice, Charlie, Eve
		});
	});

	describe("Batch toArray() correctness", () => {
		test("should handle nulls correctly", async () => {
			const dfNull = fromRecords(
				[
					{ id: 1, name: "Alice", score: null },
					{ id: 2, name: null, score: 85 },
				],
				{ id: DType.int32, name: DType.nullable.string, score: DType.nullable.float64 },
			);
			const rows = await dfNull.toArray();
			expect(rows[0]?.score).toBeNull();
			expect(rows[1]?.name).toBeNull();
			expect(rows[0]?.name).toBe("Alice");
			expect(rows[1]?.score).toBe(85);
		});

		test("should handle boolean columns", async () => {
			const rows = await df.toArray();
			expect(rows[0]?.active).toBe(true);
			expect(rows[1]?.active).toBe(false);
		});
	});
});
