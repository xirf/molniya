/**
 * End-to-end integration tests for the full optimization stack.
 *
 * Tests that filter → sort → groupBy → collect pipelines work correctly
 * through the streaming engine with all optimizations active.
 */
import { describe, test, expect } from "bun:test";
import {
	fromColumns,
	fromCsvString,
	col,
	DType,
	sum,
	count,
	avg,
	lit,
	desc,
} from "../src/index.ts";

describe("Integration: multi-operator pipelines", () => {
	test("filter → sort → collect", async () => {
		const df = fromColumns(
			{
				id: [5, 3, 8, 1, 6, 2, 9, 4, 7, 10],
				value: [50, 30, 80, 10, 60, 20, 90, 40, 70, 100],
			},
			{ id: DType.int32, value: DType.int32 },
		);

		const result = await df
			.filter(col("value").gt(lit(40)))
			.sort("value")
			.collect();
		const rows = await result.toArray();

		expect(rows.length).toBe(6);
		expect(rows[0]!.value).toBe(50);
		expect(rows[rows.length - 1]!.value).toBe(100);
		// Verify sorted order
		for (let i = 1; i < rows.length; i++) {
			expect(rows[i]!.value).toBeGreaterThanOrEqual(rows[i - 1]!.value);
		}
	});

	test("filter → groupBy → collect", async () => {
		const df = fromColumns(
			{
				category: ["A", "B", "A", "B", "A", "B", "A", "B"],
				value: [10, 20, 30, 40, 50, 60, 70, 80],
			},
			{ category: DType.string, value: DType.int32 },
		);

		const result = await df
			.filter(col("value").gt(lit(25)))
			.groupBy("category")
			.agg([
				{ name: "total", expr: sum("value") },
				{ name: "cnt", expr: count("value") },
			])
			.collect();
		const rows = await result.toArray();

		const rowMap = new Map(rows.map((r: any) => [r.category, r]));
		// A: 30+50+70 = 150, count=3
		expect(rowMap.get("A")!.total).toBe(150);
		expect(Number(rowMap.get("A")!.cnt)).toBe(3);
		// B: 40+60+80 = 180, count=3
		expect(rowMap.get("B")!.total).toBe(180);
		expect(Number(rowMap.get("B")!.cnt)).toBe(3);
	});

	test("sort → limit → collect", async () => {
		const df = fromColumns(
			{
				id: [5, 3, 8, 1, 6, 2, 9, 4, 7, 10],
				name: ["e", "c", "h", "a", "f", "b", "i", "d", "g", "j"],
			},
			{ id: DType.int32, name: DType.string },
		);

		const result = await df.sort("id").limit(3).collect();
		const rows = await result.toArray();

		expect(rows.length).toBe(3);
		expect(rows[0]!.id).toBe(1);
		expect(rows[1]!.id).toBe(2);
		expect(rows[2]!.id).toBe(3);
	});

	test("CSV source → filter → sort → collect", async () => {
		const csv = `id,name,score
1,Alice,85
2,Bob,92
3,Charlie,78
4,Diana,95
5,Eve,88
6,Frank,71
7,Grace,99`;

		const df = fromCsvString(csv, {
			id: DType.int32,
			name: DType.string,
			score: DType.int32,
		});
		const result = await df
			.filter(col("score").gt(lit(80)))
			.sort(desc("score"))
			.collect();
		const rows = await result.toArray();

		expect(rows.length).toBe(5);
		// Descending: 99, 95, 92, 88, 85
		expect(rows[0]!.name).toBe("Grace");
		expect(rows[1]!.name).toBe("Diana");
		expect(rows[2]!.name).toBe("Bob");
		expect(rows[3]!.name).toBe("Eve");
		expect(rows[4]!.name).toBe("Alice");
	});

	test("CSV source → filter → groupBy → collect", async () => {
		const csv = `dept,level,salary
Eng,Jr,50000
Eng,Sr,90000
Sales,Jr,40000
Sales,Sr,70000
Eng,Jr,55000
Eng,Sr,95000
Sales,Jr,42000
Sales,Sr,72000`;

		const df = fromCsvString(csv, {
			dept: DType.string,
			level: DType.string,
			salary: DType.int32,
		});
		const result = await df
			.filter(col("salary").gt(lit(45000)))
			.groupBy("dept")
			.agg([
				{ name: "cnt", expr: count("salary") },
				{ name: "avg_sal", expr: avg("salary") },
			])
			.collect();
		const rows = await result.toArray();

		const rowMap = new Map(rows.map((r: any) => [r.dept, r]));
		// Eng after filter: 50000,90000,55000,95000 → count=4
		expect(Number(rowMap.get("Eng")!.cnt)).toBe(4);
		// Sales after filter: 70000,72000 → count=2
		expect(Number(rowMap.get("Sales")!.cnt)).toBe(2);
	});

	test("filter fusion: multiple filters combine correctly", async () => {
		const df = fromColumns(
			{
				a: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
				b: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
			},
			{ a: DType.int32, b: DType.int32 },
		);

		// Two separate filters should fuse via AND
		const result = await df
			.filter(col("a").gt(lit(3)))
			.filter(col("a").lt(lit(8)))
			.collect();
		const rows = await result.toArray();

		// a in {4,5,6,7}
		expect(rows.length).toBe(4);
		const aValues = rows.map((r: any) => r.a).sort();
		expect(aValues).toEqual([4, 5, 6, 7]);
	});

	test("stream() yields all rows through pipeline", async () => {
		const df = fromColumns(
			{
				id: Array.from({ length: 100 }, (_, i) => i),
				value: Array.from({ length: 100 }, (_, i) => i * 10),
			},
			{ id: DType.int32, value: DType.int32 },
		);

		let rowCount = 0;
		for await (const chunk of df.filter(col("value").gte(lit(500))).stream()) {
			rowCount += chunk.rowCount;
		}
		expect(rowCount).toBe(50); // ids 50-99
	});

	test("select + filter + sort pipeline", async () => {
		const df = fromColumns(
			{
				a: [3, 1, 4, 1, 5, 9, 2, 6],
				b: [30, 10, 40, 10, 50, 90, 20, 60],
				c: ["x", "y", "x", "y", "x", "y", "x", "y"],
			},
			{ a: DType.int32, b: DType.int32, c: DType.string },
		);

		const result = await df
			.select("a", "b")
			.filter(col("a").gt(lit(2)))
			.sort("a")
			.collect();
		const rows = await result.toArray();

		expect(rows.length).toBe(5); // 3,4,5,9,6
		expect(rows[0]!.a).toBe(3);
		expect(rows[rows.length - 1]!.a).toBe(9);
	});

	test("count() aggregation through stream", async () => {
		const df = fromColumns(
			{
				category: Array.from({ length: 500 }, (_, i) => (i % 5).toString()),
				value: Array.from({ length: 500 }, (_, i) => i),
			},
			{ category: DType.string, value: DType.int32 },
		);

		const result = await df
			.groupBy("category")
			.agg([{ name: "cnt", expr: count("value") }])
			.collect();
		const rows = await result.toArray();

		expect(rows.length).toBe(5);
		// Each category should have 100 entries
		for (const row of rows) {
			expect(Number((row as any).cnt)).toBe(100);
		}
	});

	test("empty DataFrame through full pipeline", async () => {
		const df = fromColumns(
			{ id: [] as number[], value: [] as number[] },
			{ id: DType.int32, value: DType.int32 },
		);

		const result = await df
			.filter(col("id").gt(lit(0)))
			.sort("value")
			.collect();
		const rows = await result.toArray();

		expect(rows.length).toBe(0);
	});

	test("large dataset through filter + groupBy + sort", async () => {
		const n = 2000;
		const df = fromColumns(
			{
				group: Array.from({ length: n }, (_, i) => `grp_${i % 20}`),
				value: Array.from({ length: n }, (_, i) => i),
			},
			{ group: DType.string, value: DType.int32 },
		);

		const result = await df
			.filter(col("value").gte(lit(1000)))
			.groupBy("group")
			.agg([
				{ name: "total", expr: sum("value") },
				{ name: "cnt", expr: count("value") },
			])
			.collect();
		const rows = await result.toArray();

		expect(rows.length).toBe(20);
		// total count should be 1000 (values 1000..1999)
		const totalCount = rows.reduce((acc: number, r: any) => acc + Number(r.cnt), 0);
		expect(totalCount).toBe(1000);
	});
});
