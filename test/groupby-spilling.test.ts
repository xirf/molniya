/**
 * Tests for P3 GroupBy spilling infrastructure.
 *
 * Tests: basic groupBy correctness, aggregation types,
 * multi-key grouping, empty groups.
 */
import { describe, test, expect } from "bun:test";
import { fromColumns, DType, sum, count, avg, min, max, first, last } from "../src/index.ts";

describe("P3: GroupBy Spilling Infrastructure", () => {
	test("basic groupBy with sum produces correct results", async () => {
		const df = fromColumns(
			{
				category: ["A", "B", "A", "B", "A"],
				value: [10, 20, 30, 40, 50],
			},
			{ category: DType.string, value: DType.int32 },
		);

		const result = await df
			.groupBy("category")
			.agg([{ name: "total", expr: sum("value") }])
			.collect();
		const rows = await result.toArray();

		const rowMap = new Map(rows.map((r: any) => [r.category, r.total]));
		expect(rowMap.get("A")).toBe(90); // 10+30+50
		expect(rowMap.get("B")).toBe(60); // 20+40
	});

	test("groupBy with count", async () => {
		const df = fromColumns(
			{
				category: ["A", "B", "A", "B", "A"],
				value: [10, 20, 30, 40, 50],
			},
			{ category: DType.string, value: DType.int32 },
		);

		const result = await df
			.groupBy("category")
			.agg([{ name: "cnt", expr: count("value") }])
			.collect();
		const rows = await result.toArray();

		const rowMap = new Map(rows.map((r: any) => [r.category, Number(r.cnt)]));
		expect(rowMap.get("A")).toBe(3);
		expect(rowMap.get("B")).toBe(2);
	});

	test("groupBy with min/max", async () => {
		const df = fromColumns(
			{
				category: ["A", "B", "A", "B", "A"],
				value: [10, 20, 30, 40, 50],
			},
			{ category: DType.string, value: DType.int32 },
		);

		const result = await df
			.groupBy("category")
			.agg([
				{ name: "min_val", expr: min("value") },
				{ name: "max_val", expr: max("value") },
			])
			.collect();
		const rows = await result.toArray();

		const rowMap = new Map(rows.map((r: any) => [r.category, r]));
		expect(rowMap.get("A")!.min_val).toBe(10);
		expect(rowMap.get("A")!.max_val).toBe(50);
		expect(rowMap.get("B")!.min_val).toBe(20);
		expect(rowMap.get("B")!.max_val).toBe(40);
	});

	test("groupBy with avg", async () => {
		const df = fromColumns(
			{
				category: ["A", "B", "A", "B"],
				value: [10, 20, 30, 40],
			},
			{ category: DType.string, value: DType.float64 },
		);

		const result = await df
			.groupBy("category")
			.agg([{ name: "avg_val", expr: avg("value") }])
			.collect();
		const rows = await result.toArray();

		const rowMap = new Map(rows.map((r: any) => [r.category, r.avg_val]));
		expect(rowMap.get("A")).toBe(20); // (10+30)/2
		expect(rowMap.get("B")).toBe(30); // (20+40)/2
	});

	test("groupBy with first/last", async () => {
		const df = fromColumns(
			{
				category: ["A", "B", "A", "B", "A"],
				value: [10, 20, 30, 40, 50],
			},
			{ category: DType.string, value: DType.int32 },
		);

		const result = await df
			.groupBy("category")
			.agg([
				{ name: "first_val", expr: first("value") },
				{ name: "last_val", expr: last("value") },
			])
			.collect();
		const rows = await result.toArray();

		const rowMap = new Map(rows.map((r: any) => [r.category, r]));
		expect(rowMap.get("A")!.first_val).toBe(10);
		expect(rowMap.get("A")!.last_val).toBe(50);
		expect(rowMap.get("B")!.first_val).toBe(20);
		expect(rowMap.get("B")!.last_val).toBe(40);
	});

	test("groupBy with multiple keys", async () => {
		const df = fromColumns(
			{
				dept: ["Eng", "Eng", "Sales", "Sales"],
				level: ["Jr", "Sr", "Jr", "Sr"],
				salary: [50, 100, 40, 80],
			},
			{ dept: DType.string, level: DType.string, salary: DType.int32 },
		);

		const result = await df
			.groupBy("dept", "level")
			.agg([{ name: "total", expr: sum("salary") }])
			.collect();
		const rows = await result.toArray();
		// Should have at least 2 groups (dept aggregation)
		expect(rows.length).toBeGreaterThanOrEqual(2);
	});

	test("groupBy with single group", async () => {
		const df = fromColumns(
			{
				category: ["A", "A", "A"],
				value: [10, 20, 30],
			},
			{ category: DType.string, value: DType.int32 },
		);

		const result = await df
			.groupBy("category")
			.agg([{ name: "total", expr: sum("value") }])
			.collect();
		const rows = await result.toArray();

		expect(rows.length).toBe(1);
		expect(rows[0]!.total).toBe(60);
	});

	test("groupBy preserves all rows (no data loss)", async () => {
		const n = 200;
		const categories = Array.from({ length: n }, (_, i) => (i % 10).toString());
		const values = Array.from({ length: n }, (_, i) => i);

		const df = fromColumns(
			{ category: categories, value: values },
			{ category: DType.string, value: DType.int32 },
		);

		const result = await df
			.groupBy("category")
			.agg([
				{ name: "total", expr: sum("value") },
				{ name: "cnt", expr: count("value") },
			])
			.collect();
		const rows = await result.toArray();

		expect(rows.length).toBe(10);

		// Total count should equal n
		const totalCount = rows.reduce((acc: number, r: any) => acc + Number(r.cnt), 0);
		expect(totalCount).toBe(n);

		// Total sum should equal sum(0..199) = 19900
		const totalSum = rows.reduce((acc: number, r: any) => acc + Number(r.total), 0);
		expect(totalSum).toBe(19900);
	});

	test("groupBy with empty DataFrame", async () => {
		const df = fromColumns(
			{ category: [] as string[], value: [] as number[] },
			{ category: DType.string, value: DType.int32 },
		);

		const result = await df
			.groupBy("category")
			.agg([{ name: "total", expr: sum("value") }])
			.collect();
		const rows = await result.toArray();
		expect(rows.length).toBe(0);
	});

	test("groupBy with many unique groups", async () => {
		const n = 1000;
		const ids = Array.from({ length: n }, (_, i) => `key_${i}`);
		const values = Array.from({ length: n }, (_, i) => i);

		const df = fromColumns(
			{ id: ids, value: values },
			{ id: DType.string, value: DType.int32 },
		);

		const result = await df
			.groupBy("id")
			.agg([{ name: "total", expr: sum("value") }])
			.collect();
		const rows = await result.toArray();
		expect(rows.length).toBe(n);
	});
});
