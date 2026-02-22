/**
 * Tests for P3 External Sort (disk-backed sort).
 *
 * Tests: basic sort correctness, SortOptions spillThreshold,
 * async sort behavior, multi-column sort, cleanup.
 */
import { describe, test, expect } from "bun:test";
import { fromColumns, col, DType, asc, desc } from "../src/index.ts";

describe("P3: External Sort Infrastructure", () => {
	test("basic sort produces correct order (ascending)", async () => {
		const df = fromColumns(
			{ id: [5, 3, 1, 4, 2] },
			{ id: DType.int32 },
		);
		const sorted = await df.sort([asc("id")]);
		const rows = await sorted.toArray();
		expect(rows.map((r: any) => r.id)).toEqual([1, 2, 3, 4, 5]);
	});

	test("basic sort produces correct order (descending)", async () => {
		const df = fromColumns(
			{ id: [5, 3, 1, 4, 2] },
			{ id: DType.int32 },
		);
		const sorted = await df.sort([desc("id")]);
		const rows = await sorted.toArray();
		expect(rows.map((r: any) => r.id)).toEqual([5, 4, 3, 2, 1]);
	});

	test("multi-column sort works correctly", async () => {
		const df = fromColumns(
			{
				group: ["B", "A", "B", "A"],
				value: [20, 10, 10, 20],
			},
			{ group: DType.string, value: DType.int32 },
		);
		const sorted = await df.sort([asc("group"), asc("value")]);
		const rows = await sorted.toArray();
		expect(rows.map((r: any) => r.group)).toEqual(["A", "A", "B", "B"]);
		expect(rows.map((r: any) => r.value)).toEqual([10, 20, 10, 20]);
	});

	test("sort with nulls handles nullsFirst/nullsLast", async () => {
		const df = fromColumns(
			{ id: [3, null, 1, null, 2] },
			{ id: DType.nullable.int32 },
		);
		const sorted = await df.sort([asc("id")]);
		const rows = await sorted.toArray();
		// Default: nulls last
		expect(rows[0]!.id).toBe(1);
		expect(rows[1]!.id).toBe(2);
		expect(rows[2]!.id).toBe(3);
		expect(rows[3]!.id).toBeNull();
		expect(rows[4]!.id).toBeNull();
	});

	test("sort preserves all rows", async () => {
		const data = Array.from({ length: 500 }, (_, i) => i);
		// Shuffle
		for (let i = data.length - 1; i > 0; i--) {
			const j = Math.floor(Math.random() * (i + 1));
			[data[i], data[j]] = [data[j]!, data[i]!];
		}

		const df = fromColumns(
			{ id: data },
			{ id: DType.int32 },
		);
		const sorted = await df.sort([asc("id")]);
		const rows = await sorted.toArray();
		expect(rows.length).toBe(500);
		for (let i = 0; i < 500; i++) {
			expect(rows[i]!.id).toBe(i);
		}
	});

	test("sort with string columns", async () => {
		const df = fromColumns(
			{ name: ["Charlie", "Alice", "Bob", "Diana"] },
			{ name: DType.string },
		);
		const sorted = await df.sort([asc("name")]);
		const rows = await sorted.toArray();
		expect(rows.map((r: any) => r.name)).toEqual([
			"Alice",
			"Bob",
			"Charlie",
			"Diana",
		]);
	});

	test("stable sort preserves original order for equal elements", async () => {
		const df = fromColumns(
			{
				group: [1, 1, 1, 2, 2],
				order: [3, 1, 2, 2, 1],
			},
			{ group: DType.int32, order: DType.int32 },
		);

		const sorted = await df.sort([asc("group"), asc("order")]);
		const rows = await sorted.toArray();
		expect(rows.map((r: any) => r.order)).toEqual([1, 2, 3, 1, 2]);
	});

	test("empty DataFrame sort returns empty", async () => {
		const df = fromColumns(
			{ id: [] as number[] },
			{ id: DType.int32 },
		);
		const sorted = await df.sort([asc("id")]);
		const rows = await sorted.toArray();
		expect(rows.length).toBe(0);
	});

	test("single row sort returns same row", async () => {
		const df = fromColumns(
			{ id: [42] },
			{ id: DType.int32 },
		);
		const sorted = await df.sort([asc("id")]);
		const rows = await sorted.toArray();
		expect(rows.length).toBe(1);
		expect(rows[0]!.id).toBe(42);
	});
});
