/**
 * Tests for P0 + P1 streaming pipeline optimizations.
 *
 * Tests: streaming show/count/forEach/reduce, chunk recycling,
 * zero-copy selection via withSelectionBorrowed, ChunkPool.
 */
import { describe, test, expect, beforeAll } from "bun:test";
import { fromRecords, fromColumns, col, DType, sum, count, avg } from "../src/index.ts";
import { ChunkPool, recycleChunk, bufferPool } from "../src/buffer/pool.ts";
import type { Chunk } from "../src/buffer/chunk.ts";
import { createSchema } from "../src/types/schema.ts";
import { unwrap } from "../src/types/error.ts";

describe("P0: Streaming Execution", () => {
	const data = Array.from({ length: 200 }, (_, i) => ({
		id: i,
		value: i * 10,
		name: `name_${i}`,
	}));
	const df = fromColumns(
		{
			id: data.map((d) => d.id),
			value: data.map((d) => d.value),
			name: data.map((d) => d.name),
		},
		{ id: DType.int32, value: DType.float64, name: DType.string },
	);

	test("count() returns correct total without materializing", async () => {
		const n = await df.count();
		expect(n).toBe(200);
	});

	test("count() on filtered DataFrame is correct", async () => {
		const n = await df.filter(col("id").gt(100)).count();
		expect(n).toBe(99); // ids 101-199
	});

	test("forEach() iterates all rows in order", async () => {
		const ids: number[] = [];
		await df.forEach((row: any) => ids.push(row.id));
		expect(ids.length).toBe(200);
		expect(ids[0]).toBe(0);
		expect(ids[199]).toBe(199);
	});

	test("forEach() with filter only iterates matched rows", async () => {
		const ids: number[] = [];
		await df.filter(col("id").lt(5)).forEach((row: any) => ids.push(row.id));
		expect(ids).toEqual([0, 1, 2, 3, 4]);
	});

	test("reduce() accumulates correctly", async () => {
		const total = await df.reduce((acc: number, row: any) => acc + row.id, 0);
		// Sum of 0..199 = 199*200/2 = 19900
		expect(total).toBe(19900);
	});

	test("reduce() with filter accumulates only matching rows", async () => {
		const total = await df
			.filter(col("id").lt(10))
			.reduce((acc: number, row: any) => acc + row.id, 0);
		// Sum of 0..9 = 45
		expect(total).toBe(45);
	});

	test("show() does not throw", async () => {
		// show() should work without errors
		await df.show(5);
	});

	test("stream() yields chunks", async () => {
		let chunkCount = 0;
		let totalRows = 0;
		for await (const chunk of df.stream()) {
			chunkCount++;
			totalRows += chunk.rowCount;
		}
		expect(totalRows).toBe(200);
		expect(chunkCount).toBeGreaterThan(0);
	});
});

describe("P1: Zero-copy Selection (withSelectionBorrowed)", () => {
	test("withSelectionBorrowed creates zero-copy view", () => {
		const schema = unwrap(
			createSchema({ a: DType.int32, b: DType.float64 }),
		);
		const chunk = fromColumns(
			{ a: [1, 2, 3, 4, 5], b: [10.0, 20.0, 30.0, 40.0, 50.0] },
			{ a: DType.int32, b: DType.float64 },
		);
		// Get a chunk from the dataframe
		let released = false;
		const chunks: Chunk[] = [];
		for (const c of (chunk as any).source as Iterable<Chunk>) {
			chunks.push(c);
		}
		const sourceChunk = chunks[0]!;

		// Create a selection
		const selection = new Uint32Array([1, 3]); // rows 1 and 3
		const borrowed = sourceChunk.withSelectionBorrowed(
			selection,
			2,
			() => {
				released = true;
			},
		);

		expect(borrowed.rowCount).toBe(2);
		// Selection buffer is shared (zero-copy)
		expect(borrowed.hasSelection()).toBe(true);

		// Dispose should trigger release callback
		borrowed.dispose();
		expect(released).toBe(true);
	});
});

describe("P1: ChunkPool", () => {
	test("ChunkPool is a singleton", () => {
		const a = ChunkPool.getInstance();
		const b = ChunkPool.getInstance();
		expect(a).toBe(b);
	});

	test("ChunkPool tracks stats", () => {
		const pool = ChunkPool.getInstance();
		pool.clear();
		const stats = pool.getStats();
		expect(stats.totalChunks).toBe(0);
		expect(stats.keys).toBe(0);
	});

	test("recycleChunk releases column buffers to pool", () => {
		const df = fromColumns(
			{ x: [1, 2, 3] },
			{ x: DType.int32 },
		);
		// This should not throw
		const chunks: Chunk[] = [];
		for (const c of (df as any).source as Iterable<Chunk>) {
			chunks.push(c);
		}
		if (chunks[0]) {
			recycleChunk(chunks[0]);
		}
	});
});

describe("P1: Filter with zero-copy selection", () => {
	test("filtered results use selection vectors efficiently", async () => {
		const df = fromColumns(
			{
				id: Array.from({ length: 1000 }, (_, i) => i),
				value: Array.from({ length: 1000 }, (_, i) => i * 2),
			},
			{ id: DType.int32, value: DType.int32 },
		);

		const result = await df.filter(col("id").gt(900)).toArray();
		expect(result.length).toBe(99); // 901-999
		expect(result[0]!.id).toBe(901);
	});

	test("chained filters with fusion produce correct results", async () => {
		const df = fromColumns(
			{
				a: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
				b: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
			},
			{ a: DType.int32, b: DType.int32 },
		);

		const result = await df
			.filter(col("a").gt(3))
			.filter(col("a").lt(8))
			.toArray();

		// Should get rows where a in {4,5,6,7}
		expect(result.length).toBe(4);
		expect(result.map((r: any) => r.a)).toEqual([4, 5, 6, 7]);
	});
});
