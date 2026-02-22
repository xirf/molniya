/**
 * Tests for P3 adaptive chunk sizing.
 *
 * Tests: computeChunkSize behaviour for narrow vs wide schemas,
 * custom options, nullable overhead, power-of-2 rounding, edge cases.
 */
import { describe, test, expect } from "bun:test";
import { computeChunkSize, createSchema, DType } from "../src/index.ts";
import type { ChunkSizingOptions } from "../src/index.ts";

/** Helper to build a Schema from a spec (unwraps Result). */
function makeSchema(spec: Record<string, typeof DType[keyof typeof DType]>) {
	const result = createSchema(spec);
	if (result.error !== 0) throw new Error(`Schema creation failed: ${result.error}`);
	return result.value;
}

describe("P3: Adaptive Chunk Sizing", () => {
	test("narrow schema yields large chunk size", () => {
		// 3 x Float64 = 24 bytes/row + 4 selection = 28 bytes/row
		// 1MB / 28 ≈ 37449 → floorPow2 → 32768
		const schema = makeSchema({ a: DType.float64, b: DType.float64, c: DType.float64 });
		const size = computeChunkSize(schema);
		expect(size).toBe(32768);
	});

	test("wide schema yields small chunk size", () => {
		// 50 x Float64 = 400 bytes/row + 4 selection = 404 bytes/row
		// 1MB / 404 ≈ 2597 → floorPow2 → 2048
		const spec: Record<string, typeof DType.float64> = {};
		for (let i = 0; i < 50; i++) spec[`c${i}`] = DType.float64;
		const schema = makeSchema(spec);
		const size = computeChunkSize(schema);
		expect(size).toBe(2048);
	});

	test("very wide schema clamps to minChunkSize", () => {
		// 200 x Float64 = 1600 bytes/row + 4 = 1604 bytes/row
		// 1MB / 1604 ≈ 654 → clamp to 1024 → floorPow2 → 1024
		const spec: Record<string, typeof DType.float64> = {};
		for (let i = 0; i < 200; i++) spec[`c${i}`] = DType.float64;
		const schema = makeSchema(spec);
		const size = computeChunkSize(schema);
		expect(size).toBe(1024);
	});

	test("single column yields max chunk clamped to power of 2", () => {
		// 1 x Int32 = 4 bytes/row + 4 selection = 8 bytes/row
		// 1MB / 8 = 131072 → clamp to max 65536 → floorPow2 → 65536
		const schema = makeSchema({ a: DType.int32 });
		const size = computeChunkSize(schema);
		expect(size).toBe(65536);
	});

	test("nullable columns add overhead", () => {
		// 3 x nullable Float64 = 24 + 3*1 null bitmap + 4 sel = 31 bytes/row
		// 1MB / 31 ≈ 33825 → floorPow2 → 32768
		const schema = makeSchema({
			a: DType.nullable.float64,
			b: DType.nullable.float64,
			c: DType.nullable.float64,
		});
		const size = computeChunkSize(schema);
		expect(size).toBe(32768);
	});

	test("result is always a power of 2", () => {
		for (const nCols of [1, 2, 5, 10, 20, 50, 100]) {
			const spec: Record<string, typeof DType.float64> = {};
			for (let i = 0; i < nCols; i++) spec[`c${i}`] = DType.float64;
			const schema = makeSchema(spec);
			const size = computeChunkSize(schema);
			// Check power of 2: n & (n-1) === 0
			expect(size & (size - 1)).toBe(0);
			expect(size).toBeGreaterThanOrEqual(1024);
			expect(size).toBeLessThanOrEqual(65536);
		}
	});

	test("custom targetChunkBytes", () => {
		// 3 x Float64 = 24 + 4 sel = 28 bytes/row
		// With 256KB target: 262144 / 28 ≈ 9362 → floorPow2 → 8192
		const schema = makeSchema({ a: DType.float64, b: DType.float64, c: DType.float64 });
		const opts: ChunkSizingOptions = { targetChunkBytes: 256 * 1024 };
		const size = computeChunkSize(schema, opts);
		expect(size).toBe(8192);
	});

	test("custom minChunkSize and maxChunkSize", () => {
		// Single Int32 col: 4 + 4 = 8. 1MB / 8 = 131072 → clamp to custom max 8192
		const schema = makeSchema({ a: DType.int32 });
		const opts: ChunkSizingOptions = { minChunkSize: 512, maxChunkSize: 8192 };
		const size = computeChunkSize(schema, opts);
		expect(size).toBe(8192);
	});

	test("mixed dtype schema", () => {
		// Int8(1) + Int32(4) + Float64(8) + String(4) = 17 + 4 sel = 21 bytes/row
		// 1MB / 21 ≈ 49932 → clamp to 49932 → floorPow2 → 32768
		const schema = makeSchema({
			a: DType.int8,
			b: DType.int32,
			c: DType.float64,
			d: DType.string,
		});
		const size = computeChunkSize(schema);
		expect(size).toBe(32768);
	});

	test("schema with zero columns returns minChunkSize", () => {
		// Edge case: schema with no columns
		// We need to construct a Schema object directly since createSchema rejects empty
		const emptySchema = {
			columns: [] as any[],
			columnMap: new Map(),
			rowSize: 0,
			columnCount: 0,
		};
		const size = computeChunkSize(emptySchema as any);
		expect(size).toBe(1024);
	});

	test("boolean columns are 1 byte each", () => {
		// 10 x Boolean = 10 + 4 sel = 14 bytes/row
		// 1MB / 14 ≈ 74898 → clamp to 65536 → floorPow2 → 65536
		const spec: Record<string, typeof DType.boolean> = {};
		for (let i = 0; i < 10; i++) spec[`flag${i}`] = DType.boolean;
		const schema = makeSchema(spec);
		const size = computeChunkSize(schema);
		expect(size).toBe(65536);
	});
});
