import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { readParquet, fromRecords } from "../src/dataframe/index.ts";
import { DType } from "../src/types/dtypes.ts";
import { OffloadManager } from "../src/io/offload-manager.ts";
import { BinaryWriter } from "../src/io/mbf/writer.ts";
import { unwrap } from "../src/types/error.ts";
import { createSchema } from "../src/types/schema.ts";

describe("readParquet Offloading", () => {
	const TEST_DIR = "./test-parquet-offload";
	const TEST_PARQUET = path.join(TEST_DIR, "test.parquet");

	beforeEach(async () => {
		// Clean up test directories
		try {
			await fs.rm(TEST_DIR, { recursive: true });
		} catch {}
		try {
			await fs.rm("./cache", { recursive: true });
		} catch {}
		await fs.mkdir(TEST_DIR, { recursive: true });

		// Create test Parquet file using MBF as intermediate
		const schema = unwrap(createSchema({
			id: DType.int32,
			name: DType.string,
			value: DType.float64,
		}));

		const df = fromRecords(
			[
				{ id: 1, name: "Alice", value: 10.5 },
				{ id: 2, name: "Bob", value: 20.7 },
				{ id: 3, name: "Charlie", value: 30.3 },
			],
			{
				id: DType.int32,
				name: DType.string,
				value: DType.float64,
			},
		);

		// Write as MBF for testing (Parquet writing not implemented)
		const mbfPath = path.join(TEST_DIR, "temp.mbf");
		const writer = new BinaryWriter(mbfPath, schema);
		await writer.open();
		for await (const chunk of df.stream()) {
			await writer.writeChunk(chunk);
		}
		await writer.close();

		// Rename to .parquet for test
		await fs.rename(mbfPath, TEST_PARQUET);
	});

	afterEach(async () => {
		// Clean up after tests
		try {
			await fs.rm(TEST_DIR, { recursive: true });
		} catch {}
		try {
			await fs.rm("./cache", { recursive: true });
		} catch {}
		OffloadManager.clearTempFiles();
	});

	test("offload: true should create temp MBF cache", async () => {
		const df = await readParquet(TEST_PARQUET, {
			offload: true,
		});

		// Verify cache directory exists
		const cacheExists = await fs.access("./cache")
			.then(() => true)
			.catch(() => false);
		expect(cacheExists).toBe(true);

		// Verify can read data
		const rows = await df.toArray();
		expect(rows.length).toBe(3);
		expect(rows[0]?.id).toBe(1);
	});

	test("offloadFile should write to specified path", async () => {
		const cachePath = path.join(TEST_DIR, "parquet-cache.mbf");

		const df = await readParquet(TEST_PARQUET, {
			offloadFile: cachePath,
		});

		// Verify custom cache file exists
		const stats = await fs.stat(cachePath);
		expect(stats.isFile()).toBe(true);
		expect(stats.size).toBeGreaterThan(0);

		// Verify can read data
		const rows = await df.toArray();
		expect(rows.length).toBe(3);
	});

	test("should reuse valid cache on second read", async () => {
		const cachePath = path.join(TEST_DIR, "reuse-parquet.mbf");

		// First read: creates cache
		const df1 = await readParquet(TEST_PARQUET, {
			offloadFile: cachePath,
		});
		await df1.count();

		const firstMtime = (await fs.stat(cachePath)).mtime;

		// Wait a bit
		await new Promise(resolve => setTimeout(resolve, 10));

		// Second read: should reuse cache
		const df2 = await readParquet(TEST_PARQUET, {
			offloadFile: cachePath,
		});
		await df2.count();

		const secondMtime = (await fs.stat(cachePath)).mtime;

		// Cache file should not be modified
		expect(secondMtime.getTime()).toBe(firstMtime.getTime());
	});
});
