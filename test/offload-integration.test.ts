import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { DataFrame, readCsv } from "../src/dataframe/index.ts";
import { DType } from "../src/types/dtypes.ts";
import { OffloadManager } from "../src/io/offload-manager.ts";

describe("DataFrame Offloading Integration", () => {
	const TEST_DIR = "./test-offload-integration";
	const TEST_CSV = path.join(TEST_DIR, "test.csv");

	beforeEach(async () => {
		// Clean up test directory
		try {
			await fs.rm(TEST_DIR, { recursive: true });
		} catch {}
		try {
			await fs.rm("./cache", { recursive: true });
		} catch {}
		await fs.mkdir(TEST_DIR, { recursive: true });

		// Create test CSV
		await fs.writeFile(
			TEST_CSV,
			"id,name,score\n1,Alice,10\n2,Bob,20\n3,Charlie,30"
		);
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

	describe("readCsv with offload", () => {
		test("offload: true should create temp MBF file", async () => {
			const df = await readCsv(TEST_CSV, {
				id: DType.int32,
				name: DType.string,
				score: DType.int32,
			}, {
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
			expect(rows[1]?.id).toBe(2);
		});

		test("offloadFile should write to specified path", async () => {
			const cachePath = path.join(TEST_DIR, "custom.mbf");
			
			const df = await readCsv(TEST_CSV, {
				id: DType.int32,
				name: DType.string,
				score: DType.int32,
			}, {
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
			const cachePath = path.join(TEST_DIR, "reuse.mbf");

			// First read: creates cache
			const df1 = await readCsv(TEST_CSV, {
				id: DType.int32,
				name: DType.string,
				score: DType.int32,
			}, {
				offloadFile: cachePath,
			});
			await df1.count(); // Force execution

			const firstMtime = (await fs.stat(cachePath)).mtime;

			// Wait a bit
			await new Promise(resolve => setTimeout(resolve, 10));

			// Second read: should reuse cache (not recreate)
			const df2 = await readCsv(TEST_CSV, {
				id: DType.int32,
				name: DType.string,
				score: DType.int32,
			}, {
				offloadFile: cachePath,
			});
			await df2.count();

			const secondMtime = (await fs.stat(cachePath)).mtime;

			// Cache file should not be modified
			expect(secondMtime.getTime()).toBe(firstMtime.getTime());
		});

		test("should invalidate cache if source changes", async () => {
			const cachePath = path.join(TEST_DIR, "invalidate.mbf");

			// First read
			await readCsv(TEST_CSV, {
				id: DType.int32,
				name: DType.string,
				score: DType.int32,
			}, {
				offloadFile: cachePath,
			});

			const firstSize = (await fs.stat(cachePath)).size;

			// Modify source file
			await new Promise(resolve => setTimeout(resolve, 10));
			await fs.writeFile(
				TEST_CSV,
				"id,name,score\n1,Alice,10\n2,Bob,20\n3,Charlie,30\n4,David,40"
			);

			// Second read: should recreate cache
			await readCsv(TEST_CSV, {
				id: DType.int32,
				name: DType.string,
				score: DType.int32,
			}, {
				offloadFile: cachePath,
			});

			const secondSize = (await fs.stat(cachePath)).size;

			// Cache should be different (more data)
			expect(secondSize).toBeGreaterThan(firstSize);
		});
	});
});
