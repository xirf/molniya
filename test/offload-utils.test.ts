import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { writeToMbf, isCacheValid, ensureCacheDir } from "../src/io/offload-utils.ts";
import { DataFrame, fromRecords } from "../src/dataframe/index.ts";
import { DType } from "../src/types/dtypes.ts";
import { readMbf } from "../src/io/index.ts";

describe("OffloadUtils", () => {
	const TEST_DIR = "./test-offload-utils";
	const SOURCE_FILE = path.join(TEST_DIR, "source.csv");
	const CACHE_FILE = path.join(TEST_DIR, "cache.mbf");

	beforeEach(async () => {
		// Clean up test directory
		try {
			await fs.rm(TEST_DIR, { recursive: true });
		} catch {}
		await fs.mkdir(TEST_DIR, { recursive: true });
	});

	afterEach(async () => {
		// Clean up after tests
		try {
			await fs.rm(TEST_DIR, { recursive: true });
		} catch {}
	});

	describe("ensureCacheDir", () => {
		test("should create directory if it doesn't exist", async () => {
			const dir = path.join(TEST_DIR, "new-cache");
			await ensureCacheDir(dir);

			const stats = await fs.stat(dir);
			expect(stats.isDirectory()).toBe(true);
		});

		test("should not throw if directory already exists", async () => {
			const dir = path.join(TEST_DIR, "existing");
			await fs.mkdir(dir, { recursive: true });

			// Should not throw
			await ensureCacheDir(dir);
			const stats = await fs.stat(dir);
			expect(stats.isDirectory()).toBe(true);
		});
	});

	describe("writeToMbf", () => {
		test("should write DataFrame to MBF file", async () => {
			// Create a simple DataFrame
			const df = fromRecords(
				[
					{ id: 1, name: "Alice", score: 10 },
					{ id: 2, name: "Bob", score: 20 },
				],
				{
					id: DType.int32,
					name: DType.string,
					score: DType.int32,
				},
			);

			// Write to MBF
			await writeToMbf(df, CACHE_FILE);

			// Verify file exists and has content
			const stats = await fs.stat(CACHE_FILE);
			expect(stats.size).toBeGreaterThan(0);
			expect(stats.isFile()).toBe(true);
		});

		test("should create parent directory if needed", async () => {
			const nestedPath = path.join(TEST_DIR, "nested", "deep", "cache.mbf");

			const df = fromRecords(
				[{ id: 1 }],
				{ id: DType.int32 },
			);

			await writeToMbf(df, nestedPath);

			const stats = await fs.stat(nestedPath);
			expect(stats.size).toBeGreaterThan(0);
		});
	});

	describe("isCacheValid", () => {
		test("should return false if cache doesn't exist", async () => {
			await fs.writeFile(SOURCE_FILE, "test data");
			
			const valid = await isCacheValid(CACHE_FILE, SOURCE_FILE);
			expect(valid).toBe(false);
		});

		test("should return true if cache is newer than source", async () => {
			// Create source file
			await fs.writeFile(SOURCE_FILE, "source data");
			await new Promise(resolve => setTimeout(resolve, 10));

			// Create cache file (newer)
			await fs.writeFile(CACHE_FILE, "cache data");

			const valid = await isCacheValid(CACHE_FILE, SOURCE_FILE);
			expect(valid).toBe(true);
		});

		test("should return false if source is newer than cache", async () => {
			// Create cache file
			await fs.writeFile(CACHE_FILE, "cache data");
			await new Promise(resolve => setTimeout(resolve, 10));

			// Create source file (newer)
			await fs.writeFile(SOURCE_FILE, "source data");

			const valid = await isCacheValid(CACHE_FILE, SOURCE_FILE);
			expect(valid).toBe(false);
		});

		test("should return false if source doesn't exist", async () => {
			await fs.writeFile(CACHE_FILE, "cache data");

			const valid = await isCacheValid(CACHE_FILE, SOURCE_FILE);
			expect(valid).toBe(false);
		});
	});
});
