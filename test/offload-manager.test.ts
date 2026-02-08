import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { OffloadManager } from "../src/io/offload-manager.ts";

describe("OffloadManager", () => {
	const TEST_CACHE_DIR = "./test-cache";

	beforeEach(async () => {
		// Clean up test cache directory
		try {
			await fs.rm(TEST_CACHE_DIR, { recursive: true });
		} catch {}
	});

	afterEach(async () => {
		// Clean up after tests
		try {
			await fs.rm(TEST_CACHE_DIR, { recursive: true });
		} catch {}
		OffloadManager.clearTempFiles();
	});

	describe("registerTempFile", () => {
		test("should track temp file for cleanup", () => {
			const filePath = "./cache/temp.mbf";
			OffloadManager.registerTempFile(filePath);
			
			const tempFiles = OffloadManager.getTempFiles();
			expect(tempFiles.has(filePath)).toBe(true);
		});

		test("should not duplicate entries", () => {
			const filePath = "./cache/temp.mbf";
			OffloadManager.registerTempFile(filePath);
			OffloadManager.registerTempFile(filePath);
			
			const tempFiles = OffloadManager.getTempFiles();
			expect(tempFiles.size).toBe(1);
		});
	});

	describe("generateCacheKey", () => {
		test("should generate consistent hash for same file", async () => {
			const testFile = path.join(TEST_CACHE_DIR, "test.txt");
			await fs.mkdir(TEST_CACHE_DIR, { recursive: true });
			await fs.writeFile(testFile, "test content");

			const key1 = await OffloadManager.generateCacheKey(testFile);
			const key2 = await OffloadManager.generateCacheKey(testFile);

			expect(key1).toBe(key2);
			expect(key1).toMatch(/^[a-f0-9]{16}$/); // 16 char hex hash
		});

		test("should generate different hash for modified file", async () => {
			const testFile = path.join(TEST_CACHE_DIR, "test.txt");
			await fs.mkdir(TEST_CACHE_DIR, { recursive: true });
			await fs.writeFile(testFile, "content1");

			const key1 = await OffloadManager.generateCacheKey(testFile);

			// Modify file
			await new Promise(resolve => setTimeout(resolve, 10));
			await fs.writeFile(testFile, "content2");

			const key2 = await OffloadManager.generateCacheKey(testFile);

			expect(key1).not.toBe(key2);
		});
	});

	describe("getCachePath", () => {
		test("should return path in cache directory", () => {
			const cacheKey = "abc123def456";
			const cachePath = OffloadManager.getCachePath(cacheKey);

			expect(cachePath).toContain("cache");
			expect(cachePath).toContain(cacheKey);
			expect(cachePath).toMatch(/\.mbf$/);
		});
	});

	describe("cleanup", () => {
		test("should delete all registered temp files", async () => {
			const file1 = path.join(TEST_CACHE_DIR, "temp1.mbf");
			const file2 = path.join(TEST_CACHE_DIR, "temp2.mbf");

			await fs.mkdir(TEST_CACHE_DIR, { recursive: true });
			await fs.writeFile(file1, "data1");
			await fs.writeFile(file2, "data2");

			OffloadManager.registerTempFile(file1);
			OffloadManager.registerTempFile(file2);

			await OffloadManager.cleanup();

			// Files should be deleted
			let file1Exists = true;
			let file2Exists = true;
			
			try {
				await fs.access(file1);
			} catch {
				file1Exists = false;
			}
			
			try {
				await fs.access(file2);
			} catch {
				file2Exists = false;
			}

			expect(file1Exists).toBe(false);
			expect(file2Exists).toBe(false);
		});

		test("should not throw if file already deleted", async () => {
			const file = path.join(TEST_CACHE_DIR, "nonexistent.mbf");
			OffloadManager.registerTempFile(file);

			// Should not throw
			await OffloadManager.cleanup();
			expect(true).toBe(true); // Test passes if no error thrown
		});
	});

	describe("ensureCacheDir", () => {
		test("should create cache directory if it doesn't exist", async () => {
			await OffloadManager.ensureCacheDir();

			const stats = await fs.stat("./cache");
			expect(stats.isDirectory()).toBe(true);
		});

		test("should not throw if directory already exists", async () => {
			await fs.mkdir("./cache", { recursive: true });
			
			// Should not throw
			await OffloadManager.ensureCacheDir();
			const stats = await fs.stat("./cache");
			expect(stats.isDirectory()).toBe(true);
		});
	});
});
