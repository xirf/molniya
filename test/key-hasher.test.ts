/**
 * Tests for KeyHasher - numeric hash table for GroupBy
 */
/** biome-ignore-all lint/style/noNonNullAssertion: ts being ts i will fix it later */

import { describe, expect, it } from "bun:test";
import { hashKey, KeyHashTable, keysEqual } from "../src/ops/key-hasher.ts";

describe("KeyHasher", () => {
	describe("hashKey", () => {
		it("should return consistent hash for same values", () => {
			const key1 = [1, 2, 3];
			const key2 = [1, 2, 3];

			expect(hashKey(key1)).toBe(hashKey(key2));
		});

		it("should return different hashes for different values", () => {
			const key1 = [1, 2, 3];
			const key2 = [1, 2, 4];

			// Hashes might collide but very unlikely for these simple values
			expect(hashKey(key1)).not.toBe(hashKey(key2));
		});

		it("should handle null values", () => {
			const key1 = [1, null, 3];
			const key2 = [1, null, 3];

			expect(hashKey(key1)).toBe(hashKey(key2));
		});

		it("should handle bigint values", () => {
			const key1 = [1n, 2n, 3n];
			const key2 = [1n, 2n, 3n];

			expect(hashKey(key1)).toBe(hashKey(key2));
		});

		it("should handle mixed types", () => {
			const key1 = [1, 2n, null, 3.5];
			const key2 = [1, 2n, null, 3.5];

			expect(hashKey(key1)).toBe(hashKey(key2));
		});

		it("should return 0 for empty key", () => {
			expect(hashKey([])).toBe(0);
		});
	});

	describe("keysEqual", () => {
		it("should return true for identical keys", () => {
			const key1 = [1, 2, 3];
			const key2 = [1, 2, 3];

			expect(keysEqual(key1, key2)).toBe(true);
		});

		it("should return false for different keys", () => {
			const key1 = [1, 2, 3];
			const key2 = [1, 2, 4];

			expect(keysEqual(key1, key2)).toBe(false);
		});

		it("should return false for different lengths", () => {
			const key1 = [1, 2];
			const key2 = [1, 2, 3];

			expect(keysEqual(key1, key2)).toBe(false);
		});

		it("should handle null values correctly", () => {
			const key1 = [1, null, 3];
			const key2 = [1, null, 3];
			const key3 = [1, 0, 3];

			expect(keysEqual(key1, key2)).toBe(true);
			expect(keysEqual(key1, key3)).toBe(false);
		});

		it("should handle bigint values correctly", () => {
			const key1 = [1n, 2n];
			const key2 = [1n, 2n];
			const key3 = [1n, 3n];

			expect(keysEqual(key1, key2)).toBe(true);
			expect(keysEqual(key1, key3)).toBe(false);
		});
	});

	describe("KeyHashTable", () => {
		// Helper to adapt old tests to new API
		class TestKeyTable {
			table = new KeyHashTable();
			keys: any[][] = [];

			insert(key: any[], id: number) {
				const hash = hashKey(key);
				this.keys[id] = key;
				return this.table.getOrInsert(
					hash,
					(gid) => keysEqual(key, this.keys[gid]!),
					true,
					id
				);
			}

			get(key: any[]) {
				const hash = hashKey(key);
				return this.table.getOrInsert(
					hash,
					(gid) => keysEqual(key, this.keys[gid]!),
					false,
					-1
				);
			}

			getSize() { return this.table.getSize(); }
			clear() { this.table.clear(); this.keys = []; }
		}

		it("should insert and retrieve keys", () => {
			const table = new TestKeyTable();

			const key1 = [1, 2];
			const key2 = [3, 4];

			const id1 = table.insert(key1, 0);
			const id2 = table.insert(key2, 1);

			expect(id1).toBe(0);
			expect(id2).toBe(1);

			expect(table.get(key1)).toBe(0);
			expect(table.get(key2)).toBe(1);
		});

		it("should return existing group ID for duplicate keys", () => {
			const table = new TestKeyTable();

			const key = [1, 2];

			const id1 = table.insert(key, 0);
			const id2 = table.insert(key, 1); // Try to insert with different ID

			expect(id1).toBe(0);
			expect(id2).toBe(0); // Should return existing ID
		});

		it("should return -1 for non-existent keys", () => {
			const table = new TestKeyTable();

			const key = [1, 2];

			expect(table.get(key)).toBe(-1);
		});

		it("should handle many insertions and resizing", () => {
			const table = new TestKeyTable(); // Small initial capacity triggered dynamically

			// Insert many keys to trigger resize
			for (let i = 0; i < 100; i++) {
				const key = [i, i * 2];
				const id = table.insert(key, i);
				expect(id).toBe(i);
			}

			// Verify all keys are retrievable
			for (let i = 0; i < 100; i++) {
				const key = [i, i * 2];
				expect(table.get(key)).toBe(i);
			}

			expect(table.getSize()).toBe(100);
		});

		it("should handle null values in keys", () => {
			const table = new TestKeyTable();

			const key1 = [1, null];
			const key2 = [1, 2];

			const id1 = table.insert(key1, 0);
			const id2 = table.insert(key2, 1);

			expect(id1).toBe(0);
			expect(id2).toBe(1);

			expect(table.get(key1)).toBe(0);
			expect(table.get(key2)).toBe(1);
		});

		// "should return all entries sorted by groupId" test removed since entries() is gone.

		it("should clear all entries", () => {
			const table = new TestKeyTable();

			table.insert([1], 0);
			table.insert([2], 1);

			expect(table.getSize()).toBe(2);

			table.clear();

			expect(table.getSize()).toBe(0);
			expect(table.get([1])).toBe(-1);
			expect(table.get([2])).toBe(-1);
		});

		it("should handle collision scenarios", () => {
			const table = new TestKeyTable();

			// Insert keys that might collide
			const keys: number[][] = [];
			for (let i = 0; i < 50; i++) {
				keys.push([i, i * 31]);
			}

			for (let i = 0; i < keys.length; i++) {
				table.insert(keys[i]!, i);
			}

			// All should be retrievable
			for (let i = 0; i < keys.length; i++) {
				expect(table.get(keys[i]!)).toBe(i);
			}
		});
	});
});
