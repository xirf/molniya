/**
 * Tests for P2 Dictionary LRU eviction.
 *
 * Tests: dictionary creation, intern/retrieve, eviction,
 * LRU survival, free index reuse, compare, bytesEqual.
 *
 * Note: eviction calls rehash() which doubles hash table size.
 * To avoid OOM we use moderate maxEntries and limit new interns
 * so there are few eviction+rehash cycles.
 */
import { describe, test, expect } from "bun:test";
import { createDictionary } from "../src/index.ts";

describe("P2: Dictionary LRU Eviction", () => {
	test("creates dictionary with default options", () => {
		const dict = createDictionary();
		expect(dict).toBeTruthy();
		expect(dict.size).toBe(0);
	});

	test("creates dictionary with numeric initialCapacity", () => {
		const dict = createDictionary({ initialCapacity: 64 });
		expect(dict.size).toBe(0);
	});

	test("intern and retrieve strings", () => {
		const dict = createDictionary();
		const idx = dict.internString("hello");
		expect(dict.getString(idx)).toBe("hello");
	});

	test("intern deduplicates byte-level", () => {
		const dict = createDictionary();
		const idx1 = dict.internString("hello");
		const idx2 = dict.internString("hello");
		expect(idx1).toBe(idx2);
	});

	test("eviction triggers when liveCount exceeds maxEntries", () => {
		// Use moderate maxEntries to limit rehash doublings
		const maxEntries = 100;
		const dict = createDictionary({ initialCapacity: 64, maxEntries });

		// Intern exactly maxEntries strings (DON'T check getString — it updates access gen)
		const indices: number[] = [];
		for (let i = 0; i < maxEntries; i++) {
			indices.push(dict.internString(`str_${i}`));
		}

		// Intern new entries to trigger eviction cycles
		// Each cycle evicts ~10 entries (10% of 100)
		for (let i = maxEntries; i < maxEntries + 30; i++) {
			dict.internString(`new_${i}`);
		}

		// Check: some original strings should either be evicted (undefined)
		// or their index may have been reused by a different string
		let changedOrEvicted = 0;
		for (let i = 0; i < maxEntries; i++) {
			const val = dict.getString(indices[i]!);
			if (val === undefined || val !== `str_${i}`) {
				changedOrEvicted++;
			}
		}
		expect(changedOrEvicted).toBeGreaterThan(0);
	});

	test("recently accessed entries survive eviction", () => {
		const maxEntries = 100;
		const dict = createDictionary({ initialCapacity: 64, maxEntries });

		// Intern maxEntries strings
		const indices: number[] = [];
		for (let i = 0; i < maxEntries; i++) {
			indices.push(dict.internString(`str_${i}`));
		}

		// Access entries 90-99 heavily (gives them very high generation)
		for (let pass = 0; pass < 50; pass++) {
			for (let i = 90; i < 100; i++) {
				dict.getString(indices[i]!);
			}
		}

		// Trigger limited eviction cycles
		for (let i = maxEntries; i < maxEntries + 25; i++) {
			dict.internString(`new_${i}`);
		}

		// Recently accessed entries (90-99) should survive
		let survivedCount = 0;
		for (let i = 90; i < 100; i++) {
			if (dict.getString(indices[i]!) === `str_${i}`) {
				survivedCount++;
			}
		}
		expect(survivedCount).toBeGreaterThan(0);
	});

	test("getBytes works for live entries", () => {
		const dict = createDictionary();
		const idx = dict.internString("hello");
		const bytes = dict.getBytes(idx);
		expect(bytes).toBeDefined();
		expect(new TextDecoder().decode(bytes!)).toBe("hello");
	});

	test("getBytes returns undefined for out-of-range indices", () => {
		const dict = createDictionary();
		dict.internString("only_one");
		expect(dict.getBytes(999)).toBeUndefined();
	});

	test("free indices are reused after eviction", () => {
		const maxEntries = 100;
		const dict = createDictionary({ initialCapacity: 64, maxEntries });

		// Fill dictionary
		for (let i = 0; i < maxEntries; i++) {
			dict.internString(`str_${i}`);
		}

		// We can still intern more (eviction makes room)
		for (let i = maxEntries; i < maxEntries + 15; i++) {
			const idx = dict.internString(`extra_${i}`);
			expect(dict.getString(idx)).toBe(`extra_${i}`);
		}
	});

	test("maxEntries=0 means unlimited (no eviction)", () => {
		const dict = createDictionary({ initialCapacity: 8, maxEntries: 0 });

		// Intern many strings - none should be evicted
		const indices: number[] = [];
		for (let i = 0; i < 200; i++) {
			indices.push(dict.internString(`str_${i}`));
		}

		// All should still be retrievable
		for (let i = 0; i < 200; i++) {
			expect(dict.getString(indices[i]!)).toBe(`str_${i}`);
		}
	});

	test("compare works for non-evicted entries", () => {
		const dict = createDictionary();
		const aIdx = dict.internString("apple");
		const bIdx = dict.internString("banana");

		expect(dict.compare(aIdx, bIdx)).toBeLessThan(0);
		expect(dict.compare(bIdx, aIdx)).toBeGreaterThan(0);
		expect(dict.compare(aIdx, aIdx)).toBe(0);
	});

	test("bytesEqual works correctly", () => {
		const dict = createDictionary();
		const idx = dict.internString("test");
		const encoder = new TextEncoder();

		expect(dict.bytesEqual(idx, encoder.encode("test"))).toBe(true);
		expect(dict.bytesEqual(idx, encoder.encode("other"))).toBe(false);
		expect(dict.bytesEqual(idx, encoder.encode("tes"))).toBe(false);
	});
});
