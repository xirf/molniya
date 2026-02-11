/**
 * Unit tests for AdaptiveStringColumn component.
 * Tests sampling, encoding selection, and conversion logic in isolation.
 */

import { describe, test, expect, beforeEach } from "bun:test";
import { 
	AdaptiveStringColumn, 
	StringEncoding, 
	createAdaptiveStringColumn 
} from "../src/buffer/adaptive-string.ts";
import { DTypeKind, DType } from "../src/types/dtypes.ts";

describe("AdaptiveStringColumn Unit Tests", () => {
	describe("Initialization", () => {
		test("should initialize with correct properties", () => {
			const column = createAdaptiveStringColumn(1000, DType.string);
			
			expect(column.length).toBe(0);
			expect(column.encoding).toBe(StringEncoding.Adaptive);
			expect(column.isSamplingComplete).toBe(false);
		});

		test("should respect forced dictionary mode", () => {
			const column = createAdaptiveStringColumn(1000, DType.stringDict);
			
			expect(column.encoding).toBe(StringEncoding.Dictionary);
			expect(column.isSamplingComplete).toBe(true); // Skip sampling
		});

		test("should respect forced view mode", () => {
			const column = createAdaptiveStringColumn(1000, DType.stringView);
			
			expect(column.encoding).toBe(StringEncoding.View);
			expect(column.isSamplingComplete).toBe(true); // Skip sampling
		});

		test("should handle nullable columns", () => {
			const nullable = createAdaptiveStringColumn(100, DType.nullable.string);
			const nonNullable = createAdaptiveStringColumn(100, DType.string);
			
			// Both should initialize successfully
			expect(nullable.length).toBe(0);
			expect(nonNullable.length).toBe(0);
		});
	});

	describe("Basic String Operations", () => {
		let column: AdaptiveStringColumn;

		beforeEach(() => {
			column = createAdaptiveStringColumn(100, DType.stringView); // Force view for predictable testing
		});

		test("should append and retrieve strings", () => {
			const result = column.append("hello");
			expect(result.error).toBe(0);
			expect(column.length).toBe(1);
			expect(column.getString(0)).toBe("hello");
		});

		test("should handle multiple appends", () => {
			const strings = ["first", "second", "third"];
			
			strings.forEach((str, i) => {
				const result = column.append(str);
				expect(result.error).toBe(0);
				expect(column.getString(i)).toBe(str);
			});
			
			expect(column.length).toBe(3);
		});

		test("should handle empty strings", () => {
			column.append("");
			expect(column.getString(0)).toBe("");
		});

		test("should handle very long strings", () => {
			const longString = "x".repeat(10000);
			column.append(longString);
			expect(column.getString(0)).toBe(longString);
		});

		test("should handle unicode strings", () => {
			const unicode = "Hello 👋 世界 🌍";
			column.append(unicode);
			expect(column.getString(0)).toBe(unicode);
		});
	});

	describe("Null Value Handling", () => {
		let nullableColumn: AdaptiveStringColumn;
		let nonNullableColumn: AdaptiveStringColumn;

		beforeEach(() => {
			nullableColumn = createAdaptiveStringColumn(100, DType.nullable.stringView);
			nonNullableColumn = createAdaptiveStringColumn(100, DType.stringView);
		});

		test("should handle null appends in nullable column", () => {
			const result = nullableColumn.appendNull();
			expect(result.error).toBe(0);
			expect(nullableColumn.length).toBe(1);
			expect(nullableColumn.isNull(0)).toBe(true);
			expect(nullableColumn.getString(0)).toBeUndefined();
		});

		test("should reject nulls in non-nullable column", () => {
			const result = nonNullableColumn.appendNull();
			expect(result.error).toBe(102); // TypeMismatch
		});

		test("should handle mixed null and non-null values", () => {
			nullableColumn.append("first");
			nullableColumn.appendNull();
			nullableColumn.append("third");
			
			expect(nullableColumn.length).toBe(3);
			expect(nullableColumn.getString(0)).toBe("first");
			expect(nullableColumn.isNull(1)).toBe(true);
			expect(nullableColumn.getString(2)).toBe("third");
		});
	});

	describe("Capacity Limits", () => {
		test("should respect capacity limits", () => {
			const smallColumn = createAdaptiveStringColumn(2, DType.stringView);
			
			expect(smallColumn.append("first").error).toBe(0);
			expect(smallColumn.append("second").error).toBe(0);
			expect(smallColumn.append("third").error).toBe(1); // CapacityExceeded
			
			expect(smallColumn.length).toBe(2);
		});
	});

	describe("Cardinality Sampling", () => {
		test("should complete sampling after sample size reached", () => {
			const column = createAdaptiveStringColumn(
				1000, 
				DType.string, 
				{ sampleSize: 5 }
			);
			
			expect(column.isSamplingComplete).toBe(false);
			
			// Add unique strings
			for (let i = 0; i < 5; i++) {
				column.append(`unique${i}`);
			}
			
			expect(column.isSamplingComplete).toBe(true);
		});

		test("should detect low cardinality correctly", () => {
			const column = createAdaptiveStringColumn(
				1000,
				DType.string,
				{ sampleSize: 100, cardinalityThreshold: 0.1 }
			);
			
			// Add low cardinality data (only 3 unique values)
			const values = ["A", "B", "C"];
			// Sampling completes when count >= sampleSize (checked before increment)
			// So we need to add sampleSize+1 items
			for (let i = 0; i <= 100; i++) {
				column.append(values[i % 3]);
			}
			
			// Sampling completes after 101 appends (when count=100, then incremented to 101)
			expect(column.isSamplingComplete).toBe(true);
			// Low cardinality (3 unique out of 101) should recommend dictionary
			const shouldUseDict = column.shouldUseDictionary();
			expect(shouldUseDict).toBe(true);
			
			const stats = column.getCardinalityStats();
			expect(stats).toBeTruthy();
			expect(stats!.cardinality).toBeLessThan(0.1);
			expect(stats!.uniqueStrings).toBe(3);
		});

		test("should detect high cardinality correctly", () => {
			const column = createAdaptiveStringColumn(
				1000,
				DType.string,
				{ sampleSize: 50, cardinalityThreshold: 0.1 }
			);
			
			// Add high cardinality data (all unique)
			for (let i = 0; i < 50; i++) {
				column.append(`unique_${i}`);
			}
			
			expect(column.isSamplingComplete).toBe(true);
			// High cardinality should not recommend dictionary (returns false or null if < MIN_STRINGS_FOR_DICT)
			const shouldUseDict = column.shouldUseDictionary();
			// May be null if count < MIN_STRINGS_FOR_DICT (100), otherwise false
			expect(shouldUseDict === false || shouldUseDict === null).toBe(true);
			
			const stats = column.getCardinalityStats();
			expect(stats!.cardinality).toBe(1.0); // 100% unique
		});

		test("should not provide stats before sampling complete", () => {
			const column = createAdaptiveStringColumn(
				1000,
				DType.string,
				{ sampleSize: 100 }
			);
			
			column.append("test");
			expect(column.getCardinalityStats()).toBeNull();
		});

		test("should handle repeated strings in sampling", () => {
			const column = createAdaptiveStringColumn(
				1000,
				DType.string,
				{ sampleSize: 20 }
			);
			
			// Add same string multiple times - need sampleSize+1 to complete
			for (let i = 0; i <= 20; i++) {
				column.append("repeated");
			}
			
			expect(column.isSamplingComplete).toBe(true);
			const stats = column.getCardinalityStats();
			expect(stats).not.toBeNull();
			expect(stats!.uniqueStrings).toBe(1);
			expect(stats!.cardinality).toBe(1 / 21); // 1/21
		});
	});

	describe("Encoding Conversion", () => {
		test("should convert from view to dictionary", () => {
			const column = createAdaptiveStringColumn(100, DType.stringView);
			
			// Add some data
			const values = ["apple", "banana", "cherry", "apple", "banana"];
			values.forEach(val => column.append(val));
			
			expect(column.encoding).toBe(StringEncoding.View);
			
			// Convert to dictionary
			const result = column.convertToDictionary();
			expect(result.error).toBe(0);
			expect(column.encoding).toBe(StringEncoding.Dictionary);
			
			// Verify data integrity
			values.forEach((val, i) => {
				expect(column.getString(i)).toBe(val);
			});
		});

		test("should handle conversion with null values", () => {
			const column = createAdaptiveStringColumn(100, DType.nullable.stringView);
			
			column.append("first");
			column.appendNull();
			column.append("third");
			
			const result = column.convertToDictionary();
			expect(result.error).toBe(0);
			
			expect(column.getString(0)).toBe("first");
			expect(column.isNull(1)).toBe(true);
			expect(column.getString(2)).toBe("third");
		});

		test("should not convert if already dictionary", () => {
			const column = createAdaptiveStringColumn(100, DType.stringDict);
			
			column.append("test");
			expect(column.encoding).toBe(StringEncoding.Dictionary);
			
			const result = column.convertToDictionary();
			expect(result.error).toBe(0); // No-op, but successful
		});

		test("should handle empty column conversion", () => {
			const column = createAdaptiveStringColumn(100, DType.stringView);
			
			const result = column.convertToDictionary();
			expect(result.error).toBe(0);
			expect(column.encoding).toBe(StringEncoding.Dictionary);
		});
	});

	describe("Internal Access", () => {
		test("should provide access to dictionary internals", () => {
			const column = createAdaptiveStringColumn(100, DType.stringDict);
			column.append("test");
			
			const internals = column.getInternals();
			expect(internals.dictionary).toBeTruthy();
			expect(internals.indices).toBeTruthy();
			expect(internals.stringView).toBeUndefined();
		});

		test("should provide access to stringview internals", () => {
			const column = createAdaptiveStringColumn(100, DType.stringView);
			column.append("test");
			
			const internals = column.getInternals();
			expect(internals.stringView).toBeTruthy();
			expect(internals.dictionary).toBeUndefined();
			expect(internals.indices).toBeUndefined();
		});
	});

	describe("Edge Cases", () => {
		test("should handle extremely small capacity", () => {
			const column = createAdaptiveStringColumn(1, DType.stringView);
			
			expect(column.append("test").error).toBe(0);
			expect(column.append("overflow").error).toBe(1); // CapacityExceeded
		});

		test("should handle zero-length sampling", () => {
			const column = createAdaptiveStringColumn(
				100,
				DType.string,
				{ sampleSize: 0 }
			);
			
			// Sampling completes after first append when sampleSize=0
			expect(column.isSamplingComplete).toBe(false);
			column.append("test");
			expect(column.isSamplingComplete).toBe(true);
		});

		test("should handle very small cardinality threshold", () => {
			const column = createAdaptiveStringColumn(
				100,
				DType.string,
				{ sampleSize: 10, cardinalityThreshold: 0.01 }
			);
			
			// Even with 2 unique values out of 11, should prefer StringView
			for (let i = 0; i <= 10; i++) {
				column.append(i < 5 ? "A" : "B");
			}
			
			expect(column.isSamplingComplete).toBe(true);
			const stats = column.getCardinalityStats();
			expect(stats).not.toBeNull();
			expect(stats!.cardinality).toBeCloseTo(2 / 11, 2); // ~0.18
			// Since cardinality (~0.18) > threshold (0.01), should not use dictionary
			const shouldUseDict = column.shouldUseDictionary();
			// May be null if < MIN_STRINGS_FOR_DICT, otherwise false
			expect(shouldUseDict === false || shouldUseDict === null).toBe(true);
		});

		test("should handle custom sample size larger than data", () => {
			const column = createAdaptiveStringColumn(
				100,
				DType.string,
				{ sampleSize: 1000 } // Larger than we'll add
			);
			
			for (let i = 0; i < 50; i++) {
				column.append(`string${i}`);
			}
			
			// Won't complete because unique strings (50) < sampleSize (1000) and count (50) < sampleSize (1000)
			// Actually, sampling completes when sampleSet.size >= sampleSize OR count >= sampleSize
			// Since count = 50 < 1000, sampling should NOT be complete
			expect(column.isSamplingComplete).toBe(false);
		});
	});
});