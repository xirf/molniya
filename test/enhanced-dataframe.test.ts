import { describe, expect, it } from "bun:test";
import { DataFrame, fromRecords, fromColumns } from "../src/dataframe/dataframe.ts";
import { DType } from "../src/types/dtypes.ts";
import { unwrap } from "../src/types/error.ts";
import { col } from "../src/expr/builders.ts";

describe("DataFrame with EnhancedColumnBuffer Integration", () => {
	describe("fromRecords with adaptive string encoding", () => {
		it("should use dictionary encoding for low-cardinality strings", async () => {
			const records = [
				{ id: 1, status: "active" },
				{ id: 2, status: "inactive" },
				{ id: 3, status: "active" },
				{ id: 4, status: "pending" },
				{ id: 5, status: "active" },
			];

			const df = fromRecords(records, {
				id: DType.int32,
				status: DType.string, // Should auto-detect and use dictionary
			});

			const result = await df.toArray();
			expect(result.length).toBe(5);
			expect(result[0]?.status).toBe("active");
			expect(result[1]?.status).toBe("inactive");
		});

		it("should handle high-cardinality strings with StringView", async () => {
			// Generate unique strings (high cardinality)
			const records = Array.from({ length: 100 }, (_, i) => ({
				id: i,
				uniqueId: `unique_string_${i}_with_some_extra_content_${Math.random()}`,
			}));

			const df = fromRecords(records, {
				id: DType.int32,
				uniqueId: DType.string, // Should detect high cardinality
			});

			const result = await df.toArray();
			expect(result.length).toBe(100);
			expect(result[0]?.uniqueId).toContain("unique_string_0");
			expect(result[99]?.uniqueId).toContain("unique_string_99");
		});

		it("should respect explicit DType.stringDict hint", async () => {
			const records = Array.from({ length: 100 }, (_, i) => ({
				id: i,
				value: `unique_${i}`,
			}));

			const df = fromRecords(records, {
				id: DType.int32,
				value: DType.stringDict, // Force dictionary encoding
			});

			const result = await df.toArray();
			expect(result.length).toBe(100);
			expect(result[0]?.value).toBe("unique_0");
		});

		it("should respect explicit DType.stringView hint", async () => {
			const records = [
				{ id: 1, status: "active" },
				{ id: 2, status: "active" },
				{ id: 3, status: "active" },
			];

			const df = fromRecords(records, {
				id: DType.int32,
				status: DType.stringView, // Force StringView even for low cardinality
			});

			const result = await df.toArray();
			expect(result.length).toBe(3);
			expect(result[0]?.status).toBe("active");
		});

		it("should handle null values in string columns", async () => {
			const records = [
				{ id: 1, name: "Alice" },
				{ id: 2, name: null },
				{ id: 3, name: "Bob" },
				{ id: 4, name: undefined },
			];

			const df = fromRecords(records, {
				id: DType.int32,
				name: DType.nullable.string,
			});

			const result = await df.toArray();
			expect(result.length).toBe(4);
			expect(result[0]?.name).toBe("Alice");
			expect(result[1]?.name).toBeNull();
			expect(result[2]?.name).toBe("Bob");
			expect(result[3]?.name).toBeNull();
		});

		it("should handle mixed-length strings efficiently", async () => {
			const records = [
				{ id: 1, text: "a" },
				{ id: 2, text: "a very long string that takes up more space" },
				{ id: 3, text: "short" },
				{ id: 4, text: "another extremely long string with lots of content to test memory efficiency" },
			];

			const df = fromRecords(records, {
				id: DType.int32,
				text: DType.string,
			});

			const result = await df.toArray();
			expect(result.length).toBe(4);
			expect(result[0]?.text).toBe("a");
			expect(result[1]?.text).toContain("very long string");
		});
	});

	describe("fromColumns with adaptive string encoding", () => {
		it("should use dictionary encoding for low-cardinality string arrays", async () => {
			const df = fromColumns(
				{
					id: [1, 2, 3, 4, 5],
					category: ["A", "B", "A", "C", "B"],
				},
				{
					id: DType.int32,
					category: DType.string,
				},
			);

			const result = await df.toArray();
			expect(result.length).toBe(5);
			expect(result[0]?.category).toBe("A");
			expect(result[1]?.category).toBe("B");
		});

		it("should handle high-cardinality string arrays", async () => {
			const uniqueStrings = Array.from(
				{ length: 100 },
				(_, i) => `unique_value_${i}_${Math.random()}`,
			);

			const df = fromColumns(
				{
					id: Array.from({ length: 100 }, (_, i) => i),
					value: uniqueStrings,
				},
				{
					id: DType.int32,
					value: DType.string,
				},
			);

			const result = await df.toArray();
			expect(result.length).toBe(100);
			expect(result[0]?.value).toBe(uniqueStrings[0]);
			expect(result[99]?.value).toBe(uniqueStrings[99]);
		});

		it("should respect explicit encoding hints in fromColumns", async () => {
			const df = fromColumns(
				{
					id: [1, 2, 3],
					status: ["active", "active", "active"],
				},
				{
					id: DType.int32,
					status: DType.stringView, // Force StringView
				},
			);

			const result = await df.toArray();
			expect(result.length).toBe(3);
			expect(result[0]?.status).toBe("active");
		});

		it("should handle null values in fromColumns", async () => {
			const df = fromColumns(
				{
					id: [1, 2, 3, 4],
					name: ["Alice", null, "Bob", undefined],
				},
				{
					id: DType.int32,
					name: DType.nullable.string,
				},
			);

			const result = await df.toArray();
			expect(result.length).toBe(4);
			expect(result[0]?.name).toBe("Alice");
			expect(result[1]?.name).toBeNull();
			expect(result[2]?.name).toBe("Bob");
			expect(result[3]?.name).toBeNull();
		});

		it("should handle TypedArrays for numeric columns", async () => {
			const df = fromColumns(
				{
					values: new Float64Array([1.1, 2.2, 3.3, 4.4]),
					labels: ["a", "b", "c", "d"],
				},
				{
					values: DType.float64,
					labels: DType.string,
				},
			);

			const result = await df.toArray();
			expect(result.length).toBe(4);
			expect(result[0]?.values).toBeCloseTo(1.1);
			expect(result[0]?.labels).toBe("a");
		});
	});

	describe("String operations on enhanced columns", () => {
		it("should support filtering on string columns", async () => {
			const records = [
				{ id: 1, status: "active" },
				{ id: 2, status: "inactive" },
				{ id: 3, status: "active" },
			];

			const df = fromRecords(records, {
				id: DType.int32,
				status: DType.string,
			});

			const filtered = df.filter(col("status").eq("active"));
			const result = await filtered.toArray();

			expect(result.length).toBe(2);
			expect(result[0]?.status).toBe("active");
			expect(result[1]?.status).toBe("active");
		});

		it("should support selecting string columns", async () => {
			const records = [
				{ id: 1, name: "Alice", age: 25 },
				{ id: 2, name: "Bob", age: 30 },
			];

			const df = fromRecords(records, {
				id: DType.int32,
				name: DType.string,
				age: DType.int32,
			});

			const selected = await df.select("name").toArray();

			expect(selected.length).toBe(2);
			expect(selected[0]?.name).toBe("Alice");
			expect(selected[1]?.name).toBe("Bob");
		});
	});

	describe("Memory efficiency", () => {
		it("should handle large datasets with low-cardinality strings efficiently", async () => {
			const categories = ["A", "B", "C", "D", "E"];
			const records = Array.from({ length: 10000 }, (_, i) => ({
				id: i,
				category: categories[i % categories.length]!,
			}));

			const df = fromRecords(records, {
				id: DType.int32,
				category: DType.string,
			});

			const result = await df.limit(10).toArray();
			expect(result.length).toBe(10);
		});

		it("should handle large datasets with high-cardinality strings", async () => {
			const records = Array.from({ length: 1000 }, (_, i) => ({
				id: i,
				uuid: `${Math.random()}-${Math.random()}-${i}`,
			}));

			const df = fromRecords(records, {
				id: DType.int32,
				uuid: DType.string,
			});

			const result = await df.limit(10).toArray();
			expect(result.length).toBe(10);
			expect(result[0]?.uuid).toBeDefined();
		});
	});

	describe("Edge cases", () => {
		it("should handle empty DataFrames", async () => {
			const df = fromRecords([], {
				id: DType.int32,
				name: DType.string,
			});

			const result = await df.toArray();
			expect(result.length).toBe(0);
		});

		it("should handle single-row DataFrames", async () => {
			const df = fromRecords(
				[{ id: 1, name: "Alice" }],
				{
					id: DType.int32,
					name: DType.string,
				},
			);

			const result = await df.toArray();
			expect(result.length).toBe(1);
			expect(result[0]?.name).toBe("Alice");
		});

		it("should handle empty strings", async () => {
			const df = fromRecords(
				[
					{ id: 1, text: "" },
					{ id: 2, text: "non-empty" },
					{ id: 3, text: "" },
				],
				{
					id: DType.int32,
					text: DType.string,
				},
			);

			const result = await df.toArray();
			expect(result.length).toBe(3);
			expect(result[0]?.text).toBe("");
			expect(result[1]?.text).toBe("non-empty");
		});

		it("should handle unicode strings", async () => {
			const df = fromRecords(
				[
					{ id: 1, text: "Hello 世界" },
					{ id: 2, text: "🎉 Emoji" },
					{ id: 3, text: "Ñoño" },
				],
				{
					id: DType.int32,
					text: DType.string,
				},
			);

			const result = await df.toArray();
			expect(result.length).toBe(3);
			expect(result[0]?.text).toBe("Hello 世界");
			expect(result[1]?.text).toBe("🎉 Emoji");
			expect(result[2]?.text).toBe("Ñoño");
		});
	});
});
