/**
 * Simple tests for Core API Gaps implementation
 */

import { describe, expect, it } from "bun:test";
import {
	col,
	countDistinct,
	DType,
	fromArrays,
	fromColumns,
	median,
	range,
	std,
	variance,
	when,
} from "../src/index.ts";

describe("String Operations", () => {
	it("should compute string length", async () => {
		const df = fromColumns(
			{ name: ["hello", "world", "hi"] },
			{ name: DType.string },
		);
		const result = await df.withColumn("len", col("name").length()).toArray();
		expect(result[0].len).toBe(5);
		expect(result[1].len).toBe(5);
		expect(result[2].len).toBe(2);
	});

	it("should extract substring", async () => {
		const df = fromColumns(
			{ text: ["hello world"] },
			{ text: DType.string },
		);
		const result = await df
			.withColumn("sub1", col("text").substring(0, 5))
			.withColumn("sub2", col("text").substring(6))
			.toArray();
		expect(result[0].sub1).toBe("hello");
		expect(result[0].sub2).toBe("world");
	});

	it("should convert to uppercase and lowercase", async () => {
		const df = fromColumns(
			{ name: ["hello", "WORLD"] },
			{ name: DType.string },
		);
		const result = await df
			.withColumn("upper", col("name").upper())
			.withColumn("lower", col("name").lower())
			.toArray();
		expect(result[0].upper).toBe("HELLO");
		expect(result[0].lower).toBe("hello");
		expect(result[1].upper).toBe("WORLD");
		expect(result[1].lower).toBe("world");
	});

	it("should trim whitespace", async () => {
		const df = fromColumns(
			{ name: ["  hello  ", "world"] },
			{ name: DType.string },
		);
		const result = await df.withColumn("trimmed", col("name").trim()).toArray();
		expect(result[0].trimmed).toBe("hello");
		expect(result[1].trimmed).toBe("world");
	});

	it("should replace text", async () => {
		const df = fromColumns(
			{ text: ["hello world world"] },
			{ text: DType.string },
		);
		const result = await df
			.withColumn("replaced_all", col("text").replace("world", "earth", true))
			.withColumn("replaced_one", col("text").replace("world", "earth", false))
			.toArray();
		expect(result[0].replaced_all).toBe("hello earth earth");
		expect(result[0].replaced_one).toBe("hello earth world");
	});
});

describe("Math Functions", () => {
	it("should round values", async () => {
		const df = fromColumns(
			{ value: [3.14159, 2.71828] },
			{ value: DType.float64 },
		);
		const result = await df
			.withColumn("rounded_2", col("value").round(2))
			.toArray();
		expect(result[0].rounded_2).toBe(3.14);
		expect(result[1].rounded_2).toBe(2.72);
	});

	it("should floor and ceil values", async () => {
		const df = fromColumns(
			{ value: [3.7, -2.3] },
			{ value: DType.float64 },
		);
		const result = await df
			.withColumn("floored", col("value").floor())
			.withColumn("ceiled", col("value").ceil())
			.toArray();
		expect(result[0].floored).toBe(3);
		expect(result[0].ceiled).toBe(4);
		expect(result[1].floored).toBe(-3);
		expect(result[1].ceiled).toBe(-2);
	});

	it("should compute absolute value", async () => {
		const df = fromColumns(
			{ value: [-5, 3, -7.5] },
			{ value: DType.float64 },
		);
		const result = await df.withColumn("abs", col("value").abs()).toArray();
		expect(result[0].abs).toBe(5);
		expect(result[1].abs).toBe(3);
		expect(result[2].abs).toBe(7.5);
	});

	it("should compute square root", async () => {
		const df = fromColumns({ value: [9, 16, 25] }, { value: DType.float64 });
		const result = await df.withColumn("sqrt", col("value").sqrt()).toArray();
		expect(result[0].sqrt).toBe(3);
		expect(result[1].sqrt).toBe(4);
		expect(result[2].sqrt).toBe(5);
	});

	it("should compute power", async () => {
		const df = fromColumns({ value: [2, 3] }, { value: DType.float64 });
		const result = await df
			.withColumn("squared", col("value").pow(2))
			.withColumn("cubed", col("value").pow(3))
			.toArray();
		expect(result[0].squared).toBe(4);
		expect(result[0].cubed).toBe(8);
		expect(result[1].squared).toBe(9);
		expect(result[1].cubed).toBe(27);
	});
});

describe("Date/Time Operations", () => {
	it("should extract year from timestamp", async () => {
		const df = fromColumns(
			{
				ts: [
					new Date("2024-03-15T10:30:00Z").getTime(),
					new Date("2023-12-01T00:00:00Z").getTime(),
				],
			},
			{ ts: DType.timestamp },
		);
		const result = await df.withColumn("year", col("ts").year()).toArray();
		expect(result[0].year).toBe(2024);
		expect(result[1].year).toBe(2023);
	});

	it("should extract month and day from timestamp", async () => {
		const df = fromColumns(
			{
				ts: [
					new Date("2024-03-15T10:30:00Z").getTime(),
					new Date("2024-12-01T00:00:00Z").getTime(),
				],
			},
			{ ts: DType.timestamp },
		);
		const result = await df
			.withColumn("month", col("ts").month())
			.withColumn("day", col("ts").day())
			.toArray();
		expect(result[0].month).toBe(3);
		expect(result[0].day).toBe(15);
		expect(result[1].month).toBe(12);
		expect(result[1].day).toBe(1);
	});

	it("should extract hour, minute, second from timestamp", async () => {
		const df = fromColumns(
			{ ts: [new Date("2024-03-15T14:30:45Z").getTime()] },
			{ ts: DType.timestamp },
		);
		const result = await df
			.withColumn("hour", col("ts").hour())
			.withColumn("minute", col("ts").minute())
			.withColumn("second", col("ts").second())
			.toArray();
		expect(result[0].hour).toBe(14);
		expect(result[0].minute).toBe(30);
		expect(result[0].second).toBe(45);
	});
});

describe("Conditional when/otherwise", () => {
	it("should handle single when/otherwise", async () => {
		const df = fromColumns({ score: [95, 75, 85] }, { score: DType.int32 });
		const result = await df
			.withColumn(
				"grade",
				when(col("score").gte(90), "A").otherwise("Not A"),
			)
			.toArray();
		expect(result[0].grade).toBe("A");
		expect(result[1].grade).toBe("Not A");
		expect(result[2].grade).toBe("Not A");
	});

	it("should handle multiple when clauses", async () => {
		const df = fromColumns(
			{ score: [95, 85, 75, 65] },
			{ score: DType.int32 },
		);
		const result = await df
			.withColumn(
				"grade",
				when(col("score").gte(90), "A")
					.when(col("score").gte(80), "B")
					.when(col("score").gte(70), "C")
					.otherwise("D"),
			)
			.toArray();
		expect(result[0].grade).toBe("A");
		expect(result[1].grade).toBe("B");
		expect(result[2].grade).toBe("C");
		expect(result[3].grade).toBe("D");
	});

	it("should handle when with numeric return values", async () => {
		const df = fromColumns({ value: [10, -5, 0] }, { value: DType.int32 });
		const result = await df
			.withColumn(
				"sign",
				when(col("value").gt(0), 1)
					.when(col("value").lt(0), -1)
					.otherwise(0),
			)
			.toArray();
		expect(result[0].sign).toBe(1);
		expect(result[1].sign).toBe(-1);
		expect(result[2].sign).toBe(0);
	});
});

describe("Advanced Aggregations", () => {
	it("should compute standard deviation", async () => {
		const df = fromColumns(
			{
				category: ["A", "A", "A", "B", "B"],
				value: [10, 20, 30, 5, 15],
			},
			{ category: DType.string, value: DType.float64 },
		);
		const result = await df
			.groupBy("category")
			.agg([{ name: "std_val", expr: std("value") }])
			.toArray();

		// Standard deviation for A: values [10, 20, 30], mean=20, sample_std = 10
		// Standard deviation for B: values [5, 15], mean=10, sample_std ≈ 7.07
		expect(result[0].std_val).toBeCloseTo(10, 1);
		expect(result[1].std_val).toBeCloseTo(7.07, 1);
	});

	it("should compute variance", async () => {
		const df = fromColumns(
			{
				category: ["A", "A", "A"],
				value: [10, 20, 30],
			},
			{ category: DType.string, value: DType.float64 },
		);
		const result = await df
			.groupBy("category")
			.agg([{ name: "var_val", expr: variance("value") }])
			.toArray();

		// Variance for A: values [10, 20, 30], mean=20, sample_var = 100
		expect(result[0].var_val).toBeCloseTo(100, 1);
	});

	it("should compute median", async () => {
		const df = fromColumns(
			{
				category: ["A", "A", "A", "A", "A", "B", "B", "B"],
				value: [10, 20, 30, 40, 50, 1, 2, 3],
			},
			{ category: DType.string, value: DType.float64 },
		);
		const result = await df
			.groupBy("category")
			.agg([{ name: "median_val", expr: median("value") }])
			.toArray();

		// Median for A: [10, 20, 30, 40, 50] -> 30
		// Median for B: [1, 2, 3] -> 2
		expect(result[0].median_val).toBe(30);
		expect(result[1].median_val).toBe(2);
	});

	it("should count distinct values", async () => {
		const df = fromColumns(
			{
				category: ["A", "A", "A", "B", "B", "B"],
				value: [10, 20, 10, 5, 5, 5],
			},
			{ category: DType.string, value: DType.int32 },
		);
		const result = await df
			.groupBy("category")
			.agg([{ name: "distinct_count", expr: countDistinct("value") }])
			.toArray();

		expect(result[0].distinct_count).toBe(2n); // A has 2 distinct values
		expect(result[1].distinct_count).toBe(1n); // B has 1 distinct value
	});
});

describe("DataFrame Creation", () => {
	it("should create DataFrame from columns", async () => {
		const df = fromColumns({
			a: [1, 2, 3],
			b: ["x", "y", "z"],
			c: [true, false, true],
		});

		const result = await df.toArray();
		expect(result).toHaveLength(3);
		expect(result[0].a).toBe(1);
		expect(result[0].b).toBe("x");
		expect(result[0].c).toBe(true);
		expect(result[2].a).toBe(3);
		expect(result[2].b).toBe("z");
		expect(result[2].c).toBe(true);
	});

	it("should create DataFrame from arrays", async () => {
		const arr1 = new Float64Array([1.1, 2.2, 3.3]);
		const arr2 = new Int32Array([10, 20, 30]);

		const df = fromArrays(
			{ col1: arr1, col2: arr2 },
			{ col1: DType.float64, col2: DType.int32 },
		);

		const result = await df.toArray();
		expect(result[0].col1).toBe(1.1);
		expect(result[0].col2).toBe(10);
		expect(result[2].col1).toBe(3.3);
		expect(result[2].col2).toBe(30);
	});

	it("should create DataFrame with range", async () => {
		const df = range(0, 10, 2);
		const result = await df.toArray();

		expect(result).toHaveLength(5);
		expect(result[0].value).toBe(0);
		expect(result[1].value).toBe(2);
		expect(result[4].value).toBe(8);
	});

	it("should create DataFrame with range and custom column name", async () => {
		const df = range(1, 5, 1, "id");
		const result = await df.toArray();

		expect(result).toHaveLength(4);
		expect(result[0].id).toBe(1);
		expect(result[3].id).toBe(4);
	});
});

describe("Utility Operations", () => {
	it("should filter with isIn", async () => {
		const df = fromColumns(
			{ status: ["active", "pending", "inactive", "active", "completed"] },
			{ status: DType.string },
		);

		const result = await df
			.filter(col("status").isIn(["active", "pending"]))
			.toArray();

		expect(result).toHaveLength(3);
		const statuses = result.map((r) => r.status);
		expect(statuses.filter((s) => s === "active")).toHaveLength(2);
		expect(statuses.filter((s) => s === "pending")).toHaveLength(1);
	});

	it("should filter with isIn on numeric values", async () => {
		const df = fromColumns(
			{ value: [1, 2, 3, 4, 5] },
			{ value: DType.int32 },
		);

		const result = await df.filter(col("value").isIn([2, 4])).toArray();

		expect(result).toHaveLength(2);
		const values = result.map((r) => r.value).sort();
		expect(values).toEqual([2, 4]);
	});
});
