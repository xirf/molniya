import { describe, expect, it } from "bun:test";
import { DataFrame } from "../src/dataframe/dataframe.ts";
import { DType } from "../src/types/dtypes.ts";
import { unwrap } from "../src/types/error.ts";
import { createSchema } from "../src/types/schema.ts";

describe("DataFrame API Improvements", () => {
	// Helper to create a dummy dataframe
	const schema = unwrap(createSchema({ id: DType.int32, name: DType.string }));
	const df = DataFrame.empty(schema, null);

	describe("Set Operations", () => {
		it("should support crossJoin", async () => {
			const df2 = DataFrame.empty(
				unwrap(createSchema({ val: DType.float64 })),
				null,
			);
			expect(typeof df.crossJoin).toBe("function");
			const result = await df.crossJoin(df2);
			expect(result).toBeInstanceOf(DataFrame);
		});

		it("should support antiJoin", async () => {
			expect(typeof df.antiJoin).toBe("function");
		});

		it("should support semiJoin", async () => {
			expect(typeof df.semiJoin).toBe("function");
		});
		
		it("should support union and unionAll", async () => {
			const df2 = DataFrame.empty(schema, null);
			expect(typeof df.union).toBe("function");
			expect(typeof df.unionAll).toBe("function");

			const unioned = await df.union(df2);
			const unionAll = await df.unionAll(df2);
			expect(unioned).toBeInstanceOf(DataFrame);
			expect(unionAll).toBeInstanceOf(DataFrame);
		});
	});

	describe("Transformations", () => {
		it("should support explode on array columns", () => {
			expect(typeof df.explode).toBe("function");
		});
	});

	describe("Aggregation", () => {
		it("should support chained groupBy syntax", () => {
			const grouped = df.groupBy("id");
			expect(grouped).not.toBeInstanceOf(DataFrame);
			expect(typeof grouped.count).toBe("function");
			expect(typeof grouped.agg).toBe("function");
		});

		it("should support shortcut stats methods", /* async */ () => {
			expect(typeof df.min).toBe("function");
			expect(typeof df.max).toBe("function");
			expect(typeof df.mean).toBe("function");
		});
	});

	describe("Inspection", () => {
		it("should have printSchema", () => {
			expect(typeof df.printSchema).toBe("function");
		});

		it("should have explain", () => {
			expect(typeof df.explain).toBe("function");
		});
	});
});
