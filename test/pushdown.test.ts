/**
 * Tests for P2 CSV predicate pushdown.
 *
 * Tests: compilePushdownFilter for various expression types,
 * CSV source filter application, pushdown wiring in stream/collect.
 */
import { describe, test, expect } from "bun:test";
import { compilePushdownFilter } from "../src/ops/pushdown.ts";
import { col, lit } from "../src/expr/builders.ts";
import { ExprType, type Expr } from "../src/expr/ast.ts";
import { DType } from "../src/types/dtypes.ts";
import { createSchema } from "../src/types/schema.ts";
import { unwrap } from "../src/types/error.ts";
import { fromCsvString, readCsv } from "../src/dataframe/dataframe.ts";

const schema = unwrap(
	createSchema({
		id: DType.int32,
		name: DType.string,
		value: DType.float64,
		active: DType.boolean,
	}),
);

describe("P2: compilePushdownFilter", () => {
	test("compiles simple eq comparison", () => {
		const expr = col("id").eq(5);
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles neq comparison", () => {
		const expr = col("id").neq(5);
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles lt comparison", () => {
		const expr = col("id").lt(10);
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles lte comparison", () => {
		const expr = col("id").lte(10);
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles gt comparison", () => {
		const expr = col("id").gt(10);
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles gte comparison", () => {
		const expr = col("id").gte(10);
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles isNull check", () => {
		const expr = col("name").isNull();
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles isNotNull check", () => {
		const expr = col("name").isNotNull();
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles string contains", () => {
		const expr = col("name").contains("foo");
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles string startsWith", () => {
		const expr = col("name").startsWith("A");
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles string endsWith", () => {
		const expr = col("name").endsWith("z");
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles AND expressions", () => {
		const expr: Expr = {
			type: ExprType.And,
			exprs: [col("id").gt(5), col("id").lt(10)],
		};
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles OR expressions", () => {
		const expr: Expr = {
			type: ExprType.Or,
			exprs: [col("id").eq(1), col("id").eq(2)],
		};
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("compiles NOT expressions", () => {
		const expr: Expr = {
			type: ExprType.Not,
			expr: col("id").gt(lit(5)),
		};
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();
	});

	test("returns null for non-pushdownable expression", () => {
		// An expression involving column-to-column comparison
		// or a function call should not be pushdownable
		const expr: Expr = {
			type: ExprType.Add,
			left: col("id").toExpr(),
			right: col("value").toExpr(),
		};
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).toBeNull();
	});

	test("compiled predicate filters correctly", () => {
		const expr = col("id").gt(2);
		const predicate = compilePushdownFilter(expr, schema);
		expect(predicate).not.toBeNull();

		// Test the predicate function
		const getValue = (idx: number) => {
			if (idx === 0) return 5; // id=5
			return null;
		};
		expect(predicate!(getValue)).toBe(true);

		const getValue2 = (idx: number) => {
			if (idx === 0) return 1; // id=1
			return null;
		};
		expect(predicate!(getValue2)).toBe(false);
	});
});

describe("P2: CSV Pushdown Integration", () => {
	const csvData = `id,name,value
1,Alice,10.5
2,Bob,20.0
3,Charlie,30.5
4,Diana,40.0
5,Eve,50.5`;

	test("filter pushes down to CSV source and produces correct results", async () => {
		const df = fromCsvString(
			csvData,
			{ id: DType.int32, name: DType.string, value: DType.float64 },
		);

		const result = await df.filter(col("id").gt(3)).toArray();
		expect(result.length).toBe(2);
		expect(result[0]!.name).toBe("Diana");
		expect(result[1]!.name).toBe("Eve");
	});

	test("string filter pushes down correctly", async () => {
		const df = fromCsvString(
			csvData,
			{ id: DType.int32, name: DType.string, value: DType.float64 },
		);

		const result = await df.filter(col("name").startsWith("D")).toArray();
		expect(result.length).toBe(1);
		expect(result[0]!.name).toBe("Diana");
	});

	test("compound filter pushes down correctly", async () => {
		const df = fromCsvString(
			csvData,
			{ id: DType.int32, name: DType.string, value: DType.float64 },
		);

		const result = await df
			.filter(col("id").gte(2))
			.filter(col("id").lte(4))
			.toArray();
		expect(result.length).toBe(3);
		expect(result.map((r: any) => r.id)).toEqual([2, 3, 4]);
	});
});
