/**
 * Aggregate operator.
 *
 * Performs aggregations without grouping (full-table aggregation).
 * Buffers all input then produces a single row of results.
 */
/** biome-ignore-all lint/style/noNonNullAssertion: Intentional */

import { Chunk } from "../buffer/chunk.ts";
import { ColumnBuffer } from "../buffer/column-buffer.ts";
import { createDictionary, type Dictionary } from "../buffer/dictionary.ts";
import { recycleChunk } from "../buffer/pool.ts";
import { type Expr, ExprType } from "../expr/ast.ts";
import { type CompiledValue, compileValue } from "../expr/compiler.ts";
import { inferExprType } from "../expr/types.ts";
import type { DType } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import { createSchema, type Schema, type SchemaSpec } from "../types/schema.ts";
import { type AggState, AggType, createAggState } from "./agg-state.ts";
import {
	type Operator,
	type OperatorResult,
	opDone,
	opEmpty,
	opResult,
} from "./operator.ts";

/** Aggregation specification */
export interface AggSpec {
	/** Output column name */
	name: string;
	/** Aggregation expression (must be an aggregation type) */
	expr: Expr;
}

/** Compiled aggregation info */
interface CompiledAgg {
	name: string;
	state: AggState;
	valueExpr: CompiledValue | null; // null for count(*)
	isCountAll: boolean;
}

/**
 * Aggregate operator - full table aggregation.
 */
export class AggregateOperator implements Operator {
	readonly name = "Aggregate";
	readonly outputSchema: Schema;

	private readonly aggs: CompiledAgg[];
	private readonly dictionary: Dictionary;
	private finished: boolean = false;

	private constructor(
		outputSchema: Schema,
		aggs: CompiledAgg[],
		dictionary: Dictionary,
	) {
		this.outputSchema = outputSchema;
		this.aggs = aggs;
		this.dictionary = dictionary;
	}

	/**
	 * Create an aggregate operator.
	 */
	static create(
		inputSchema: Schema,
		specs: AggSpec[],
	): Result<AggregateOperator> {
		if (specs.length === 0) {
			return err(ErrorCode.EmptySchema);
		}

		const aggs: CompiledAgg[] = [];
		const outputSpec: SchemaSpec = {};
		const seen = new Set<string>();

		for (const spec of specs) {
			if (seen.has(spec.name)) {
				return err(ErrorCode.DuplicateColumn);
			}
			seen.add(spec.name);

			const compiled = compileAggExpr(spec.expr, inputSchema);
			if (compiled.error !== ErrorCode.None) {
				return err(compiled.error);
			}

			aggs.push({
				name: spec.name,
				state: compiled.value.state,
				valueExpr: compiled.value.valueExpr,
				isCountAll: compiled.value.isCountAll,
			});

			outputSpec[spec.name] = compiled.value.state.outputDType;
		}

		const schemaResult = createSchema(outputSpec);
		if (schemaResult.error !== ErrorCode.None) {
			return err(schemaResult.error);
		}

		return ok(
			new AggregateOperator(schemaResult.value, aggs, createDictionary()),
		);
	}

	process(chunk: Chunk): Result<OperatorResult> {
		if (this.finished) {
			recycleChunk(chunk);
			return ok(opDone());
		}

		const rowCount = chunk.rowCount;
		if (rowCount === 0) {
			recycleChunk(chunk);
			return ok(opEmpty());
		}

		for (const agg of this.aggs) {
			if (agg.isCountAll) {
				// Vectorized: accumulate entire chunk at once instead of per-row loop
				agg.state.accumulate(rowCount);
			} else if (agg.valueExpr !== null) {
				// Evaluate expression and accumulate
				for (let i = 0; i < rowCount; i++) {
					const value = agg.valueExpr(chunk, i);
					agg.state.accumulate(value as number | bigint | null);
				}
			}
		}

		// Don't emit until finish()
		recycleChunk(chunk);
		return ok(opEmpty());
	}

	finish(): Result<OperatorResult> {
		if (this.finished) {
			return ok(opDone());
		}
		this.finished = true;

		// Create output columns
		const columns: ColumnBuffer[] = [];

		for (let i = 0; i < this.aggs.length; i++) {
			const agg = this.aggs[i]!;
			const dtype = this.outputSchema.columns[i]!.dtype;
			const col = new ColumnBuffer(dtype.kind, 1, dtype.nullable);

			const result = agg.state.result();
			if (result === null) {
				col.appendNull();
			} else if (typeof result === "bigint") {
				col.append(result as never);
			} else {
				col.append(result as never);
			}

			columns.push(col);
		}

		const resultChunk = new Chunk(this.outputSchema, columns, this.dictionary);
		return ok(opResult(resultChunk, true));
	}

	reset(): void {
		this.finished = false;
		for (const agg of this.aggs) {
			agg.state.reset();
		}
	}
}

/** Compile an aggregation expression */
function compileAggExpr(
	expr: Expr,
	schema: Schema,
): Result<{
	state: AggState;
	valueExpr: CompiledValue | null;
	isCountAll: boolean;
}> {
	let aggType: AggType;
	let innerExpr: Expr | null = null;
	let isCountAll = false;

	switch (expr.type) {
		case ExprType.Sum:
			aggType = AggType.Sum;
			innerExpr = expr.expr;
			break;
		case ExprType.Avg:
			aggType = AggType.Avg;
			innerExpr = expr.expr;
			break;
		case ExprType.Min:
			aggType = AggType.Min;
			innerExpr = expr.expr;
			break;
		case ExprType.Max:
			aggType = AggType.Max;
			innerExpr = expr.expr;
			break;
		case ExprType.First:
			aggType = AggType.First;
			innerExpr = expr.expr;
			break;
		case ExprType.Last:
			aggType = AggType.Last;
			innerExpr = expr.expr;
			break;
		case ExprType.Count:
			if (expr.expr === null) {
				// count(*)
				aggType = AggType.CountAll;
				isCountAll = true;
			} else {
				aggType = AggType.Count;
				innerExpr = expr.expr;
			}
			break;
		case ExprType.Std:
			aggType = AggType.Std;
			innerExpr = expr.expr;
			break;
		case ExprType.Var:
			aggType = AggType.Var;
			innerExpr = expr.expr;
			break;
		case ExprType.Median:
			aggType = AggType.Median;
			innerExpr = expr.expr;
			break;
		case ExprType.CountDistinct:
			aggType = AggType.CountDistinct;
			innerExpr = expr.expr;
			break;
		default:
			return err(ErrorCode.InvalidAggregation);
	}

	let valueExpr: CompiledValue | null = null;
	let inputDType: DType | undefined;

	if (innerExpr !== null) {
		const typeResult = inferExprType(innerExpr, schema);
		if (typeResult.error !== ErrorCode.None) {
			return err(typeResult.error);
		}
		inputDType = typeResult.value.dtype;

		const valueResult = compileValue(innerExpr, schema);
		if (valueResult.error !== ErrorCode.None) {
			return err(valueResult.error);
		}
		valueExpr = valueResult.value;
	}

	const state = createAggState(aggType, inputDType);
	return ok({ state, valueExpr, isCountAll });
}

/**
 * Create an aggregate operator.
 */
export function aggregate(
	inputSchema: Schema,
	specs: AggSpec[],
): Result<AggregateOperator> {
	return AggregateOperator.create(inputSchema, specs);
}
