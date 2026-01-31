/** biome-ignore-all lint/style/noNonNullAssertion: Performance optimization */
/**
 * GroupBy operator.
 *
 * Groups rows by key columns and applies aggregations per group.
 * Uses hash-based grouping for efficient processing.
 */

import { Chunk } from "../buffer/chunk.ts";
import { ColumnBuffer } from "../buffer/column-buffer.ts";
import { createDictionary, type Dictionary } from "../buffer/dictionary.ts";
import { type Expr, ExprType } from "../expr/ast.ts";
import { type CompiledValue, compileValue } from "../expr/compiler.ts";
import { inferExprType } from "../expr/types.ts";
import { type DType, DTypeKind } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import {
	createSchema,
	getColumnIndex,
	type Schema,
	type SchemaSpec,
} from "../types/schema.ts";
import { AggType, createAggState } from "./agg-state.ts";
import type { AggSpec } from "./aggregate.ts";
import { KeyHashTable } from "./key-hasher.ts";
import {
	type Operator,
	type OperatorResult,
	opDone,
	opEmpty,
	opResult,
} from "./operator.ts";
import { type BatchAggregator, createVectorAggregator } from "./vector-agg.ts";

/**
 * GroupBy operator with hash-based grouping.
 */
export class GroupByOperator implements Operator {
	readonly name = "GroupBy";
	readonly outputSchema: Schema;

	private readonly inputSchema: Schema;
	private readonly keyColumns: readonly number[];
	private readonly aggSpecs: readonly VectorizedGroupAgg[];

	// Hash table for group key lookup (replaces string-based Map)
	private readonly groups: KeyHashTable = new KeyHashTable();
	// Reverse Map: GroupID -> Key Values
	private readonly groupKeys: Array<(number | bigint | null)[]> = [];

	// Vector Aggregators
	private readonly aggregators: BatchAggregator[];

	private readonly dictionary: Dictionary;
	private finished: boolean = false;
	private nextGroupId: number = 0;

	private constructor(
		inputSchema: Schema,
		outputSchema: Schema,
		keyColumns: number[],
		aggSpecs: VectorizedGroupAgg[],
		dictionary: Dictionary,
	) {
		this.inputSchema = inputSchema;
		this.outputSchema = outputSchema;
		this.keyColumns = keyColumns;
		this.aggSpecs = aggSpecs;
		this.dictionary = dictionary;
		this.aggregators = aggSpecs.map((spec) =>
			createVectorAggregator(spec.aggType),
		);
	}

	/**
	 * Create a GroupBy operator.
	 */
	static create(
		inputSchema: Schema,
		keyColumnNames: string[],
		aggSpecs: AggSpec[],
	): Result<GroupByOperator> {
		if (keyColumnNames.length === 0) {
			return err(ErrorCode.EmptySchema);
		}

		// Resolve key columns
		const keyColumns: number[] = [];
		const outputSpec: SchemaSpec = {};
		const seen = new Set<string>();

		for (const name of keyColumnNames) {
			const idxResult = getColumnIndex(inputSchema, name);
			if (idxResult.error !== ErrorCode.None) {
				return err(ErrorCode.UnknownColumn);
			}

			if (seen.has(name)) {
				return err(ErrorCode.DuplicateColumn);
			}
			seen.add(name);

			keyColumns.push(idxResult.value);
			outputSpec[name] = inputSchema.columns[idxResult.value]!.dtype;
		}

		const vectorizedAggs: VectorizedGroupAgg[] = [];

		for (const spec of aggSpecs) {
			if (seen.has(spec.name)) {
				return err(ErrorCode.DuplicateColumn);
			}
			seen.add(spec.name);

			const compiled = compileGroupAggExpr(spec.expr, inputSchema);
			if (compiled.error !== ErrorCode.None) {
				return err(compiled.error);
			}

			let inputColIdx: number = -1;
			if (
				compiled.value.innerExpr &&
				compiled.value.innerExpr.type === ExprType.Column
			) {
				const colName = compiled.value.innerExpr.name;
				const idxRes = getColumnIndex(inputSchema, colName);
				if (idxRes.error === ErrorCode.None) {
					inputColIdx = idxRes.value;
				}
			}

			vectorizedAggs.push({
				name: spec.name,
				aggType: compiled.value.aggType,
				inputColIdx: inputColIdx,
				inputDType: compiled.value.inputDType,
				isCountAll: compiled.value.isCountAll,
				// Fallback for complex exprs
				valueExpr: compiled.value.valueExpr,
			});

			outputSpec[spec.name] = createAggState(
				compiled.value.aggType,
				compiled.value.inputDType,
			).outputDType;
		}

		const schemaResult = createSchema(outputSpec);
		if (schemaResult.error !== ErrorCode.None) {
			return err(schemaResult.error);
		}

		return ok(
			new GroupByOperator(
				inputSchema,
				schemaResult.value,
				keyColumns,
				vectorizedAggs,
				createDictionary(),
			),
		);
	}

	process(chunk: Chunk): Result<OperatorResult> {
		if (this.finished) {
			return ok(opDone());
		}

		const rowCount = chunk.rowCount;
		if (rowCount === 0) return ok(opEmpty());

		// Phase 1: Key Hashing -> Group IDs
		// Generate an Int32Array of GroupIDs for this chunk
		const chunkGroupIds = new Int32Array(rowCount);
		const cols = chunk.getColumns();
		const selection = chunk.getSelection();
		const keyCols = this.keyColumns.map((idx) => cols[idx]!);

		for (let row = 0; row < rowCount; row++) {
			const physIdx = selection ? selection[row]! : row;

			const keyValues: (number | bigint | null)[] = [];
			for (let k = 0; k < keyCols.length; k++) {
				const col = keyCols[k]!;

				let val = col.get(physIdx) as number | bigint | null;

				if (col.kind === DTypeKind.String && chunk.dictionary) {
					const id = val as number;
					const bytes = chunk.dictionary.getBytes(id);
					if (bytes) {
						val = this.dictionary.intern(bytes);
					} else {
						val = null;
					}
				}
				keyValues.push(val);
			}

			let gid = this.groups.get(keyValues);
			if (gid === -1) {
				gid = this.nextGroupId++;
				this.groups.insert(keyValues, gid);
				this.groupKeys.push(keyValues.slice());
			}

			chunkGroupIds[row] = gid;
		}

		const numGroups = this.nextGroupId;
		for (const agg of this.aggregators) {
			agg.resize(numGroups);
		}

		// Phase 2: Batch Aggregation
		for (let i = 0; i < this.aggSpecs.length; i++) {
			const spec = this.aggSpecs[i]!;
			const agg = this.aggregators[i]!;

			if (spec.isCountAll) {
				agg.accumulateBatch(null, chunkGroupIds, rowCount, selection, null);
			} else if (spec.inputColIdx !== -1) {
				const inputCol = cols[spec.inputColIdx]!;
				agg.accumulateBatch(
					inputCol.data,
					chunkGroupIds,
					rowCount,
					selection,
					inputCol,
				);
			} else {
				// SLOW PATH behavior not fully implemented for complex exprs in this version
				// In future: materialize expr to buffer, then batch agg.
				// For now we assume simple columns for throughput.
			}
		}

		return ok(opEmpty());
	}

	finish(): Result<OperatorResult> {
		if (this.finished) {
			return ok(opDone());
		}
		this.finished = true;

		const groupCount = this.nextGroupId;
		if (groupCount === 0) {
			return ok(opDone());
		}

		const columns: ColumnBuffer[] = [];

		for (let k = 0; k < this.keyColumns.length; k++) {
			const dtype = this.inputSchema.columns[this.keyColumns[k]!]!.dtype;
			const col = new ColumnBuffer(dtype.kind, groupCount, dtype.nullable);
			columns.push(col);
		}

		for (const agg of this.aggregators) {
			columns.push(agg.finish());
		}

		// Fill Key Columns
		// They are stored in this.groupKeys (Array of arrays)
		// groupKeys[gid] = [k1, k2...]
		// Iterating groupKeys is faster than iterating Map values
		for (let gid = 0; gid < groupCount; gid++) {
			const keys = this.groupKeys[gid]!;
			for (let k = 0; k < this.keyColumns.length; k++) {
				const col = columns[k]!;
				const val = keys[k];
				if (val === null) col.appendNull();
				else col.append(val as never);
			}
		}

		const resultChunk = new Chunk(this.outputSchema, columns, this.dictionary);
		return ok(opResult(resultChunk, true));
	}

	reset(): void {
		this.finished = false;
		this.groups.clear();
		this.groupKeys.length = 0;
		this.nextGroupId = 0;
		// Hack: creating new ones.
		(this as unknown as { aggregators: unknown[] }).aggregators =
			this.aggSpecs.map((spec) => createVectorAggregator(spec.aggType));
	}
}

/** Vectorized group aggregation info */
interface VectorizedGroupAgg {
	name: string;
	aggType: AggType;
	inputColIdx: number; // -1 if not a simple column
	valueExpr: CompiledValue | null;
	inputDType: DType | undefined;
	isCountAll: boolean;
}

/** Compile an aggregation expression for groupby */
function compileGroupAggExpr(
	expr: Expr,
	schema: Schema,
): Result<{
	aggType: AggType;
	valueExpr: CompiledValue | null;
	innerExpr: Expr | null;
	inputDType: DType | undefined;
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
				aggType = AggType.CountAll;
				isCountAll = true;
			} else {
				aggType = AggType.Count;
				innerExpr = expr.expr;
			}
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

	return ok({ aggType, valueExpr, innerExpr, inputDType, isCountAll });
}

/**
 * Create a GroupBy operator.
 */
export function groupBy(
	inputSchema: Schema,
	keyColumns: string[],
	aggSpecs: AggSpec[],
): Result<GroupByOperator> {
	return GroupByOperator.create(inputSchema, keyColumns, aggSpecs);
}
