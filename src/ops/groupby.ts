/** biome-ignore-all lint/style/noNonNullAssertion: Performance optimization */
/**
 * GroupBy operator.
 *
 * Groups rows by key columns and applies aggregations per group.
 * Uses hash-based grouping for efficient processing.
 */

import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { Chunk } from "../buffer/chunk.ts";
import { ColumnBuffer } from "../buffer/column-buffer.ts";
import { createDictionary, type Dictionary } from "../buffer/dictionary.ts";
import { recycleChunk } from "../buffer/pool.ts";
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
import { KeyHashTable, hashKey } from "./key-hasher.ts";
import { StringViewColumnBuffer } from "../buffer/string-view-column.ts";
import {
	type Operator,
	type OperatorResult,
	opDone,
	opEmpty,
	opResult,
} from "./operator.ts";
import { type BatchAggregator, createVectorAggregator } from "./vector-agg.ts";
import { BinaryWriter } from "../io/mbf/writer.ts";
import { BinaryReader } from "../io/mbf/reader.ts";

/** Default max groups before spilling (250K) */
const DEFAULT_MAX_GROUPS = 250_000;

/** Options for GroupBy operator */
export interface GroupByOptions {
	/** Max number of groups before spilling partial results to disk (default: 250K, 0 = never spill) */
	maxGroups?: number;
}

/** Merge-aggregation type mapping: how partial results are combined */
const MERGE_AGG_TYPE: Partial<Record<AggType, AggType>> = {
	[AggType.Sum]: AggType.Sum,
	[AggType.Count]: AggType.Sum, // sum partial counts
	[AggType.CountAll]: AggType.Sum, // sum partial counts
	[AggType.Min]: AggType.Min,
	[AggType.Max]: AggType.Max,
	[AggType.First]: AggType.First,
	[AggType.Last]: AggType.Last,
};

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
	// Columnar key storage: GroupID -> Key Values
	private groupKeyCols: ColumnBuffer[];

	// Vector Aggregators
	private readonly aggregators: BatchAggregator[];

	private readonly dictionary: Dictionary;
	private finished: boolean = false;
	private nextGroupId: number = 0;
	/** Pre-allocated key buffer reused per row to avoid GC pressure (3.2 optimization) */
	private readonly _keyBuffer: (number | bigint | Uint8Array | null)[];

	/** Spill config: max groups before spilling (0 = disabled) */
	private readonly maxGroups: number;
	/** Paths to spill files containing partial aggregate results */
	private spillFiles: string[] = [];
	/** Temp directory for spill files */
	private spillDir: string | null = null;
	/** Whether the current agg types support partial merging */
	private readonly canMerge: boolean;
	/** Key column names for re-aggregation */
	private readonly keyColumnNames: string[];
	/** Original agg specs for re-aggregation */
	private readonly originalAggSpecs: AggSpec[];

	private constructor(
		inputSchema: Schema,
		outputSchema: Schema,
		keyColumns: number[],
		aggSpecs: VectorizedGroupAgg[],
		dictionary: Dictionary,
		options?: GroupByOptions,
		keyColumnNames?: string[],
		originalAggSpecs?: AggSpec[],
	) {
		this.inputSchema = inputSchema;
		this.outputSchema = outputSchema;
		this.keyColumns = keyColumns;
		this.aggSpecs = aggSpecs;
		this.dictionary = dictionary;
		this._keyBuffer = new Array(keyColumns.length);
		this.aggregators = aggSpecs.map((spec) =>
			createVectorAggregator(spec.aggType),
		);
		this.maxGroups = options?.maxGroups ?? DEFAULT_MAX_GROUPS;
		this.keyColumnNames = keyColumnNames ?? [];
		this.originalAggSpecs = originalAggSpecs ?? [];

		this.groupKeyCols = this.keyColumns.map((cIdx) => {
			const dtype = this.inputSchema.columns[cIdx]!.dtype;
			if (dtype.kind === DTypeKind.StringView) {
				return new StringViewColumnBuffer(1024, dtype.nullable) as unknown as ColumnBuffer;
			}
			return new ColumnBuffer(dtype.kind, 1024, dtype.nullable);
		});

		// Check if all agg types support partial merging
		this.canMerge = aggSpecs.every(
			(spec) => MERGE_AGG_TYPE[spec.aggType] !== undefined,
		);
	}

	/**
	 * Create a GroupBy operator.
	 */
	static create(
		inputSchema: Schema,
		keyColumnNames: string[],
		aggSpecs: AggSpec[],
		options?: GroupByOptions,
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
				options,
				keyColumnNames,
				aggSpecs,
			),
		);
	}

	process(chunk: Chunk): Result<OperatorResult> | Promise<Result<OperatorResult>> {
		if (this.finished) {
			return ok(opDone());
		}

		// If a spill is in flight from the previous chunk, await it before proceeding.
		// This ensures state is fully reset before accumulating new rows.
		if (this._pendingSpill) {
			return this._processAfterSpill(chunk);
		}

		return this._processChunk(chunk);
	}

	/** Await any pending spill then process the chunk. */
	private async _processAfterSpill(chunk: Chunk): Promise<Result<OperatorResult>> {
		if (this._pendingSpill) {
			await this._pendingSpill;
			this._pendingSpill = null;
		}
		return this._processChunk(chunk);
	}

	/** Core chunk processing logic (sync). */
	private _processChunk(chunk: Chunk): Result<OperatorResult> {
		const rowCount = chunk.rowCount;
		if (rowCount === 0) {
			recycleChunk(chunk);
			return ok(opEmpty());
		}

		// Phase 1: Key Hashing -> Group IDs
		// Generate an Int32Array of GroupIDs for this chunk
		const chunkGroupIds = new Int32Array(rowCount);
		const cols = chunk.getColumns();
		const selection = chunk.getSelection();
		const keyCols = this.keyColumns.map((idx) => cols[idx]!);

		for (let row = 0; row < rowCount; row++) {
			const physIdx = selection ? selection[row]! : row;

			// Reuse a single key buffer to avoid per-row array allocation (3.2 optimization)
			for (let k = 0; k < keyCols.length; k++) {
				const col = keyCols[k]!;

				let val: number | bigint | Uint8Array | null;

				if (col.kind === DTypeKind.StringView) {
					const bytes = (col as unknown as import("../buffer/string-view-column.ts").StringViewColumnBuffer).getBytes(physIdx);
					val = bytes ?? null;
				} else {
					val = col.get(physIdx) as number | bigint | null;

					if (col.kind === DTypeKind.String && chunk.dictionary) {
						const id = val as number;
						const bytes = chunk.dictionary.getBytes(id);
						if (bytes) {
							val = this.dictionary.intern(bytes);
						} else {
							val = null;
						}
					}
				}
				this._keyBuffer[k] = val;
			}

			const hash = hashKey(this._keyBuffer);
			const keysEqual = (gid: number) => {
				for (let k = 0; k < keyCols.length; k++) {
					const col = this.groupKeyCols[k]!;
					const v = this._keyBuffer[k];
					if (v === null) {
						if (!col.isNull(gid)) return false;
					} else {
						if (col.isNull(gid)) return false;
						if (v instanceof Uint8Array) {
							const bytes = (col as unknown as StringViewColumnBuffer).getBytes(gid);
							if (!bytes || bytes.length !== v.length) return false;
							for (let j = 0; j < v.length; j++) {
								if (bytes[j] !== v[j]) return false;
							}
						} else {
							if (col.get(gid) !== v) return false;
						}
					}
				}
				return true;
			};

			let gid = this.groups.getOrInsert(hash, keysEqual, true, this.nextGroupId);
			if (gid === this.nextGroupId) {
				this.nextGroupId++;
				for (let k = 0; k < keyCols.length; k++) {
					const col = this.groupKeyCols[k]!;
					const v = this._keyBuffer[k];
					if (v === null) {
						col.appendNull();
					} else if (v instanceof Uint8Array) {
						(col as unknown as StringViewColumnBuffer).appendBytes(v, 0, v.length);
					} else {
						col.append(v as never);
					}
				}
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
				agg.accumulateBatch(null, chunkGroupIds, rowCount, selection, null, chunk.dictionary);
			} else if (spec.inputColIdx !== -1) {
				const inputCol = cols[spec.inputColIdx]!;
				agg.accumulateBatch(
					inputCol.data,
					chunkGroupIds,
					rowCount,
					selection,
					inputCol,
					chunk.dictionary,
				);
			} else {
				// SLOW PATH behavior not fully implemented for complex exprs in this version
			}
		}

		// Check if we should spill partial results
		if (
			this.maxGroups > 0 &&
			this.canMerge &&
			this.nextGroupId > this.maxGroups
		) {
			this._pendingSpill = this.spillPartial();
		}

		recycleChunk(chunk);
		return ok(opEmpty());
	}

	/** Pending spill promise (if any) */
	private _pendingSpill: Promise<void> | null = null;

	finish(): Result<OperatorResult> | Promise<Result<OperatorResult>> {
		// If spill files exist, need async merge
		if (this.spillFiles.length > 0 || this._pendingSpill) {
			return this.finishAsync();
		}

		if (this.finished) {
			return ok(opDone());
		}
		this.finished = true;

		return ok(this.buildResult());
	}

	/** Build the result chunk from current in-memory state */
	private buildResult(): OperatorResult {
		const groupCount = this.nextGroupId;
		if (groupCount === 0) {
			return opDone();
		}

		const columns: ColumnBuffer[] = [];

		for (let k = 0; k < this.keyColumns.length; k++) {
			columns.push(this.groupKeyCols[k]!);
		}

		for (const agg of this.aggregators) {
			columns.push(agg.finish());
		}

		const resultChunk = new Chunk(this.outputSchema, columns, this.dictionary);
		return opResult(resultChunk, true);
	}

	/** Spill partial aggregate results to disk and reset state */
	private async spillPartial(): Promise<void> {
		if (this.nextGroupId === 0) return;

		// Create temp dir on first spill
		if (!this.spillDir) {
			this.spillDir = await fs.mkdtemp(
				path.join(os.tmpdir(), "molniya-groupby-"),
			);
		}

		const result = this.buildResult();
		if (!result.chunk) return;

		// Write partial result to MBF file
		const spillPath = path.join(
			this.spillDir,
			`partial-${this.spillFiles.length}.mbf`,
		);
		const writer = new BinaryWriter(spillPath, this.outputSchema);
		await writer.open();
		try {
			await writer.writeChunk(result.chunk);
		} finally {
			await writer.close();
		}

		this.spillFiles.push(spillPath);

		// Reset state for next batch
		this.resetState();
	}

	/** Async finish path: merge spilled partial results */
	private async finishAsync(): Promise<Result<OperatorResult>> {
		// Wait for any pending spill
		if (this._pendingSpill) {
			await this._pendingSpill;
			this._pendingSpill = null;
		}

		this.finished = true;

		// Build final partial from remaining in-memory state
		const finalPartial = this.buildResult();

		// Read all spill files
		const allChunks: Chunk[] = [];
		for (const file of this.spillFiles) {
			const reader = new BinaryReader(file);
			await reader.open();
			for await (const chunk of reader.scan()) {
				allChunks.push(chunk);
			}
			await reader.close();
		}
		if (finalPartial.chunk) {
			allChunks.push(finalPartial.chunk);
		}

		// Clean up spill files
		await this.cleanup();

		if (allChunks.length === 0) {
			return ok(opDone());
		}

		// Re-aggregate using mapped merge types
		const mergeResult = this.mergePartials(allChunks);
		return ok(opResult(mergeResult, true));
	}

	/** Merge partial aggregate chunks using hash-based re-aggregation */
	private mergePartials(partials: Chunk[]): Chunk {
		const mergeDict = createDictionary();
		const mergeGroups = new KeyHashTable();

		const mergeGroupKeyCols: ColumnBuffer[] = this.keyColumns.map((_idx, k) => {
			const dtype = this.outputSchema.columns[k]!.dtype;
			if (dtype.kind === DTypeKind.StringView) {
				return new StringViewColumnBuffer(1024, dtype.nullable) as unknown as ColumnBuffer;
			}
			return new ColumnBuffer(dtype.kind, 1024, dtype.nullable);
		});
		let mergeNextGroupId = 0;

		// Create merge aggregators with mapped types
		const mergeAggs: BatchAggregator[] = this.aggSpecs.map((spec) => {
			const mergeType = MERGE_AGG_TYPE[spec.aggType] ?? spec.aggType;
			return createVectorAggregator(mergeType);
		});

		const keyBuffer = new Array(this.keyColumns.length) as (number | bigint | Uint8Array | null)[];

		for (const chunk of partials) {
			const rowCount = chunk.rowCount;
			const cols = chunk.getColumns();
			const selection = chunk.getSelection();
			const chunkGroupIds = new Int32Array(rowCount);

			// Key columns are the first N columns in the output schema
			for (let row = 0; row < rowCount; row++) {
				const physIdx = selection ? selection[row]! : row;

				for (let k = 0; k < this.keyColumns.length; k++) {
					const col = cols[k]!;
					let val: number | bigint | Uint8Array | null;

					if (col.kind === DTypeKind.StringView) {
						const bytes = (col as unknown as import("../buffer/string-view-column.ts").StringViewColumnBuffer).getBytes(physIdx);
						val = bytes ?? null;
					} else {
						val = col.get(physIdx) as number | bigint | null;

						if (col.kind === DTypeKind.String && chunk.dictionary) {
							const id = val as number;
							const bytes = chunk.dictionary.getBytes(id);
							if (bytes) {
								val = mergeDict.intern(bytes);
							} else {
								val = null;
							}
						}
					}
					keyBuffer[k] = val;
				}

				const hash = hashKey(keyBuffer);
				const keysEqual = (gid: number) => {
					for (let k = 0; k < this.keyColumns.length; k++) {
						const col = mergeGroupKeyCols[k]!;
						const v = keyBuffer[k];
						if (v === null) {
							if (!col.isNull(gid)) return false;
						} else {
							if (col.isNull(gid)) return false;
							if (v instanceof Uint8Array) {
								const bytes = (col as unknown as StringViewColumnBuffer).getBytes(gid);
								if (!bytes || bytes.length !== v.length) return false;
								for (let j = 0; j < v.length; j++) {
									if (bytes[j] !== v[j]) return false;
								}
							} else {
								if (col.get(gid) !== v) return false;
							}
						}
					}
					return true;
				};

				let gid = mergeGroups.getOrInsert(hash, keysEqual, true, mergeNextGroupId);
				if (gid === mergeNextGroupId) {
					mergeNextGroupId++;
					for (let k = 0; k < this.keyColumns.length; k++) {
						const col = mergeGroupKeyCols[k]!;
						const v = keyBuffer[k];
						if (v === null) {
							col.appendNull();
						} else if (v instanceof Uint8Array) {
							(col as unknown as StringViewColumnBuffer).appendBytes(v, 0, v.length);
						} else {
							col.append(v as never);
						}
					}
				}

				chunkGroupIds[row] = gid;
			}

			for (const agg of mergeAggs) {
				agg.resize(mergeNextGroupId);
			}

			// Aggregate value columns (offset after key columns)
			const keyCount = this.keyColumns.length;
			for (let i = 0; i < this.aggSpecs.length; i++) {
				const agg = mergeAggs[i]!;
				const valCol = cols[keyCount + i]!;
				agg.accumulateBatch(
					valCol.data,
					chunkGroupIds,
					rowCount,
					selection,
					valCol,
				);
			}
		}

		// Build merged result
		const groupCount = mergeNextGroupId;
		const columns: ColumnBuffer[] = [];

		for (let k = 0; k < this.keyColumns.length; k++) {
			columns.push(mergeGroupKeyCols[k]!);
		}

		for (const agg of mergeAggs) {
			columns.push(agg.finish());
		}

		return new Chunk(this.outputSchema, columns, mergeDict);
	}

	/** Reset aggregation state without resetting spill state */
	private resetState(): void {
		this.groups.clear();
		this.nextGroupId = 0;
		this.groupKeyCols = this.keyColumns.map((cIdx) => {
			const dtype = this.inputSchema.columns[cIdx]!.dtype;
			if (dtype.kind === DTypeKind.StringView) {
				return new StringViewColumnBuffer(1024, dtype.nullable) as unknown as ColumnBuffer;
			}
			return new ColumnBuffer(dtype.kind, 1024, dtype.nullable);
		});
		// Reuse existing aggregator objects — call resetState() to zero arrays
		// without discarding and reallocating them (avoids GC churn per spill cycle).
		for (const agg of this.aggregators) {
			agg.resetState();
		}
	}

	/** Clean up spill files */
	async cleanup(): Promise<void> {
		for (const file of this.spillFiles) {
			try {
				await fs.unlink(file);
			} catch {
				// Ignore cleanup errors
			}
		}
		this.spillFiles = [];
		if (this.spillDir) {
			try {
				await fs.rmdir(this.spillDir);
			} catch {
				// Ignore
			}
			this.spillDir = null;
		}
	}

	reset(): void {
		this.finished = false;
		this.resetState();
		this.cleanup();
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

	return ok({ aggType, valueExpr, innerExpr, inputDType, isCountAll });
}

/**
 * Create a GroupBy operator.
 */
export function groupBy(
	inputSchema: Schema,
	keyColumns: string[],
	aggSpecs: AggSpec[],
	options?: GroupByOptions,
): Result<GroupByOperator> {
	return GroupByOperator.create(inputSchema, keyColumns, aggSpecs, options);
}
