/**
 * Transform operator.
 *
 * Computes new columns from expressions.
 * Used for withColumn operations.
 */

import { Chunk } from "../buffer/chunk.ts";
import { ColumnBuffer } from "../buffer/column-buffer.ts";
import type { Expr } from "../expr/ast.ts";
import { type CompiledValue, compileValue } from "../expr/compiler.ts";
import { inferExprType } from "../expr/types.ts";
import { DTypeKind, type DType } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import { addColumn, type Schema } from "../types/schema.ts";
import {
	type OperatorResult,
	opEmpty,
	opResult,
	SimpleOperator,
} from "./operator.ts";

/** Specification for a computed column */
export interface ComputedColumn {
	/** Name for the new column */
	name: string;
	/** Expression to compute the column value */
	expr: Expr;
}

/**
 * Transform operator that adds computed columns.
 */
export class TransformOperator extends SimpleOperator {
	readonly name = "Transform";
	readonly outputSchema: Schema;

	private readonly inputSchema: Schema;
	private readonly computedColumns: readonly CompiledColumn[];

	private constructor(
		inputSchema: Schema,
		outputSchema: Schema,
		computedColumns: CompiledColumn[],
		_maxChunkSize: number,
	) {
		super();
		this.inputSchema = inputSchema;
		this.outputSchema = outputSchema;
		this.computedColumns = computedColumns;
	}

	/**
	 * Create a transform operator for adding computed columns.
	 */
	static create(
		inputSchema: Schema,
		columns: ComputedColumn[],
		maxChunkSize: number = 65536,
	): Result<TransformOperator> {
		if (columns.length === 0) {
			// No columns to add - could just use passthrough
			return ok(
				new TransformOperator(inputSchema, inputSchema, [], maxChunkSize),
			);
		}

		const compiledColumns: CompiledColumn[] = [];
		let currentSchema = inputSchema;

		for (const col of columns) {
			// Infer the output type
			const typeResult = inferExprType(col.expr, inputSchema);
			if (typeResult.error !== ErrorCode.None) {
				return err(typeResult.error);
			}

			// Compile the value expression
			const valueResult = compileValue(col.expr, inputSchema);
			if (valueResult.error !== ErrorCode.None) {
				return err(valueResult.error);
			}

			// Add column to schema
			const schemaResult = addColumn(
				currentSchema,
				col.name,
				typeResult.value.dtype,
			);
			if (schemaResult.error !== ErrorCode.None) {
				return err(schemaResult.error);
			}
			currentSchema = schemaResult.value;

			compiledColumns.push({
				name: col.name,
				dtype: typeResult.value.dtype,
				compute: valueResult.value,
			});
		}

		return ok(
			new TransformOperator(
				inputSchema,
				currentSchema,
				compiledColumns,
				maxChunkSize,
			),
		);
	}

	process(chunk: Chunk): Result<OperatorResult> {
		if (chunk.rowCount === 0) {
			return ok(opEmpty());
		}

		if (this.computedColumns.length === 0) {
			// No columns to add
			return ok(opResult(chunk));
		}

		// Get existing columns
		const newColumns: ColumnBuffer[] = [];
		for (let i = 0; i < this.inputSchema.columnCount; i++) {
			const col = chunk.getColumn(i);
			if (col === undefined) {
				return err(ErrorCode.InvalidOffset);
			}
			newColumns.push(col);
		}

		// Materialize chunk if it has a selection (we need actual indices)
		let workingChunk = chunk;
		if (chunk.hasSelection()) {
			const materializeResult = chunk.materialize();
			if (materializeResult.error !== ErrorCode.None) {
				return err(materializeResult.error);
			}
			workingChunk = materializeResult.value;

			// Re-get columns from materialized chunk
			newColumns.length = 0;
			for (let i = 0; i < this.inputSchema.columnCount; i++) {
				const col = workingChunk.getColumn(i);
				if (col === undefined) {
					return err(ErrorCode.InvalidOffset);
				}
				newColumns.push(col);
			}
		}

		const rowCount = workingChunk.rowCount;

		// Compute new columns
		for (const computed of this.computedColumns) {
			const buffer = createColumnForDType(computed.dtype, rowCount);
			const isString = computed.dtype.kind === DTypeKind.String;

			// Evaluate expression for each row
			for (let i = 0; i < rowCount; i++) {
				const value = computed.compute(workingChunk, i);
				if (value === null) {
					buffer.appendNull();
				} else if (isString && typeof value === "string") {
					// Intern string into dictionary and store dict index
					const dictIndex = workingChunk.dictionary?.internString(value) ?? 0;
					buffer.append(dictIndex as never);
				} else if (typeof value === "bigint") {
					// For bigint, need to handle based on buffer type
					buffer.append(value as never);
				} else if (typeof value === "boolean") {
					buffer.append((value ? 1 : 0) as never);
				} else {
					buffer.append(value as never);
				}
			}

			newColumns.push(buffer);
		}

		// Create new chunk with all columns
		const resultChunk = new Chunk(
			this.outputSchema,
			newColumns,
			chunk.dictionary,
		);

		return ok(opResult(resultChunk));
	}
}

/** Internal compiled column representation */
interface CompiledColumn {
	name: string;
	dtype: DType;
	compute: CompiledValue;
}

/** Create a column buffer for a specific DType */
function createColumnForDType(dtype: DType, capacity: number): ColumnBuffer {
	return new ColumnBuffer(dtype.kind, capacity, dtype.nullable);
}

/**
 * Create a transform operator for adding computed columns.
 */
export function transform(
	inputSchema: Schema,
	columns: ComputedColumn[],
	maxChunkSize?: number,
): Result<TransformOperator> {
	return TransformOperator.create(inputSchema, columns, maxChunkSize);
}

/**
 * Create a single computed column specification.
 */
export function withColumn(name: string, expr: Expr): ComputedColumn {
	return { name, expr };
}
