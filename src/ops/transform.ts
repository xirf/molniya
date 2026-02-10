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

	private readonly computedColumns: readonly CompiledColumn[];

	private constructor(
		outputSchema: Schema,
		computedColumns: CompiledColumn[],
		_maxChunkSize: number,
	) {
		super();
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
				new TransformOperator(inputSchema, [], maxChunkSize),
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

		// Get physical row count (length of first column buffer)
		const inputColumns = chunk.getColumns();
		const physicalRowCount = inputColumns[0]?.length ?? 0;
		const selection = chunk.getSelection();

		const newColumns: ColumnBuffer[] = [...inputColumns];

		// Compute new columns
		for (const computed of this.computedColumns) {
			// Create buffer with physical capacity
			const buffer = createColumnForDType(computed.dtype, physicalRowCount);
			const isString = computed.dtype.kind === DTypeKind.String;

			// Initialize buffer length to physical row count
			// We will sparsely populate it if there is a selection
			buffer.setLength(physicalRowCount);

			if (selection) {
				// Sparse population matching selection
				const selectedCount = chunk.rowCount;
				for (let i = 0; i < selectedCount; i++) {
					const physicalIndex = selection[i]!;
					// Compute using logical index (chunk + i)
					const value = computed.compute(chunk, i);

					if (value === null) {
						buffer.setNull(physicalIndex, true);
					} else if (isString && typeof value === "string") {
						const dictIndex = chunk.dictionary?.internString(value) ?? 0;
						buffer.set(physicalIndex, dictIndex as never);
					} else if (typeof value === "bigint") {
						buffer.set(physicalIndex, value as never);
					} else if (typeof value === "boolean") {
						buffer.set(physicalIndex, (value ? 1 : 0) as never);
					} else {
						buffer.set(physicalIndex, value as never);
					}
				}
			} else {
				// Dense population (all rows)
				const rowCount = chunk.rowCount;
				for (let i = 0; i < rowCount; i++) {
					const value = computed.compute(chunk, i);

					if (value === null) {
						buffer.setNull(i, true);
					} else if (isString && typeof value === "string") {
						const dictIndex = chunk.dictionary?.internString(value) ?? 0;
						buffer.set(i, dictIndex as never);
					} else if (typeof value === "bigint") {
						buffer.set(i, value as never);
					} else if (typeof value === "boolean") {
						buffer.set(i, (value ? 1 : 0) as never);
					} else {
						buffer.set(i, value as never);
					}
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

		// Re-apply selection if it existed
		if (selection) {
			resultChunk.applySelection(selection, chunk.rowCount);
		}

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
