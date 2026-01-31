/**
 * Filter operator.
 *
 * Applies a predicate to filter rows.
 * Uses selection vectors to avoid copying data.
 */

import type { Chunk } from "../buffer/chunk.ts";
import { selectionPool } from "../buffer/selection-pool.ts";
import type { Expr } from "../expr/ast.ts";
import {
	applyPredicate,
	type CompiledPredicate,
	compilePredicate,
} from "../expr/compiler.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import type { Schema } from "../types/schema.ts";
import {
	type OperatorResult,
	opEmpty,
	opResult,
	SimpleOperator,
} from "./operator.ts";

/**
 * Filter operator that applies a predicate expression.
 */
export class FilterOperator extends SimpleOperator {
	readonly name = "Filter";
	readonly outputSchema: Schema;

	private readonly predicate: CompiledPredicate;
	private selectionBuffer: Uint32Array;
	private readonly maxChunkSize: number;

	private constructor(
		schema: Schema,
		predicate: CompiledPredicate,
		maxChunkSize: number,
	) {
		super();
		this.outputSchema = schema;
		this.predicate = predicate;
		this.maxChunkSize = maxChunkSize;
		// Acquire buffer from pool instead of allocating
		this.selectionBuffer = selectionPool.acquire(maxChunkSize);
	}

	/**
	 * Create a filter operator from an expression.
	 */
	static create(
		schema: Schema,
		expr: Expr,
		maxChunkSize: number = 65536,
	): Result<FilterOperator> {
		const predicateResult = compilePredicate(expr, schema);
		if (predicateResult.error !== ErrorCode.None) {
			return err(predicateResult.error);
		}

		return ok(new FilterOperator(schema, predicateResult.value, maxChunkSize));
	}

	/**
	 * Create a filter operator from a pre-compiled predicate.
	 */
	static fromPredicate(
		schema: Schema,
		predicate: CompiledPredicate,
		maxChunkSize: number = 65536,
	): FilterOperator {
		return new FilterOperator(schema, predicate, maxChunkSize);
	}

	process(chunk: Chunk): Result<OperatorResult> {
		const rowCount = chunk.rowCount;

		if (rowCount === 0) {
			return ok(opEmpty());
		}

		// Apply predicate to generate selection vector
		const selectedCount = applyPredicate(
			this.predicate,
			chunk,
			this.selectionBuffer,
		);

		if (selectedCount === 0) {
			// No rows matched
			return ok(opEmpty());
		}

		if (selectedCount === rowCount && !chunk.hasSelection()) {
			// All rows matched and no existing selection - pass through
			return ok(opResult(chunk));
		}

		// Apply the new selection to the chunk
		// Instead of slicing (which allocates), we release the old buffer
		// and acquire a new one of the exact size needed
		const selection = this.selectionBuffer.subarray(0, selectedCount);
		chunk.applySelection(selection, selectedCount);

		// Release the buffer back to pool and acquire fresh one for next chunk
		selectionPool.release(this.selectionBuffer);
		this.selectionBuffer = selectionPool.acquire(this.maxChunkSize);

		return ok(opResult(chunk));
	}
}

/**
 * Create a filter operator from an expression.
 */
export function filter(
	schema: Schema,
	expr: Expr,
	maxChunkSize?: number,
): Result<FilterOperator> {
	return FilterOperator.create(schema, expr, maxChunkSize);
}
