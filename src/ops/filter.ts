/**
 * Filter operator.
 *
 * Applies a predicate to filter rows.
 * Uses selection vectors to avoid copying data.
 * Supports vectorized predicates for 10-50x faster evaluation.
 */

import type { Chunk } from "../buffer/chunk.ts";
import { selectionPool } from "../buffer/selection-pool.ts";
import { recycleChunk } from "../buffer/pool.ts";
import type { Expr } from "../expr/ast.ts";
import {
	applyPredicate,
	type CompiledPredicate,
	compilePredicate,
	compileVectorized,
	type VectorizedPredicate,
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
 * Uses vectorized evaluation when available for maximum throughput.
 */
export class FilterOperator extends SimpleOperator {
	readonly name = "Filter";
	readonly outputSchema: Schema;

	private readonly predicate: CompiledPredicate;
	private readonly vectorized: VectorizedPredicate | undefined;
	private selectionBuffer: Uint32Array | null = null;
	private readonly maxChunkSize: number;
	/** Original expression AST (used by optimizer for filter fusion) */
	readonly expr: Expr | null;

	private constructor(
		schema: Schema,
		predicate: CompiledPredicate,
		maxChunkSize: number,
		vectorized?: VectorizedPredicate,
		expr?: Expr,
	) {
		super();
		this.outputSchema = schema;
		this.predicate = predicate;
		this.vectorized = vectorized;
		this.maxChunkSize = maxChunkSize;
		this.expr = expr ?? null;
	}

	/**
	 * Create a filter operator from an expression.
	 * Automatically compiles vectorized fast path when possible.
	 */
	static create(
		schema: Schema,
		expr: Expr,
		maxChunkSize: number = 32768,
	): Result<FilterOperator> {
		const predicateResult = compilePredicate(expr, schema);
		if (predicateResult.error !== ErrorCode.None) {
			return err(predicateResult.error);
		}

		// Try to compile vectorized version (returns null if not vectorizable)
		const vectorized = compileVectorized(expr, schema) ?? undefined;

		return ok(new FilterOperator(schema, predicateResult.value, maxChunkSize, vectorized, expr));
	}

	/**
	 * Create a filter operator from a pre-compiled predicate.
	 */
	static fromPredicate(
		schema: Schema,
		predicate: CompiledPredicate,
		maxChunkSize: number = 32768,
	): FilterOperator {
		return new FilterOperator(schema, predicate, maxChunkSize);
	}

	process(chunk: Chunk): Result<OperatorResult> {
		const rowCount = chunk.rowCount;

		if (rowCount === 0) {
			return ok(opEmpty());
		}

		// Lazily acquire buffer only when this operator actually processes data.
		// This prevents large transient allocations when many short-lived filter
		// operators are created in benchmark loops.
		if (this.selectionBuffer === null || this.selectionBuffer.length < this.maxChunkSize) {
			this.selectionBuffer = selectionPool.acquire(this.maxChunkSize);
		}

		// Apply predicate to generate selection vector
		// Uses vectorized path if available (10-50x faster for simple predicates)
		const selectedCount = applyPredicate(
			this.predicate,
			chunk,
			this.selectionBuffer,
			this.vectorized,
		);

		if (selectedCount === 0) {
			// No rows matched
			recycleChunk(chunk);
			return ok(opEmpty());
		}

		if (selectedCount === rowCount && !chunk.hasSelection()) {
			// All rows matched and no existing selection - pass through
			return ok(opResult(chunk));
		}

		// Zero-copy: borrow the selection buffer instead of copying it.
		// The release callback returns the buffer to the pool and acquires
		// a fresh one for the next chunk only when needed.
		const currentBuffer = this.selectionBuffer;
		const filteredChunk = chunk.withSelectionBorrowed(
			currentBuffer,
			selectedCount,
			() => {
				selectionPool.release(currentBuffer);
			},
		);

		// Acquire a new buffer for the next chunk processing
		this.selectionBuffer = selectionPool.acquire(this.maxChunkSize);

		return ok(opResult(filteredChunk));
	}

	override reset(): void {
		if (this.selectionBuffer !== null) {
			selectionPool.release(this.selectionBuffer);
			this.selectionBuffer = null;
		}
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
