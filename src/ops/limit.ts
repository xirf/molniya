/**
 * Limit operator.
 *
 * Limits the number of rows passing through.
 * Signals done when limit is reached for early termination.
 */

import type { Chunk } from "../buffer/chunk.ts";
import { selectionPool } from "../buffer/selection-pool.ts";
import { recycleChunk } from "../buffer/pool.ts";
import { ok, type Result } from "../types/error.ts";
import type { Schema } from "../types/schema.ts";
import {
	type Operator,
	type OperatorResult,
	opDone,
	opEmpty,
} from "./operator.ts";

/**
 * Limit operator that restricts row count.
 */
export class LimitOperator implements Operator {
	readonly name = "Limit";
	readonly outputSchema: Schema;

	private readonly limit: number;
	private readonly offset: number;
	private rowsSkipped: number = 0;
	private rowsPassed: number = 0;

	constructor(schema: Schema, limit: number, offset: number = 0) {
		this.outputSchema = schema;
		this.limit = limit;
		this.offset = offset;
	}

	process(inputChunk: Chunk): Result<OperatorResult> {
		// Already reached limit
		if (this.rowsPassed >= this.limit) {
			recycleChunk(inputChunk);
			return ok(opDone());
		}

		let chunk = inputChunk;
		const rowCount = chunk.rowCount;
		if (rowCount === 0) {
			recycleChunk(chunk);
			return ok(opEmpty());
		}

		// Handle offset (skip rows)
		if (this.rowsSkipped < this.offset) {
			const toSkip = Math.min(rowCount, this.offset - this.rowsSkipped);
			this.rowsSkipped += toSkip;

			if (toSkip === rowCount) {
				// Skip entire chunk
				recycleChunk(chunk);
				return ok(opEmpty());
			}

			// Partial skip - create selection for remaining rows (non-mutating)
			const remaining = rowCount - toSkip;
			const selection = selectionPool.acquire(remaining);
			for (let i = 0; i < remaining; i++) {
				selection[i] = toSkip + i;
			}
			chunk = chunk.withSelection(selection, remaining);
			selectionPool.release(selection);
		}

		// Check how many rows we can still pass
		const canPass = this.limit - this.rowsPassed;
		const currentRows = chunk.rowCount;

		if (currentRows <= canPass) {
			// Can pass entire chunk
			this.rowsPassed += currentRows;
			const done = this.rowsPassed >= this.limit;
			return ok({ chunk, hasMore: false, done });
		}

		// Need to limit this chunk (non-mutating)
		const selection = selectionPool.acquire(canPass);
		for (let i = 0; i < canPass; i++) {
			selection[i] = i;
		}
		const limitedChunk = chunk.withSelection(selection, canPass);
		selectionPool.release(selection);

		this.rowsPassed += canPass;
		return ok({ chunk: limitedChunk, hasMore: false, done: true });
	}

	finish(): Result<OperatorResult> {
		return ok(opDone());
	}

	reset(): void {
		this.rowsSkipped = 0;
		this.rowsPassed = 0;
	}
}

/**
 * Create a limit operator.
 */
export function limit(
	schema: Schema,
	count: number,
	offset?: number,
): LimitOperator {
	return new LimitOperator(schema, count, offset);
}
