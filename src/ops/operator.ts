/**
 * Operator interface and base types.
 *
 * Operators are the building blocks of the execution pipeline.
 * Each operator transforms a Chunk and passes it downstream.
 *
 * Key principles:
 * - Operators work on Chunks (batches of rows)
 * - Selection vectors avoid copying data for filters
 * - Operators are stateless where possible
 * - Backpressure is handled by the executor, not operators
 */

import type { Chunk } from "../buffer/chunk.ts";
import { ok, type Result } from "../types/error.ts";
import type { Schema } from "../types/schema.ts";

/**
 * Operator execution result.
 * Contains the transformed chunk and signals for flow control.
 */
export interface OperatorResult {
	/** The transformed chunk (may be same instance with selection applied) */
	chunk: Chunk | null;

	/** True if this operator has more output to produce */
	hasMore: boolean;

	/** True if downstream should stop pulling (e.g., limit reached) */
	done: boolean;
}

/**
 * Operator interface.
 *
 * Operators transform input chunks to output chunks.
 * They can be:
 * - 1:1 (filter, project, transform)
 * - 1:N (explode, unnest)
 * - N:1 (aggregate, sort - need to buffer)
 */
export interface Operator {
	/** Human-readable name for debugging */
	readonly name: string;

	/** Output schema after transformation */
	readonly outputSchema: Schema;

	/**
	 * Process an input chunk.
	 * Returns the transformed result, or a Promise when the operator
	 * needs to perform I/O before it can accept new rows (e.g., GroupBy spill).
	 */
	process(chunk: Chunk): Result<OperatorResult> | Promise<Result<OperatorResult>>;

	/**
	 * Signal end of input.
	 * Operators that buffer (aggregate, sort) return final results here.
	 * May return a Promise for operators that need async I/O (e.g., external sort).
	 */
	finish(): Result<OperatorResult> | Promise<Result<OperatorResult>>;

	/**
	 * Reset operator state for reuse.
	 */
	reset(): void;
}

/**
 * Base class for simple 1:1 operators that don't buffer.
 */
export abstract class SimpleOperator implements Operator {
	abstract readonly name: string;
	abstract readonly outputSchema: Schema;

	abstract process(chunk: Chunk): Result<OperatorResult>;

	finish(): Result<OperatorResult> {
		return ok(opEmpty());
	}

	reset(): void {
		// No state to reset in simple operators
	}
}

/**
 * Create a successful operator result with a chunk.
 */
export function opResult(chunk: Chunk, done: boolean = false): OperatorResult {
	return { chunk, hasMore: false, done };
}

/**
 * Create an empty operator result (no output for this input).
 */
export function opEmpty(): OperatorResult {
	return { chunk: null, hasMore: false, done: false };
}

/**
 * Create a done result (operator has finished, no more output).
 */
export function opDone(): OperatorResult {
	return { chunk: null, hasMore: false, done: true };
}

/**
 * Operator that passes chunks through unchanged.
 * Useful as a base or for testing.
 */
export class PassthroughOperator extends SimpleOperator {
	readonly name = "Passthrough";
	readonly outputSchema: Schema;

	constructor(inputSchema: Schema) {
		super();
		this.outputSchema = inputSchema;
	}

	process(chunk: Chunk): Result<OperatorResult> {
		return ok(opResult(chunk));
	}
}
