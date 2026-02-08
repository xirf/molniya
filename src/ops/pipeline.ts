/** biome-ignore-all lint/style/noNonNullAssertion: Performance optimization */
/**
 * Pipeline executor.
 *
 * Executes a chain of operators on a stream of chunks.
 * Supports both pull-based and push-based execution.
 */

import type { Chunk } from "../buffer/chunk.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import type { Schema } from "../types/schema.ts";
import type { Operator } from "./operator.ts";

/**
 * Pipeline execution result.
 */
export interface PipelineResult {
	/** Output chunks produced */
	chunks: Chunk[];

	/** Total rows processed */
	rowsIn: number;

	/** Total rows output */
	rowsOut: number;

	/** Execution time in milliseconds */
	timeMs: number;
}

/**
 * Pipeline executor that chains operators.
 */
export class Pipeline {
	private readonly operators: Operator[];
	private readonly outputSchema: Schema;

	constructor(operators: Operator[]) {
		if (operators.length === 0) {
			throw new Error("Pipeline must have at least one operator");
		}
		this.operators = operators;
		// If no operators, use empty schema or throw (assuming input schema determines output if empty)
		// For now, assert that we have at least one operator or handle effectively
		const lastOp = operators[operators.length - 1];
		if (!lastOp) {
			throw new Error("Pipeline must have at least one operator");
		}
		this.outputSchema = lastOp.outputSchema;
	}

	/**
	 * Get the output schema of the pipeline.
	 */
	get schema(): Schema {
		return this.outputSchema;
	}

	/**
	 * Execute the pipeline on a single chunk.
	 * Returns the output chunks and whether pipeline is done.
	 */
	executeChunk(chunk: Chunk): Result<{ chunks: Chunk[]; done: boolean }> {
		const outputs: Chunk[] = [];
		let current: Chunk | null = chunk;
		let pipelineDone = false;

		for (const op of this.operators) {
			if (current === null) {
				break;
			}

			const result = op.process(current);
			if (result.error !== ErrorCode.None) {
				return err(result.error);
			}

			current = result.value.chunk;

			// If operator signals done, pipeline should stop after this chunk
			if (result.value.done) {
				pipelineDone = true;
			}
		}

		if (current !== null) {
			outputs.push(current);
		}

		return ok({ chunks: outputs, done: pipelineDone });
	}

	/**
	 * Execute the pipeline on multiple chunks (streaming).
	 */
	execute(chunks: Iterable<Chunk>): Result<PipelineResult> {
		const startTime = performance.now();
		const outputChunks: Chunk[] = [];
		let rowsIn = 0;
		let rowsOut = 0;
		let pipelineDone = false;

		// Process all input chunks
		for (const chunk of chunks) {
			if (pipelineDone) break;

			rowsIn += chunk.rowCount;

			const result = this.executeChunk(chunk);
			if (result.error !== ErrorCode.None) {
				return err(result.error);
			}

			for (const outChunk of result.value.chunks) {
				rowsOut += outChunk.rowCount;
				outputChunks.push(outChunk);
			}

			pipelineDone = result.value.done;
		}

		// Finish all operators (for buffering operators like aggregate, sort)
		// When a buffering operator returns a chunk in finish(), we need to
		// pass it through the remaining downstream operators.
		for (let i = 0; i < this.operators.length; i++) {
			const op = this.operators[i]!;
			const result = op.finish();
			if (result.error !== ErrorCode.None) {
				return err(result.error);
			}

			if (result.value.chunk !== null) {
				// Pass the finish output through remaining operators
				let chunk: Chunk | null = result.value.chunk;
				for (let j = i + 1; j < this.operators.length && chunk !== null; j++) {
					const downstream = this.operators[j]!;
					const downstreamResult = downstream.process(chunk);
					if (downstreamResult.error !== ErrorCode.None) {
						return err(downstreamResult.error);
					}
					chunk = downstreamResult.value.chunk;
					if (downstreamResult.value.done) {
						pipelineDone = true;
					}
				}

				if (chunk !== null) {
					rowsOut += chunk.rowCount;
					outputChunks.push(chunk);
				}
			}
		}

		return ok({
			chunks: outputChunks,
			rowsIn,
			rowsOut,
			timeMs: performance.now() - startTime,
		});
	}

	/**
	 * Execute pipeline on async chunk stream.
	 */
	async executeAsync(
		chunks: AsyncIterable<Chunk>,
	): Promise<Result<PipelineResult>> {
		const startTime = performance.now();
		const outputChunks: Chunk[] = [];
		let rowsIn = 0;
		let rowsOut = 0;
		let pipelineDone = false;

		for await (const chunk of chunks) {
			if (pipelineDone) break;

			rowsIn += chunk.rowCount;

			const result = this.executeChunk(chunk);
			if (result.error !== ErrorCode.None) {
				return err(result.error);
			}

			for (const outChunk of result.value.chunks) {
				rowsOut += outChunk.rowCount;
				outputChunks.push(outChunk);
			}

			pipelineDone = result.value.done;
		}

		// Finish all operators (for buffering operators like aggregate, sort)
		for (let i = 0; i < this.operators.length; i++) {
			const op = this.operators[i]!;
			const result = op.finish();
			if (result.error !== ErrorCode.None) {
				return err(result.error);
			}

			if (result.value.chunk !== null) {
				// Pass the finish output through remaining operators
				let chunk: Chunk | null = result.value.chunk;
				for (let j = i + 1; j < this.operators.length && chunk !== null; j++) {
					const downstream = this.operators[j]!;
					const downstreamResult = downstream.process(chunk);
					if (downstreamResult.error !== ErrorCode.None) {
						return err(downstreamResult.error);
					}
					chunk = downstreamResult.value.chunk;
					if (downstreamResult.value.done) {
						pipelineDone = true;
					}
				}

				if (chunk !== null) {
					rowsOut += chunk.rowCount;
					outputChunks.push(chunk);
				}
			}
		}

		return ok({
			chunks: outputChunks,
			rowsIn,
			rowsOut,
			timeMs: performance.now() - startTime,
		});
	}

	/**
	 * Execute pipeline and stream results (generator).
	 */
	*stream(chunks: Iterable<Chunk>): Generator<Chunk> {
		for (const chunk of chunks) {
			const result = this.executeChunk(chunk);
			// If error, we should throw or handle? Generator can throw.
			if (result.error !== ErrorCode.None) {
				throw new Error(`Pipeline error: ${result.error}`);
			}

			if (result.value.chunks.length > 0) {
				for (const outChunk of result.value.chunks) {
					yield outChunk;
				}
			}

			if (result.value.done) break;
		}

		// Finish operators
		for (let i = 0; i < this.operators.length; i++) {
			const op = this.operators[i]!;
			const result = op.finish();
			if (result.error !== ErrorCode.None) {
				throw new Error(`Pipeline error: ${result.error}`);
			}

			if (result.value.chunk !== null) {
				let chunk: Chunk | null = result.value.chunk;
				// Pass through remaining operators
				for (let j = i + 1; j < this.operators.length && chunk !== null; j++) {
					const downstream = this.operators[j]!;
					const downstreamResult = downstream.process(chunk);
					if (downstreamResult.error !== ErrorCode.None) {
						throw new Error(`Pipeline error: ${downstreamResult.error}`);
					}
					chunk = downstreamResult.value.chunk;
				}

				if (chunk !== null) {
					yield chunk;
				}
			}
		}
	}

	/**
	 * Execute pipeline on async stream (async generator).
	 */
	async *streamAsync(chunks: AsyncIterable<Chunk>): AsyncGenerator<Chunk> {
		for await (const chunk of chunks) {
			const result = this.executeChunk(chunk);
			if (result.error !== ErrorCode.None) {
				throw new Error(`Pipeline error: ${result.error}`);
			}

			if (result.value.chunks.length > 0) {
				for (const outChunk of result.value.chunks) {
					yield outChunk;
				}
			}

			if (result.value.done) break;
		}

		// Finish operators
		for (let i = 0; i < this.operators.length; i++) {
			const op = this.operators[i]!;
			const result = op.finish();
			if (result.error !== ErrorCode.None) {
				throw new Error(`Pipeline error: ${result.error}`);
			}

			if (result.value.chunk !== null) {
				let chunk: Chunk | null = result.value.chunk;
				// Pass through remaining operators
				for (let j = i + 1; j < this.operators.length && chunk !== null; j++) {
					const downstream = this.operators[j]!;
					const downstreamResult = downstream.process(chunk);
					if (downstreamResult.error !== ErrorCode.None) {
						throw new Error(`Pipeline error: ${downstreamResult.error}`);
					}
					chunk = downstreamResult.value.chunk;
				}

				if (chunk !== null) {
					yield chunk;
				}
			}
		}
	}

	/**
	 * Reset all operators for reuse.
	 */
	reset(): void {
		for (const op of this.operators) {
			op.reset();
		}
	}
}

/**
 * Create a pipeline from operators.
 */
export function pipeline(...operators: Operator[]): Pipeline {
	return new Pipeline(operators);
}
