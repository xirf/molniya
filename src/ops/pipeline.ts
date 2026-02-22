/** biome-ignore-all lint/style/noNonNullAssertion: Performance optimization */
/**
 * Pipeline executor.
 *
 * Executes a chain of operators on a stream of chunks.
 * All pipeline execution paths are async to support operators whose
 * process() method may return a Promise (e.g., GroupBy during disk spill).
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
 * All execution methods are async to uniformly support operators that
 * may perform I/O during process() (e.g., GroupBy disk spill).
 */
export class Pipeline {
	private readonly operators: Operator[];
	private readonly outputSchema: Schema;

	constructor(operators: Operator[]) {
		if (operators.length === 0) {
			throw new Error("Pipeline must have at least one operator");
		}
		this.operators = operators;
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
	 * Always async to uniformly handle operators that may spill during process().
	 */
	async executeChunk(chunk: Chunk): Promise<Result<{ chunks: Chunk[]; done: boolean }>> {
		const outputs: Chunk[] = [];
		let current: Chunk | null = chunk;
		let pipelineDone = false;

		for (const op of this.operators) {
			if (current === null) break;

			const result = await op.process(current);
			if (result.error !== ErrorCode.None) {
				return err(result.error);
			}

			current = result.value.chunk;

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
	 * Execute the pipeline on multiple chunks.
	 * Returns a Promise — use streamAsync() for lazy/streaming consumption.
	 */
	async execute(chunks: Iterable<Chunk>): Promise<Result<PipelineResult>> {
		this.reset();

		const startTime = performance.now();
		const outputChunks: Chunk[] = [];
		let rowsIn = 0;
		let rowsOut = 0;
		let pipelineDone = false;

		for (const chunk of chunks) {
			if (pipelineDone) break;

			rowsIn += chunk.rowCount;

			const result = await this.executeChunk(chunk);
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

			let hasMore = true;
			while (hasMore && !pipelineDone) {
				const result = await op.finish();
				if (result.error !== ErrorCode.None) {
					return err(result.error);
				}

				hasMore = result.value.hasMore;

				if (result.value.chunk !== null) {
					let chunk: Chunk | null = result.value.chunk;
					for (let j = i + 1; j < this.operators.length && chunk !== null; j++) {
						const downstream = this.operators[j]!;
						const downstreamResult = await downstream.process(chunk);
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

				if (result.value.done) {
					pipelineDone = true;
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
		this.reset();

		const startTime = performance.now();
		const outputChunks: Chunk[] = [];
		let rowsIn = 0;
		let rowsOut = 0;
		let pipelineDone = false;

		for await (const chunk of chunks) {
			if (pipelineDone) break;

			rowsIn += chunk.rowCount;

			const result = await this.executeChunk(chunk);
			if (result.error !== ErrorCode.None) {
				return err(result.error);
			}

			for (const outChunk of result.value.chunks) {
				rowsOut += outChunk.rowCount;
				outputChunks.push(outChunk);
			}

			pipelineDone = result.value.done;
		}

		// Finish all operators
		for (let i = 0; i < this.operators.length; i++) {
			const op = this.operators[i]!;

			let hasMore = true;
			while (hasMore && !pipelineDone) {
				const result = await op.finish();
				if (result.error !== ErrorCode.None) {
					return err(result.error);
				}

				hasMore = result.value.hasMore;

				if (result.value.chunk !== null) {
					let chunk: Chunk | null = result.value.chunk;
					for (let j = i + 1; j < this.operators.length && chunk !== null; j++) {
						const downstream = this.operators[j]!;
						const downstreamResult = await downstream.process(chunk);
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

				if (result.value.done) {
					pipelineDone = true;
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
	 * Execute pipeline and stream results lazily (async generator).
	 * Preferred over execute() for large datasets — output is consumed
	 * incrementally instead of being fully buffered in memory.
	 */
	async *stream(chunks: Iterable<Chunk>): AsyncGenerator<Chunk> {
		this.reset();

		for (const chunk of chunks) {
			const result = await this.executeChunk(chunk);
			if (result.error !== ErrorCode.None) {
				throw new Error(`Pipeline error: ${result.error}`);
			}

			for (const outChunk of result.value.chunks) {
				yield outChunk;
			}

			if (result.value.done) break;
		}

		// Finish operators
		let pipelineDone = false;
		for (let i = 0; i < this.operators.length; i++) {
			const op = this.operators[i]!;

			let hasMore = true;
			while (hasMore && !pipelineDone) {
				const result = await op.finish();
				if (result.error !== ErrorCode.None) {
					throw new Error(`Pipeline error: ${result.error}`);
				}

				hasMore = result.value.hasMore;

				if (result.value.chunk !== null) {
					let chunk: Chunk | null = result.value.chunk;
					for (let j = i + 1; j < this.operators.length && chunk !== null; j++) {
						const downstream = this.operators[j]!;
						const downstreamResult = await downstream.process(chunk);
						if (downstreamResult.error !== ErrorCode.None) {
							throw new Error(`Pipeline error: ${downstreamResult.error}`);
						}
						chunk = downstreamResult.value.chunk;
						if (downstreamResult.value.done) {
							pipelineDone = true;
						}
					}

					if (chunk !== null) {
						yield chunk;
					}
				}

				if (result.value.done) {
					pipelineDone = true;
				}
			}
		}
	}

	/**
	 * Execute pipeline on async stream (async generator).
	 */
	async *streamAsync(chunks: AsyncIterable<Chunk>): AsyncGenerator<Chunk> {
		this.reset();

		for await (const chunk of chunks) {
			const result = await this.executeChunk(chunk);
			if (result.error !== ErrorCode.None) {
				throw new Error(`Pipeline error: ${result.error}`);
			}

			for (const outChunk of result.value.chunks) {
				yield outChunk;
			}

			if (result.value.done) break;
		}

		// Finish operators (await for async operators like external sort)
		let pipelineStreamDone = false;
		for (let i = 0; i < this.operators.length; i++) {
			const op = this.operators[i]!;

			let hasMore = true;
			while (hasMore && !pipelineStreamDone) {
				const result = await op.finish();
				if (result.error !== ErrorCode.None) {
					throw new Error(`Pipeline error: ${result.error}`);
				}

				hasMore = result.value.hasMore;

				if (result.value.chunk !== null) {
					let chunk: Chunk | null = result.value.chunk;
					for (let j = i + 1; j < this.operators.length && chunk !== null; j++) {
						const downstream = this.operators[j]!;
						const downstreamResult = await downstream.process(chunk);
						if (downstreamResult.error !== ErrorCode.None) {
							throw new Error(`Pipeline error: ${downstreamResult.error}`);
						}
						chunk = downstreamResult.value.chunk;
						if (downstreamResult.value.done) {
							pipelineStreamDone = true;
						}
					}

					if (chunk !== null) {
						yield chunk;
					}
				}

				if (result.value.done) {
					pipelineStreamDone = true;
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
