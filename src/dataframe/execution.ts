/* EXECUTION METHODS
/*-----------------------------------------------------
/* Execute pipeline and output results
/* ==================================================== */

import type { Chunk } from "../buffer/chunk.ts";
import { recycleChunk } from "../buffer/pool.ts";
import { Pipeline } from "../ops/pipeline.ts";
import { DTypeKind } from "../types/dtypes.ts";
import type { DataFrame } from "./core.ts";

export function addExecutionMethods(df: typeof DataFrame.prototype) {
	df.toChunks = async function (): Promise<Chunk[]> {
		const collected = await this.collect();
		return collected.source as Chunk[];
	};

	/**
	 * Count rows. Uses the most efficient path available:
	 * - No operators: direct source iteration (zero-copy)
	 * - Sync source + operators: sync pipeline (no async overhead)
	 * - Async source + operators: streaming async pipeline
	 */
	df.count = async function (): Promise<number> {
		let total = 0;

		if (this.operators.length === 0) {
			// Fast path: count source chunks directly
			if (Symbol.asyncIterator in this.source) {
				for await (const chunk of this.source as AsyncIterable<Chunk>) {
					total += chunk.rowCount;
				}
			} else {
				for (const chunk of this.source as Iterable<Chunk>) {
					total += chunk.rowCount;
				}
			}
		} else {
			// Pipeline path: use sync pipeline for in-memory sources to avoid async overhead
			const pipe = new Pipeline(this.operators);
			if (Symbol.asyncIterator in this.source) {
				for await (const chunk of pipe.streamAsync(this.source as AsyncIterable<Chunk>)) {
					total += chunk.rowCount;
				}
			} else {
				for (const chunk of pipe.stream(this.source as Iterable<Chunk>)) {
					total += chunk.rowCount;
				}
			}
		}

		return total;
	};

	df.toArray = async function (): Promise<Record<string, unknown>[]> {
		const collected = await this.collect();
		const dictionary = collected._dictionary;
		const chunks = collected.source as Chunk[];
		const schema = collected._schema;

		// Calculate total rows
		let totalRows = 0;
		for (const chunk of chunks) totalRows += chunk.rowCount;
		if (totalRows === 0) return [];

		// Pre-allocate result array
		const rows = new Array<Record<string, unknown>>(totalRows);

		// Create a row template from the schema for fast object creation
		const colCount = schema.columnCount;
		const colNames = new Array<string>(colCount);
		const colDTypes = new Array<DTypeKind>(colCount);
		for (let c = 0; c < colCount; c++) {
			const col = schema.columns[c]!;
			colNames[c] = col.name;
			colDTypes[c] = col.dtype.kind;
		}

		// Column-first iteration: process one column at a time across all rows
		// This is friendlier to CPU caches than row-first iteration
		let rowOffset = 0;

		for (const chunk of chunks) {
			const rowCount = chunk.rowCount;
			const enhancedChunk = (chunk as any)._enhancedChunk;

			// Pre-allocate row objects for this chunk
			for (let r = 0; r < rowCount; r++) {
				rows[rowOffset + r] = {};
			}

			// Process each column across all rows (column-first for cache efficiency)
			for (let c = 0; c < colCount; c++) {
				const colName = colNames[c]!;
				const kind = colDTypes[c]!;

				if (kind === DTypeKind.StringView && enhancedChunk) {
					const adapter = enhancedChunk.getColumnAdapter(c);
					for (let r = 0; r < rowCount; r++) {
						if (chunk.isNull(c, r)) {
							rows[rowOffset + r]![colName] = null;
						} else {
							rows[rowOffset + r]![colName] = adapter?.getStringValue(r) ?? null;
						}
					}
				} else if ((kind === DTypeKind.String || kind === DTypeKind.StringDict)) {
					// Batch string decode: prefer chunk's own dictionary for correct
					// cross-chunk resolution, fall back to DataFrame-level dictionary
					const dict = chunk.dictionary ?? dictionary;
					if (dict) {
						for (let r = 0; r < rowCount; r++) {
							if (chunk.isNull(c, r)) {
								rows[rowOffset + r]![colName] = null;
							} else {
								const dictIndex = chunk.getValue(c, r) as number;
								rows[rowOffset + r]![colName] = dict.getString(dictIndex);
							}
						}
					}
				} else if (kind === DTypeKind.Boolean) {
					for (let r = 0; r < rowCount; r++) {
						if (chunk.isNull(c, r)) {
							rows[rowOffset + r]![colName] = null;
						} else {
							rows[rowOffset + r]![colName] = chunk.getValue(c, r) === 1;
						}
					}
				} else {
					for (let r = 0; r < rowCount; r++) {
						if (chunk.isNull(c, r)) {
							rows[rowOffset + r]![colName] = null;
						} else {
							rows[rowOffset + r]![colName] = chunk.getValue(c, r);
						}
					}
				}
			}

			rowOffset += rowCount;
		}

		return rows;
	};

	/**
	 * Export as columnar data (zero-copy for numeric columns).
	 * Returns raw typed arrays for numeric columns and string[] for string columns.
	 * Much faster than toArray() when you don't need row objects.
	 */
	df.toColumns = async function (): Promise<Record<string, unknown>> {
		const collected = await this.collect();
		const dictionary = collected._dictionary;
		const chunks = collected.source as Chunk[];
		const schema = collected._schema;

		// Calculate total rows
		let totalRows = 0;
		for (const chunk of chunks) totalRows += chunk.rowCount;

		const result: Record<string, unknown> = {};

		for (let c = 0; c < schema.columnCount; c++) {
			const col = schema.columns[c]!;
			const kind = col.dtype.kind;

			if ((kind === DTypeKind.String || kind === DTypeKind.StringDict)) {
				// String columns: prefer chunk's own dictionary for correct cross-chunk resolution
				const strings = new Array<string | null>(totalRows);
				let offset = 0;
				for (const chunk of chunks) {
					const dict = chunk.dictionary ?? dictionary;
					if (!dict) { offset += chunk.rowCount; continue; }
					for (let r = 0; r < chunk.rowCount; r++) {
						if (chunk.isNull(c, r)) {
							strings[offset++] = null;
						} else {
							const idx = chunk.getValue(c, r) as number;
							strings[offset++] = dict.getString(idx) ?? null;
						}
					}
				}
				result[col.name] = strings;
			} else if (kind === DTypeKind.Boolean) {
				const bools = new Array<boolean | null>(totalRows);
				let offset = 0;
				for (const chunk of chunks) {
					for (let r = 0; r < chunk.rowCount; r++) {
						if (chunk.isNull(c, r)) {
							bools[offset++] = null;
						} else {
							bools[offset++] = chunk.getValue(c, r) === 1;
						}
					}
				}
				result[col.name] = bools;
			} else {
				// Numeric columns: if single chunk with no selection, zero-copy view
				if (chunks.length === 1 && !chunks[0]!.getSelection()) {
					const colBuf = chunks[0]!.getColumn(c);
					if (colBuf) {
						result[col.name] = colBuf.data.subarray(0, totalRows);
						continue;
					}
				}
				// Multiple chunks or has selection: copy into new typed array
				const colBuf = chunks[0]?.getColumn(c);
				if (colBuf) {
					const TypedArrayCtor = colBuf.data.constructor as new (len: number) => ArrayLike<unknown>;
					const arr = new TypedArrayCtor(totalRows);
					let offset = 0;
					for (const chunk of chunks) {
						for (let r = 0; r < chunk.rowCount; r++) {
							(arr as any)[offset++] = chunk.getValue(c, r);
						}
					}
					result[col.name] = arr;
				}
			}
		}

		return result;
	};

	/**
	 * Streaming forEach — processes rows through the pipeline without buffering.
	 * Memory: O(chunk_size) instead of O(total_rows).
	 * Uses sync pipeline for in-memory sources to avoid async generator overhead.
	 */
	df.forEach = async function (fn: (row: Record<string, unknown>) => void): Promise<void> {
		const schema = this.currentSchema();
		const colCount = schema.columnCount;
		const colNames = new Array<string>(colCount);
		const colDTypes = new Array<DTypeKind>(colCount);
		for (let c = 0; c < colCount; c++) {
			colNames[c] = schema.columns[c]!.name;
			colDTypes[c] = schema.columns[c]!.dtype.kind;
		}

		const processChunk = (chunk: Chunk) => {
			const dictionary = chunk.dictionary ?? this._dictionary;
			// Flyweight pattern: reuse a single row object instead of
			// allocating a new one per row. Properties are overwritten
			// each iteration so the GC never sees the intermediate objects.
			const row: Record<string, unknown> = {};
			for (let r = 0; r < chunk.rowCount; r++) {
				for (let c = 0; c < colCount; c++) {
					const kind = colDTypes[c]!;
					if (chunk.isNull(c, r)) {
						row[colNames[c]!] = null;
					} else if ((kind === DTypeKind.String || kind === DTypeKind.StringDict) && dictionary) {
						row[colNames[c]!] = dictionary.getString(chunk.getValue(c, r) as number);
					} else if (kind === DTypeKind.Boolean) {
						row[colNames[c]!] = chunk.getValue(c, r) === 1;
					} else {
						row[colNames[c]!] = chunk.getValue(c, r);
					}
				}
				fn(row);
			}
		};

		if (this.operators.length === 0) {
			if (Symbol.asyncIterator in this.source) {
				for await (const chunk of this.source as AsyncIterable<Chunk>) processChunk(chunk);
			} else {
				for (const chunk of this.source as Iterable<Chunk>) processChunk(chunk);
			}
		} else {
			const pipe = new Pipeline(this.operators);
			if (Symbol.asyncIterator in this.source) {
				for await (const chunk of pipe.streamAsync(this.source as AsyncIterable<Chunk>)) processChunk(chunk);
			} else {
				for (const chunk of pipe.stream(this.source as Iterable<Chunk>)) processChunk(chunk);
			}
		}
	};

	/**
	 * Streaming reduce — accumulates a result through the pipeline without buffering.
	 * Memory: O(1) for the accumulator.
	 * Uses sync pipeline for in-memory sources to avoid async generator overhead.
	 */
	df.reduce = async function <R>(fn: (acc: R, row: Record<string, unknown>) => R, initial: R): Promise<R> {
		const schema = this.currentSchema();
		const colCount = schema.columnCount;
		const colNames = new Array<string>(colCount);
		const colDTypes = new Array<DTypeKind>(colCount);
		for (let c = 0; c < colCount; c++) {
			colNames[c] = schema.columns[c]!.name;
			colDTypes[c] = schema.columns[c]!.dtype.kind;
		}

		let acc = initial;
		const processChunk = (chunk: Chunk) => {
			const dictionary = chunk.dictionary ?? this._dictionary;
			// Flyweight pattern: reuse a single row object per chunk
			const row: Record<string, unknown> = {};
			for (let r = 0; r < chunk.rowCount; r++) {
				for (let c = 0; c < colCount; c++) {
					const kind = colDTypes[c]!;
					if (chunk.isNull(c, r)) {
						row[colNames[c]!] = null;
					} else if ((kind === DTypeKind.String || kind === DTypeKind.StringDict) && dictionary) {
						row[colNames[c]!] = dictionary.getString(chunk.getValue(c, r) as number);
					} else if (kind === DTypeKind.Boolean) {
						row[colNames[c]!] = chunk.getValue(c, r) === 1;
					} else {
						row[colNames[c]!] = chunk.getValue(c, r);
					}
				}
				acc = fn(acc, row);
			}
		};

		if (this.operators.length === 0) {
			if (Symbol.asyncIterator in this.source) {
				for await (const chunk of this.source as AsyncIterable<Chunk>) processChunk(chunk);
			} else {
				for (const chunk of this.source as Iterable<Chunk>) processChunk(chunk);
			}
		} else {
			const pipe = new Pipeline(this.operators);
			if (Symbol.asyncIterator in this.source) {
				for await (const chunk of pipe.streamAsync(this.source as AsyncIterable<Chunk>)) processChunk(chunk);
			} else {
				for (const chunk of pipe.stream(this.source as Iterable<Chunk>)) processChunk(chunk);
			}
		}
		return acc;
	};

	df.show = async function (maxRows: number = 10): Promise<void> {
		// Optimization: stream only required rows
		const rows: Record<string, unknown>[] = [];
		let count = 0;
		const _dictionary = this._dictionary;

		// Streaming fetch
		if (this.operators.length === 0 && Symbol.asyncIterator in this.source) {
			for await (const chunk of this.source as AsyncIterable<Chunk>) {
				const need = maxRows - count;
				if (need <= 0) {
					recycleChunk(chunk);
					break;
				}

				const take = Math.min(need, chunk.rowCount);
				const schema = this._schema;
				const enhancedChunk = (chunk as any)._enhancedChunk;
				
				for (let r = 0; r < take; r++) {
					const row: Record<string, unknown> = {};
					for (let c = 0; c < schema.columnCount; c++) {
						const col = schema.columns[c];
						if (!col) continue;
						const colName = col.name;
						const dtype = col.dtype;

						if (chunk.isNull(c, r)) {
							row[colName] = null;
						} else if (dtype.kind === DTypeKind.StringView && enhancedChunk) {
							const adapter = enhancedChunk.getColumnAdapter(c);
							row[colName] = adapter?.getStringValue(r) ?? null;
						} else if (
							(dtype.kind === DTypeKind.String || dtype.kind === DTypeKind.StringDict)
						) {
							// Use chunk's dictionary first (correct for cross-chunk MBF data),
							// fall back to DataFrame-level dictionary
							const dict = chunk.dictionary ?? this._dictionary;
							const dictIndex = chunk.getValue(c, r) as number;
							row[colName] = dict?.getString(dictIndex);
						} else {
							row[colName] = chunk.getValue(c, r);
						}
					}
					rows.push(row);
				}

				count += take;
				recycleChunk(chunk);
				if (count >= maxRows) break;
			}
		} else {
			// Fallback to collect
			const _limited = this.limit(maxRows); // Helper that might support streaming?
			// But limit() returns DataFrame with Limit operator.
			// And collect() on that will buffer.
			// For show(), we just want N rows.

			// Let's just use toArray() which uses collect()
			const allRows = await this.limit(maxRows).toArray();
			rows.push(...allRows);
		}

		if (rows.length === 0) {
			console.log("Empty DataFrame");
			return;
		}

		const keys = Object.keys(rows[0] ?? {});
		const widths = keys.map((k) => {
			const values = rows.map((r) => String(r[k]));
			return Math.max(k.length, ...values.map((v) => v.length));
		});

		const header = keys.map((k, i) => k.padEnd(widths[i] ?? 0)).join(" │ ");
		const separator = widths.map((w) => "─".repeat(w)).join("─┼─");

		console.log(`┌─${separator}─┐`);
		console.log(`│ ${header} │`);
		console.log(`├─${separator}─┤`);

		for (const row of rows) {
			const line = keys
				.map((k, i) => String(row[k]).padEnd(widths[i] ?? 0))
				.join(" │ ");
			console.log(`│ ${line} │`);
		}

		console.log(`└─${separator}─┘`);
		console.log(`\nShowing first ${rows.length} rows`);
	};
}
