/* EXECUTION METHODS
/*-----------------------------------------------------
/* Execute pipeline and output results
/* ==================================================== */

import type { Chunk } from "../buffer/chunk.ts";
import { recycleChunk } from "../buffer/pool.ts";
import { DTypeKind } from "../types/dtypes.ts";
import type { DataFrame } from "./core.ts";

export function addExecutionMethods(df: typeof DataFrame.prototype) {
	df.toChunks = async function (): Promise<Chunk[]> {
		const collected = await this.collect();
		return collected.source as Chunk[];
	};

	df.count = async function (): Promise<number> {
		// If operators exist, we might need pipeline
		// But for simple count on source, streaming is best
		if (this.operators.length === 0) {
			let total = 0;
			if (Symbol.asyncIterator in this.source) {
				for await (const chunk of this.source as AsyncIterable<Chunk>) {
					total += chunk.rowCount;
					recycleChunk(chunk);
				}
			} else {
				for (const chunk of this.source as Iterable<Chunk>) {
					total += chunk.rowCount;
					// recycleChunk(chunk); // Only recycle if we own it?
					// For in-memory source (array), we shouldn't recycle!
					// But this.source might be a generator?
					// Safest: only recycle if it came from async source (File IO) or if we know it's transient.
					// But `Df.fromStream` wraps async iterable.
					// Assuming sync iterable is usually in-memory array or simple generator.
					// Let's safe side: recycle only consumed async chunks?
					// Actually if it's CsvSource, it is AsyncIterable.
				}
			}
			return total;
		}

		// With operators, fallback to collect for now, or implement streaming pipeline
		// For now, keeping existing logic for operators case
		const collected = await this.collect();
		let total = 0;
		for (const chunk of collected.source as Chunk[]) {
			total += chunk.rowCount;
		}
		return total;
	};

	df.toArray = async function (): Promise<Record<string, unknown>[]> {
		const collected = await this.collect();
		const rows: Record<string, unknown>[] = [];
		const dictionary = collected._dictionary;

		// We can't recycle here effectively because we need dictionary?
		// Actually dictionary is shared.
		// But we are returning rows, so we are materializing anyway.

		const chunks = collected.source as Chunk[];

		for (const chunk of chunks) {
			const schema = collected._schema;
			for (let r = 0; r < chunk.rowCount; r++) {
				const row: Record<string, unknown> = {};
				for (let c = 0; c < schema.columnCount; c++) {
					const col = schema.columns[c];
					if (!col) continue; // Should not happen
					const colName = col.name;
					const dtype = col.dtype;

					if (chunk.isNull(c, r)) {
						row[colName] = null;
					} else if (dtype.kind === DTypeKind.String && dictionary?.getString) {
						const dictIndex = chunk.getValue(c, r) as number;
						row[colName] = dictionary.getString(dictIndex);
					} else if (dtype.kind === DTypeKind.Boolean) {
						// Convert 0/1 to boolean
						row[colName] = chunk.getValue(c, r) === 1;
					} else {
						// Direct value (for parquet or non-dictionary columns)
						row[colName] = chunk.getValue(c, r);
					}
				}
				rows.push(row);
			}
		}

		return rows;
	};

	df.show = async function (maxRows: number = 10): Promise<void> {
		// Optimization: stream only required rows
		const rows: Record<string, unknown>[] = [];
		let count = 0;
		const _dictionary = this._dictionary;

		// Streaming fetch
		if (this.operators.length === 0 && Symbol.asyncIterator in this.source) {
			for await (const chunk of this.source as AsyncIterable<Chunk>) {
				// Update dictionary if chunk has one (progressive update?)
				// Schema is fixed for CsvSource. Dictionary is shared.

				// Iterate rows
				const need = maxRows - count;
				if (need <= 0) {
					recycleChunk(chunk);
					break; // Stop streaming
				}

				const take = Math.min(need, chunk.rowCount);
				// chunk.rows() is expensive generator.
				// Better manual extraction to reuse logic?
				// Re-using toArray logic for single chunk
				const schema = this._schema;
				for (let r = 0; r < take; r++) {
					const row: Record<string, unknown> = {};
					for (let c = 0; c < schema.columnCount; c++) {
						const col = schema.columns[c];
						if (!col) continue;
						const colName = col.name;
						const dtype = col.dtype;

						if (chunk.isNull(c, r)) {
							row[colName] = null;
						} else if (
							dtype.kind === DTypeKind.String &&
							this._dictionary?.getString
						) {
							const dictIndex = chunk.getValue(c, r) as number;
							// Need dictionary. CsvSource shares dictionary.
							// But chunk.dictionary might be null if no strings?
							// CsvParser.dictionary is passed to Chunk.
							row[colName] = this._dictionary.getString(dictIndex);
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
