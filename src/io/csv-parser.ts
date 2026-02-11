/**
 * CSV tokenizer and parser.
 *
 * High-performance streaming CSV parser that outputs chunks.
 * Supports quoted fields, escaped quotes, and configurable delimiters.
 */
/** biome-ignore-all lint/style/noNonNullAssertion: Guarantee no null*/

import { Chunk } from "../buffer/chunk.ts";
import type { ColumnBuffer } from "../buffer/column-buffer.ts";
import { createDictionary, type Dictionary } from "../buffer/dictionary.ts";
import { bufferPool } from "../buffer/pool.ts";
import { type DType, DTypeKind } from "../types/dtypes.ts";
import type { Schema } from "../types/schema.ts";

/** CSV parsing options */
export interface CsvOptions {
	/** Field delimiter (default: comma) */
	delimiter?: string;
	/** Quote character (default: double quote) */
	quote?: string;
	/** Whether the first row contains headers (default: true) */
	hasHeader?: boolean;
	/** Maximum rows per chunk (default: 8192) */
	chunkSize?: number;
	/** Skip initial rows (default: 0) */
	skipRows?: number;
	/** Maximum rows to read (default: Infinity) */
	maxRows?: number;
	/** Indices of columns to include. If undefined, all columns are included. */
	projection?: number[];
}

/** Parser state */
export enum ParseState {
	FieldStart,
	Field,
	QuotedField,
	QuoteInQuotedField,
	CR,
}
const DEFAULT_OPTIONS = {
	delimiter: ",",
	quote: '"',
	hasHeader: true,
	chunkSize: 16384,
	skipRows: 0,
	maxRows: Infinity,
};

type ResolvedCsvOptions = typeof DEFAULT_OPTIONS & {
	projection?: number[];
};

/**
 * CSV parser that produces chunks.
 */
export class CsvParser {
	private readonly options: ResolvedCsvOptions;
	private readonly schema: Schema;
	private dictionary: Dictionary;
	private readonly decoder = new TextDecoder();

	private state: ParseState = ParseState.FieldStart;
	private rowCount: number = 0;
	private skipRemaining: number;
	private maxRemaining: number;
	private totalRowCount: number = 0;

	// Refactored fields
	private fieldStart: number = 0;
	private currentFieldPrefix: Uint8Array | null = null; // Accumulated from previous chunks

	// Direct column buffers access
	private columns: ColumnBuffer[] | null = null;
	private chunkRowCount: number = 0;

	private currentColumnIndex: number = 0;

	// Mapping from CSV column index to Schema column index
	private csvToSchema: Int32Array | null = null;

	constructor(schema: Schema, options?: CsvOptions) {
		this.schema = schema;
		this.dictionary = createDictionary();
		this.options = {
			delimiter: options?.delimiter ?? ",",
			quote: options?.quote ?? '"',
			hasHeader: options?.hasHeader ?? true,
			chunkSize: options?.chunkSize ?? 16384,
			skipRows: options?.skipRows ?? 0,
			maxRows: options?.maxRows ?? Infinity,
			projection: options?.projection,
		};

		this.skipRemaining = this.options.skipRows;
		this.maxRemaining = this.options.maxRows;

		// Initialize Mapping
		if (this.options.projection) {
			let maxIdx = 0;
			for (const idx of this.options.projection) {
				if (idx > maxIdx) maxIdx = idx;
			}
			this.csvToSchema = new Int32Array(maxIdx + 1).fill(-1);
			for (let i = 0; i < this.options.projection.length; i++) {
				const csvIdx = this.options.projection[i];
				if (csvIdx !== undefined) {
					this.csvToSchema[csvIdx] = i;
				}
			}
		}
	}

	reset(): void {
		this.state = ParseState.FieldStart;
		this.currentFieldPrefix = null;
		this.fieldStart = 0;
		this.rowCount = 0;
		this.totalRowCount = 0;
		this.skipRemaining = this.options.skipRows;
		this.maxRemaining = this.options.maxRows;
		if (this.columns) {
			for (const col of this.columns) {
				bufferPool.release(col);
			}
		}
		this.columns = null;
		this.chunkRowCount = 0;
		this.currentColumnIndex = 0;
		// Reset dictionary between iterations so that re-streaming the same
		// source doesn't accumulate strings across passes. Within a single
		// pass, the dictionary is shared across all chunks (ensuring correct
		// cross-chunk string resolution).
		this.dictionary = createDictionary();
	}

	getMetadata(): { totalRowCount: number } {
		return { totalRowCount: this.totalRowCount };
	}

	getDictionary(): Dictionary {
		return this.dictionary;
	}

	getSchema(): Schema {
		return this.schema;
	}

	// Helper for skipping non-nullable columns
	private appendDefault(col: ColumnBuffer, dtype: DType) {
		switch (dtype.kind) {
			case DTypeKind.String:
				col.append(this.dictionary.internString("") as never);
				break;
			case DTypeKind.Int8:
			case DTypeKind.Int16:
			case DTypeKind.Int32:
			case DTypeKind.UInt8:
			case DTypeKind.UInt16:
			case DTypeKind.UInt32:
			case DTypeKind.Float32:
			case DTypeKind.Float64:
			case DTypeKind.Boolean:
				col.append(0 as never);
				break;
			case DTypeKind.Int64:
			case DTypeKind.UInt64:
			case DTypeKind.Timestamp:
				col.append(0n as never);
				break;
			default:
				col.appendNull();
		}
	}

	parse(data: Uint8Array): Chunk[] {
		const chunks: Chunk[] = [];
		const len = data.length;

		const DELIMITER = this.options.delimiter.charCodeAt(0);
		const CR = 13;
		const LF = 10;
		const QUOTE = this.options.quote.charCodeAt(0);

		let i = 0;
		while (i < len) {
			// Fast Path for Field state (common case)
			if (
				this.state === ParseState.Field ||
				this.state === ParseState.FieldStart
			) {
				// 1. Check for Quote at start
				if (this.state === ParseState.FieldStart && data[i] === QUOTE) {
					this.state = ParseState.QuotedField;
					this.fieldStart = i + 1;
					i++;
					continue;
				}

				if (this.state === ParseState.FieldStart) {
					this.fieldStart = i;
					this.state = ParseState.Field;
				}

				// 2. Scan for delimiter or newline
				while (i < len) {
					const c = data[i]!;
					if (c === DELIMITER) {
						// Inline projection check
						if (this.csvToSchema) {
							const sIdx = this.csvToSchema[this.currentColumnIndex];
							if (sIdx !== undefined && sIdx !== -1) {
								this.finishField(data, i);
							}
						} else {
							this.finishField(data, i);
						}

						this.currentColumnIndex++;
						this.state = ParseState.FieldStart;
						i++; // consume delim
						break;
					} else if (c === LF) {
						// Inline projection check
						if (this.csvToSchema) {
							const sIdx = this.csvToSchema[this.currentColumnIndex];
							if (sIdx !== undefined && sIdx !== -1) {
								this.finishField(data, i);
							}
						} else {
							this.finishField(data, i);
						}

						this.currentColumnIndex++;
						this.finishRow(chunks);
						this.state = ParseState.FieldStart;
						i++; // consume LF
						break;
					} else if (c === CR) {
						// End of field (CRLF) or CR?
						// Inline projection check
						if (this.csvToSchema) {
							const sIdx = this.csvToSchema[this.currentColumnIndex];
							if (sIdx !== undefined && sIdx !== -1) {
								this.finishField(data, i);
							}
						} else {
							this.finishField(data, i);
						}

						this.currentColumnIndex++;
						this.state = ParseState.CR;
						i++;
						break;
					}
					i++;
				}
				continue;
			}

			// Fallback / Complex States
			const charCode = data[i]!;
			switch (this.state) {
				// FieldStart and Field handled above primarily, but checked for transitions
				case ParseState.QuotedField:
					if (charCode === QUOTE) {
						this.state = ParseState.QuoteInQuotedField;
					}
					break;

				case ParseState.QuoteInQuotedField:
					if (charCode === QUOTE) {
						// Escaped quote ("")
						this.state = ParseState.QuotedField;
					} else if (charCode === DELIMITER) {
						this.finishField(data, i - 1); // exclude closing quote
						this.currentColumnIndex++;
						this.state = ParseState.FieldStart;
					} else if (charCode === CR) {
						this.finishField(data, i - 1);
						this.currentColumnIndex++;
						this.state = ParseState.CR;
					} else if (charCode === LF) {
						this.finishField(data, i - 1);
						this.currentColumnIndex++;
						this.finishRow(chunks);
						this.state = ParseState.FieldStart;
					} else {
						this.state = ParseState.Field;
					}
					break;

				case ParseState.CR:
					if (charCode === LF) {
						this.finishRow(chunks);
						this.state = ParseState.FieldStart;
					} else {
						this.finishRow(chunks);
						this.state = ParseState.FieldStart;
						// Don't consume this char, re-evaluate
						i--;
					}
					break;
			}
			i++;
		}

		// Handle buffer end
		if (
			this.state === ParseState.Field ||
			this.state === ParseState.QuotedField ||
			this.state === ParseState.QuoteInQuotedField
		) {
			// Identify slice
			const slice = data.subarray(this.fieldStart, len);
			if (this.currentFieldPrefix) {
				// Append to existing
				const newPrefix = new Uint8Array(
					this.currentFieldPrefix.length + slice.length,
				);
				newPrefix.set(this.currentFieldPrefix);
				newPrefix.set(slice, this.currentFieldPrefix.length);
				this.currentFieldPrefix = newPrefix;
			} else {
				this.currentFieldPrefix = new Uint8Array(slice);
			}
			this.fieldStart = 0;
		}

		return chunks;
	}

	finish(): Chunk | null {
		const chunks: Chunk[] = [];

		if (this.currentFieldPrefix !== null || this.columns !== null) {
			if (this.currentFieldPrefix !== null) {
				let val = this.decoder.decode(this.currentFieldPrefix).trim();
				if (this.state === ParseState.QuoteInQuotedField) {
					if (val.endsWith(this.options.quote)) {
						val = val.slice(0, -1);
					}
					if (val.includes(this.options.quote + this.options.quote)) {
						val = val.replaceAll(
							this.options.quote + this.options.quote,
							this.options.quote,
						);
					}
				} else if (this.state === ParseState.QuotedField) {
					if (val.includes(this.options.quote + this.options.quote)) {
						val = val.replaceAll(
							this.options.quote + this.options.quote,
							this.options.quote,
						);
					}
				}

				this.pushValueString(val);
				this.currentFieldPrefix = null;
				this.currentColumnIndex++;
			}
			this.finishRow(chunks, true);
		}

		if (chunks.length > 0) {
			return chunks[0]!;
		}
		return null;
	}

	private finishField(data: Uint8Array, end: number): void {
		this.ensureColumns();
		if (this.shouldSkip()) return;

		let schemaIdx = -1;
		if (this.csvToSchema) {
			if (this.currentColumnIndex < this.csvToSchema.length) {
				schemaIdx = this.csvToSchema[this.currentColumnIndex] ?? -1;
			}
		} else {
			if (this.currentColumnIndex < this.schema.columnCount) {
				schemaIdx = this.currentColumnIndex;
			}
		}

		if (schemaIdx === -1) return;

		if (
			this.currentFieldPrefix === null &&
			this.state !== ParseState.QuoteInQuotedField &&
			this.state !== ParseState.QuotedField
		) {
			// Zero-copy path: Direct call
			const col = this.columns?.[schemaIdx];
			if (!col) return;
			const dtype = this.schema.columns[schemaIdx]!.dtype;
			this.appendValueSlice(col, data, this.fieldStart, end, dtype);
			return;
		}

		// Slow path: prefixes or quotes
		let fullField: Uint8Array;
		if (this.currentFieldPrefix !== null) {
			const slice = data.subarray(this.fieldStart, end);
			fullField = new Uint8Array(this.currentFieldPrefix.length + slice.length);
			fullField.set(this.currentFieldPrefix);
			fullField.set(slice, this.currentFieldPrefix.length);
			this.currentFieldPrefix = null;
		} else {
			fullField = data.subarray(this.fieldStart, end);
		}

		let val = this.decoder.decode(fullField);

		if (
			this.state === ParseState.QuoteInQuotedField ||
			this.state === ParseState.QuotedField
		) {
			if (val.includes(this.options.quote + this.options.quote)) {
				val = val.replaceAll(
					this.options.quote + this.options.quote,
					this.options.quote,
				);
			}
		}

		if (this.columns) {
			const col = this.columns[schemaIdx];
			if (col) {
				const dtype = this.schema.columns[schemaIdx]!.dtype;
				this.appendValueString(col, val, dtype);
			}
		}
	}

	private finishRow(chunks: Chunk[], force: boolean = false): void {
		if (!force) {
			if (this.rowCount === 0 && this.options.hasHeader) {
				this.rowCount++;
				this.currentColumnIndex = 0;
				return;
			}

			if (this.skipRemaining > 0) {
				this.skipRemaining--;
				this.rowCount++;
				this.currentColumnIndex = 0;
				return;
			}

			if (this.maxRemaining <= 0) return;

			this.maxRemaining--;
			this.rowCount++;
			this.totalRowCount++;
			this.chunkRowCount++;
		}

		if (this.columns && this.currentColumnIndex < this.schema.columnCount) {
			for (let i = this.currentColumnIndex; i < this.schema.columnCount; i++) {
				const col = this.columns[i];
				if (col) {
					const dtype = this.schema.columns[i]!.dtype;
					if (dtype.nullable) {
						col.appendNull();
					} else {
						this.appendDefault(col, dtype);
					}
				}
			}
		}

		this.currentColumnIndex = 0;

		if (this.chunkRowCount >= this.options.chunkSize || force) {
			if (this.columns) {
				// Create chunk with shared dictionary.
				// All chunks share the same dictionary so that dictionary indices
				// are consistent across chunks. The dictionary only grows by the
				// number of UNIQUE strings, not by row count, so it doesn't
				// accumulate unbounded memory for typical datasets.
				const chunk = new Chunk(this.schema, this.columns, this.dictionary);
				chunks.push(chunk);
				this.columns = null;
				this.chunkRowCount = 0;
			}
		}
	}

	private pushValueString(value: string): void {
		this.ensureColumns();
		if (this.shouldSkip()) return;

		let schemaIdx = -1;
		if (this.csvToSchema) {
			if (this.currentColumnIndex < this.csvToSchema.length) {
				schemaIdx = this.csvToSchema[this.currentColumnIndex] ?? -1;
			}
		} else {
			if (this.currentColumnIndex < this.schema.columnCount) {
				schemaIdx = this.currentColumnIndex;
			}
		}

		if (schemaIdx !== -1 && this.columns) {
			const col = this.columns[schemaIdx];
			if (col) {
				const dtype = this.schema.columns[schemaIdx]!.dtype;
				this.appendValueString(col, value, dtype);
			}
		}
	}

	private ensureColumns(): void {
		if (this.columns === null) {
			if (this.options.chunkSize <= 0) throw new Error("Invalid chunk size");

			if (
				this.skipRemaining > 0 ||
				(this.rowCount === 0 && this.options.hasHeader)
			)
				return;

			this.columns = new Array(this.schema.columnCount);
			for (let i = 0; i < this.schema.columnCount; i++) {
				const colDef = this.schema.columns[i]!;
				this.columns[i] = bufferPool.acquire(
					colDef.dtype.kind,
					this.options.chunkSize,
					colDef.dtype.nullable,
				);
			}
			this.chunkRowCount = 0;
		}
	}

	private shouldSkip(): boolean {
		if (this.skipRemaining > 0) return true;
		if (this.maxRemaining <= 0) return true;
		if (this.rowCount === 0 && this.options.hasHeader) return true;
		return false;
	}

	// OPTIMIZED BYTE PARSER
	private appendValueSlice(
		col: ColumnBuffer,
		data: Uint8Array,
		start: number,
		end: number,
		dtype: DType,
	): void {
		// Trim whitespace by adjusting indices
		let s = start;
		let e = end;

		// Fast path: if start and end chars are not whitespace
		if (s < e) {
			if (data[s]! <= 32) {
				while (s < e && data[s]! <= 32) s++;
			}
			if (e > s && data[e - 1]! <= 32) {
				while (e > s && data[e - 1]! <= 32) e--;
			}
		}

		if (s >= e) {
			if (dtype.nullable) {
				col.appendNull();
			} else {
				this.appendDefault(col, dtype);
			}
			return;
		}

		switch (dtype.kind) {
			case DTypeKind.Int32:
			case DTypeKind.Int16:
			case DTypeKind.Int8:
			case DTypeKind.UInt32:
			case DTypeKind.UInt16:
			case DTypeKind.UInt8: {
				// INLINED parseIntFromBytes
				let idx = s;
				// note: whitespace already trimmed

				if (idx >= e) {
					col.appendNull();
					break;
				}

				let sign = 1;
				const first = data[idx]!;
				if (first === 45) {
					// '-'
					sign = -1;
					idx++;
				} else if (first === 43) {
					// '+'
					idx++;
				}

				let val = 0;
				let hasDigits = false;

				while (idx < e) {
					const c = data[idx]!;
					if (c >= 48 && c <= 57) {
						val = val * 10 + (c - 48);
						hasDigits = true;
						idx++;
					} else {
						val = NaN;
						break;
					}
				}

				const intVal = hasDigits ? sign * val : NaN;
				if (Number.isNaN(intVal)) col.appendNull();
				else col.append(intVal as never);
				break;
			}
			case DTypeKind.Float64:
			case DTypeKind.Float32: {
				// INLINE parseFloatFromBytes
				let idx = s;
				let f = NaN;

				if (idx < e) {
					let sign = 1;
					const first = data[idx]!;
					if (first === 45) {
						// '-'
						sign = -1;
						idx++;
					} else if (first === 43) {
						// '+'
						idx++;
					}

					let val = 0;
					let hasDigits = false;

					// Integer part
					while (idx < e) {
						const c = data[idx]!;
						if (c >= 48 && c <= 57) {
							val = val * 10 + (c - 48);
							hasDigits = true;
							idx++;
						} else if (c === 46) {
							// '.'
							idx++;
							break;
						} else {
							// Exponent or invalid
							hasDigits = false;
							val = NaN;
							break;
						}
					}

					// Fraction part
					if (!Number.isNaN(val)) {
						if (idx < e) {
							let fraction = 0.1;
							while (idx < e) {
								const c = data[idx]!;
								if (c >= 48 && c <= 57) {
									val += (c - 48) * fraction;
									fraction *= 0.1;
									hasDigits = true;
									idx++;
								} else {
									hasDigits = false;
									break;
								}
							}
						}
						if (hasDigits) f = sign * val;
					}
				}

				if (Number.isNaN(f)) {
					// Fallback to string decode for scientific notation
					const str = this.decoder.decode(data.subarray(s, e));
					const f2 = parseFloat(str);
					if (Number.isNaN(f2)) col.appendNull();
					else col.append(f2 as never);
				} else {
					col.append(f as never);
				}
				break;
			}
			case DTypeKind.String: {
				const id = this.dictionary.intern(data.subarray(s, e));
				col.append(id as never);
				break;
			}
			case DTypeKind.Int64:
			case DTypeKind.UInt64:
				// BigInt handles whitespace but we already trimmed.
				// Still need to decode to string for BigInt constructor
				try {
					const str = this.decoder.decode(data.subarray(s, e));
					col.append(BigInt(str) as never);
				} catch {
					col.append(0n as never);
				}
				break;
			case DTypeKind.Boolean: {
				const c = data[s]!;
				let b = false;
				if (c === 49)
					b = true; // '1'
				else if (c === 84 || c === 116)
					b = true; // 'T' or 't'
				else if (c === 89 || c === 121) b = true; // 'Y' or 'y'
				col.append((b ? 1 : 0) as never);
				break;
			}
			case DTypeKind.Timestamp: {
				try {
					const str = this.decoder.decode(data.subarray(s, e));
					const ts = BigInt(Date.parse(str));
					col.append(ts);
				} catch {
					col.append(0n);
				}
				break;
			}
			default:
				col.appendNull();
		}
	}

	private appendValueString(
		col: ColumnBuffer,
		value: string,
		dtype: DType,
	): void {
		const trimmed = value.trim();

		if (trimmed === "" && dtype.nullable) {
			col.appendNull();
			return;
		}

		try {
			switch (dtype.kind) {
				case DTypeKind.String:
					// TODO: Use AdaptiveStringColumn for automatic StringView/Dictionary selection
					// based on cardinality. For high-cardinality strings (e.g., URLs, IDs),
					// StringView would be more memory-efficient than dictionary encoding.
					col.append(this.dictionary.internString(trimmed) as never);
					break;
				case DTypeKind.Int32:
				case DTypeKind.Int16:
				case DTypeKind.Int8:
				case DTypeKind.UInt32:
				case DTypeKind.UInt16:
				case DTypeKind.UInt8: {
					const i = parseInt(trimmed, 10);
					if (Number.isNaN(i)) col.appendNull();
					else col.append(i as never);
					break;
				}
				case DTypeKind.Float64:
				case DTypeKind.Float32: {
					const f = parseFloat(trimmed);
					if (Number.isNaN(f)) col.appendNull();
					else col.append(f as never);
					break;
				}
				case DTypeKind.Boolean: {
					const b =
						trimmed.toLowerCase() === "true" ||
						trimmed === "1" ||
						trimmed.toLowerCase() === "yes";
					col.append((b ? 1 : 0) as never);
					break;
				}
				case DTypeKind.Int64:
				case DTypeKind.UInt64:
					col.append(BigInt(trimmed) as never);
					break;
				case DTypeKind.Timestamp: {
					const ts = BigInt(Date.parse(trimmed));
					col.append(ts);
					break;
				}
				default:
					col.appendNull();
			}
		} catch {
			col.appendNull();
		}
	}
}



export function createCsvParser(
	schema: Schema,
	options?: CsvOptions,
): CsvParser {
	return new CsvParser(schema, options);
}
