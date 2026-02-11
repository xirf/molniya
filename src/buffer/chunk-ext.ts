/**
 * EnhancedChunk: Extends Chunk to support StringView and mixed string encodings.
 *
 * This is a wrapper around the base Chunk class that adds support for:
 * - StringView columns (UTF-8 bytes, not dictionary-encoded)
 * - Mixed encodings (Dictionary + StringView in same chunk)
 * - Seamless access to string values regardless of encoding
 */

import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import type { Schema } from "../types/schema.ts";
import { ColumnBuffer, type TypedArray } from "./column-buffer.ts";
import { Chunk } from "./chunk.ts";
import type { Dictionary } from "./dictionary.ts";
import { StringView } from "./string-view.ts";
import {
	ColumnStorageAdapter,
	fromColumnBuffer,
	fromStringView,
	type ColumnStorage,
} from "./column-storage.ts";

/**
 * Enhanced chunk that supports multiple string storage strategies.
 */
export class EnhancedChunk {
	/** Underlying base chunk (for non-StringView columns) */
	private readonly baseChunk: Chunk;

	/** Additional column storage adapters (includes both base + StringView columns) */
	private readonly columnAdapters: ColumnStorageAdapter[];

	/** Number of rows */
	private readonly rows: number;

	constructor(
		schema: Schema,
		columns: ColumnBuffer[],
		dictionary: Dictionary | null = null,
		stringViews: Map<number, StringView> = new Map()
	) {
		// Create base chunk with ColumnBuffer columns only
		this.baseChunk = new Chunk(schema, columns, dictionary);
		this.rows = columns.length > 0 ? (columns[0]?.length ?? 0) : 0;

		// Build column adapters
		this.columnAdapters = [];
		for (let i = 0; i < schema.columnCount; i++) {
			const stringView = stringViews.get(i);
			if (stringView) {
				// StringView column
				const kind = schema.columns[i]?.kind ?? DTypeKind.StringView;
				this.columnAdapters.push(fromStringView(stringView, kind));
			} else {
				// ColumnBuffer column
				const buffer = columns[i];
				if (buffer) {
					this.columnAdapters.push(fromColumnBuffer(buffer, dictionary));
				}
			}
		}
	}

	/** Get the schema */
	get schema(): Schema {
		return this.baseChunk.schema;
	}

	/** Number of rows */
	get rowCount(): number {
		return this.baseChunk.rowCount;
	}

	/** Number of columns */
	get columnCount(): number {
		return this.baseChunk.columnCount;
	}

	/** Get underlying Chunk (for compatibility with existing operators) */
	getBaseChunk(): Chunk {
		return this.baseChunk;
	}

	/**
	 * Get column storage adapter by index.
	 */
	getColumnAdapter(index: number): ColumnStorageAdapter | undefined {
		return this.columnAdapters[index];
	}

	/**
	 * Check if a cell is null.
	 */
	isNull(columnIndex: number, rowIndex: number): boolean {
		const adapter = this.columnAdapters[columnIndex];
		if (!adapter) return true;

		// Respect selection vector from base chunk
		const physIdx = this.baseChunk.physicalIndex(rowIndex);
		return adapter.isNull(physIdx);
	}

	/**
	 * Get string value (works for both Dictionary and StringView encodings).
	 */
	getStringValue(columnIndex: number, rowIndex: number): string | undefined {
		const adapter = this.columnAdapters[columnIndex];
		if (!adapter) return undefined;

		// Respect selection vector from base chunk
		const physIdx = this.baseChunk.physicalIndex(rowIndex);
		return adapter.getStringValue(physIdx);
	}

	/**
	 * Get raw value (for non-string columns).
	 */
	getValue(
		columnIndex: number,
		rowIndex: number
	): TypedArray[number] | undefined {
		// Delegate to base chunk (respects selection)
		return this.baseChunk.getValue(columnIndex, rowIndex);
	}

	/**
	 * Apply a selection vector.
	 */
	applySelection(selection: Uint32Array, count: number): void {
		this.baseChunk.applySelection(selection, count);
	}

	/**
	 * Clear selection.
	 */
	clearSelection(): void {
		this.baseChunk.clearSelection();
	}

	/**
	 * Check if a selection vector is active.
	 */
	hasSelection(): boolean {
		return this.baseChunk.hasSelection();
	}

	/**
	 * Get the selection vector.
	 */
	getSelection(): Uint32Array | null {
		return this.baseChunk.getSelection();
	}

	/**
	 * Iterate over selected rows.
	 */
	*iterateRows(): Generator<Map<string, any>> {
		const colNames = this.schema.columns.map((col) => col.name);

		for (let i = 0; i < this.rowCount; i++) {
			const row = new Map<string, any>();

			for (let j = 0; j < this.columnCount; j++) {
				const name = colNames[j];
				if (!name) continue;

				if (this.isNull(j, i)) {
					row.set(name, null);
				} else {
					const adapter = this.columnAdapters[j];
					if (!adapter) continue;

					const kind = adapter.getKind();
					if (
						kind === DTypeKind.String ||
						kind === DTypeKind.StringDict ||
						kind === DTypeKind.StringView
					) {
						row.set(name, this.getStringValue(j, i));
					} else {
						row.set(name, this.getValue(j, i));
					}
				}
			}

			yield row;
		}
	}
}

/**
 * Builder for creating EnhancedChunks.
 */
export class EnhancedChunkBuilder {
	private schema: Schema | null = null;
	private columns: ColumnBuffer[] = [];
	private dictionary: Dictionary | null = null;
	private stringViews = new Map<number, StringView>();

	/**
	 * Set the schema.
	 */
	withSchema(schema: Schema): this {
		this.schema = schema;
		return this;
	}

	/**
	 * Add a ColumnBuffer column.
	 */
	addColumn(index: number, buffer: ColumnBuffer): this {
		this.columns[index] = buffer;
		return this;
	}

	/**
	 * Add a StringView column.
	 */
	addStringView(index: number, view: StringView): this {
		this.stringViews.set(index, view);
		return this;
	}

	/**
	 * Set the shared dictionary.
	 */
	withDictionary(dict: Dictionary): this {
		this.dictionary = dict;
		return this;
	}

	/**
	 * Build the EnhancedChunk.
	 */
	build(): Result<EnhancedChunk> {
		if (!this.schema) {
			return err(ErrorCode.InvalidArgument, "Schema is required");
		}

		// Determine row count from first available column (either ColumnBuffer or StringView)
		let rowCount = 0;
		for (let i = 0; i < this.schema.columnCount; i++) {
			const view = this.stringViews.get(i);
			const buffer = this.columns[i];
			if (view) {
				rowCount = view.length;
				break;
			} else if (buffer) {
				rowCount = buffer.length;
				break;
			}
		}

		// For StringView columns, create minimal placeholder ColumnBuffers
		// These won't be used for data access, but Chunk needs them for row count consistency
		const finalColumns: ColumnBuffer[] = [];
		for (let i = 0; i < this.schema.columnCount; i++) {
			if (this.stringViews.has(i)) {
				// StringView column - create minimal placeholder
				// Use Int8 (smallest type) with rowCount capacity
				const placeholder = new ColumnBuffer(DTypeKind.Int8, rowCount, false);
				// Set length to match actual data rows
				placeholder.setLength(rowCount);
				finalColumns[i] = placeholder;
			} else {
				// Normal ColumnBuffer
				const buffer = this.columns[i];
				if (!buffer) {
					return err(ErrorCode.InvalidArgument, `Missing column at index ${i}`);
				}
				finalColumns[i] = buffer;
			}
		}

		const chunk = new EnhancedChunk(
			this.schema,
			finalColumns,
			this.dictionary,
			this.stringViews
		);

		return ok(chunk);
	}
}
