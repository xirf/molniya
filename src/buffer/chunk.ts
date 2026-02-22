/**
 * Chunk: A batch of rows stored in columnar format.
 *
 * A Chunk is the unit of data that flows through the pipeline.
 * It contains multiple columns and optionally a selection vector.
 *
 * Key properties:
 * - Columns are stored as TypedArrays for cache efficiency
 * - Selection vector enables filtering without copying data
 * - All columns have the same logical length
 */

import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import { getColumnNames, type Schema } from "../types/schema.ts";
import { ColumnBuffer, type TypedArray } from "./column-buffer.ts";
import type { DictIndex, Dictionary } from "./dictionary.ts";

/**
 * A chunk of columnar data.
 */
export class Chunk {
	/** Schema describing the columns */
	readonly schema: Schema;

	/** Column data indexed by column position */
	private readonly columns: ColumnBuffer[];

	/** Dictionary for string columns (shared across chunk) */
	readonly dictionary: Dictionary | null;

	/** Selection vector (indices of valid rows). Null means all rows are valid. */
	private selection: Uint32Array | null;

	/** Number of selected rows */
	private selectedCount: number;

	/** Actual number of rows in the underlying buffers */
	private physicalRowCount: number;

	/** Callback to release a borrowed selection buffer (for zero-copy filtering) */
	private _selectionReleaseCallback: (() => void) | null = null;

	constructor(
		schema: Schema,
		columns: ColumnBuffer[],
		dictionary: Dictionary | null = null,
	) {
		this.schema = schema;
		this.columns = columns;
		this.dictionary = dictionary;
		this.selection = null;
		this.physicalRowCount = columns.length > 0 ? (columns[0]?.length ?? 0) : 0;
		this.selectedCount = this.physicalRowCount;
	}

	/** Number of rows (after selection) */
	get rowCount(): number {
		return this.selectedCount;
	}

	/** Number of columns */
	get columnCount(): number {
		return this.schema.columnCount;
	}

	/** Get column buffer by index */
	getColumn(index: number): ColumnBuffer | undefined {
		return this.columns[index];
	}

	/** Get column buffer by name */
	getColumnByName(name: string): ColumnBuffer | undefined {
		const idx = this.schema.columnMap.get(name);
		if (idx === undefined) return undefined;
		return this.columns[idx];
	}

	/** Check if a selection vector is active */
	hasSelection(): boolean {
		return this.selection !== null;
	}

	/** Get the selection vector (or null if all rows selected) */
	getSelection(): Uint32Array | null {
		return this.selection;
	}

	/** Get raw columns (internal access for operators) */
	getColumns(): ColumnBuffer[] {
		return this.columns;
	}

	/** Get the number of physical (unselected) rows in underlying buffers */
	getPhysicalRowCount(): number {
		return this.physicalRowCount;
	}

	/**
	 * Apply a selection vector.
	 * This doesn't copy data; it just marks which rows are valid.
	 */
	applySelection(selection: Uint32Array, count: number): void {
		if (this.selection === null) {
			// First selection - copy to avoid aliasing issues with pooled buffers
			this.selection = selection.slice(0, count);
			this.selectedCount = count;
		} else {
			// Compose selections: map through existing selection
			const newSelection = new Uint32Array(count);
			for (let i = 0; i < count; i++) {
				const idx = selection[i] ?? 0;
				newSelection[i] = this.selection[idx] ?? 0;
			}
			this.selection = newSelection;
			this.selectedCount = count;
		}
	}

	/**
	 * Create a new Chunk view with the given selection applied.
	 * Shares the same underlying column data (zero-copy) but has its own selection.
	 * This is the non-mutating alternative to applySelection.
	 */
	withSelection(selection: Uint32Array, count: number): Chunk {
		const view = new Chunk(this.schema, this.columns, this.dictionary);
		if (this.selection !== null) {
			// Compose: map new selection through existing selection
			const composed = new Uint32Array(count);
			for (let i = 0; i < count; i++) {
				const idx = selection[i] ?? 0;
				composed[i] = this.selection[idx] ?? 0;
			}
			view.selection = composed;
		} else {
			view.selection = selection.slice(0, count);
		}
		view.selectedCount = count;
		view.physicalRowCount = this.physicalRowCount;
		return view;
	}

	/**
	 * Create a new Chunk view that borrows the selection buffer (zero-copy).
	 * Instead of copying the selection array, it directly references the provided buffer.
	 * When the chunk is no longer needed, call the releaseCallback to return the buffer.
	 * 
	 * This avoids per-chunk allocation for filtering hot paths.
	 * The caller MUST NOT modify the selection buffer while this chunk is alive.
	 */
	withSelectionBorrowed(
		selection: Uint32Array,
		count: number,
		releaseCallback: () => void,
	): Chunk {
		const view = new Chunk(this.schema, this.columns, this.dictionary);
		if (this.selection !== null) {
			// Must compose — can't borrow when composing with an existing selection
			const composed = new Uint32Array(count);
			for (let i = 0; i < count; i++) {
				const idx = selection[i] ?? 0;
				composed[i] = this.selection[idx] ?? 0;
			}
			view.selection = composed;
			// Release borrowed buffer immediately since we didn't use it
			releaseCallback();
		} else {
			// Zero-copy: directly borrow the buffer subarray
			view.selection = selection.subarray(0, count);
			view._selectionReleaseCallback = releaseCallback;
		}
		view.selectedCount = count;
		view.physicalRowCount = this.physicalRowCount;
		return view;
	}

	/** Clear selection (all rows become valid again) */
	clearSelection(): void {
		this.selection = null;
		this.selectedCount = this.physicalRowCount;
	}

	/**
	 * Release transient resources (e.g., borrowed selection buffers)
	 * without disposing column buffers.
	 */
	releaseTransientResources(): void {
		if (this._selectionReleaseCallback) {
			this._selectionReleaseCallback();
			this._selectionReleaseCallback = null;
		}
	}

	/**
	 * Get the physical row index for a logical row.
	 * If no selection, physical = logical.
	 */
	physicalIndex(logicalIndex: number): number {
		if (this.selection === null) {
			return logicalIndex;
		}
		return this.selection[logicalIndex] ?? logicalIndex;
	}

	/**
	 * Get a value from the chunk.
	 * Returns the value at the logical row index (respects selection).
	 */
	getValue(
		columnIndex: number,
		rowIndex: number,
	): TypedArray[number] | undefined {
		const column = this.columns[columnIndex];
		if (column === undefined) return undefined;

		const physIdx = this.physicalIndex(rowIndex);
		return column.get(physIdx);
	}

	/**
	 * Check if a cell is null.
	 */
	isNull(columnIndex: number, rowIndex: number): boolean {
		const column = this.columns[columnIndex];
		if (column === undefined) return true;

		const physIdx = this.physicalIndex(rowIndex);
		return column.isNull(physIdx);
	}

	/**
	 * Get string value (resolves dictionary index OR returns direct value).
	 */
	getStringValue(columnIndex: number, rowIndex: number): string | undefined {
		const column = this.columns[columnIndex];
		if (column === undefined) return undefined;

		const physIdx = this.physicalIndex(rowIndex);

		if (column.kind === DTypeKind.StringView) {
			return (column as unknown as import("./string-view-column.ts").StringViewColumnBuffer).getString(physIdx);
		}

		if (column.kind !== DTypeKind.String) return undefined;
		const value = column.get(physIdx);

		// If dictionary exists, dereference index
		if (this.dictionary !== null) {
			return this.dictionary.getString(value as DictIndex);
		}

		// Direct string value (e.g., from parquet)
		return value as unknown as string;
	}

	/**
	 * Get raw bytes value for zero-copy binary comparisons (only valid for StringView).
	 */
	getBytesValue(columnIndex: number, rowIndex: number): Uint8Array | undefined {
		const column = this.columns[columnIndex];
		if (column === undefined) return undefined;
		if (column.kind === DTypeKind.StringView) {
			const physIdx = this.physicalIndex(rowIndex);
			return (column as unknown as import("./string-view-column.ts").StringViewColumnBuffer).getBytes(physIdx);
		}
		return undefined;
	}

	/**
	 * Materialize the chunk into a compact form (apply selection).
	 * Returns a new Chunk with the selection baked in.
	 */
	materialize(): Result<Chunk> {
		if (this.selection === null) {
			// No selection, return self
			return ok(this);
		}

		const newColumns: ColumnBuffer[] = [];

		for (let i = 0; i < this.columns.length; i++) {
			const srcCol = this.columns[i];
			if (!srcCol) continue;
			const dstCol = new ColumnBuffer(
				srcCol.kind,
				this.selectedCount,
				srcCol.isNullable,
			);

			const error = dstCol.copySelected(
				srcCol,
				this.selection,
				this.selectedCount,
			);
			if (error !== ErrorCode.None) {
				return err(error);
			}

			newColumns.push(dstCol);
		}

		return ok(new Chunk(this.schema, newColumns, this.dictionary));
	}

	/**
	 * Create an iterator over rows (for debugging/display).
	 *
	 * ⚠️ PERFORMANCE WARNING: This allocates a new object for EVERY row!
	 * This completely defeats the zero-copy columnar architecture and will
	 * cause massive GC pressure on large datasets. Only use for:
	 * - Debugging (inspecting a few rows)
	 * - Display/preview (limited row count)
	 * - Small datasets where performance doesn't matter
	 *
	 * For performance-critical iteration, use:
	 * - rowsReusable() - Reuses the same object (flyweight pattern)
	 * - Direct columnar access via getColumn()
	 * - Operators that work on chunks directly
	 */
	*rows(): IterableIterator<Record<string, unknown>> {
		const columnNames = getColumnNames(this.schema);

		for (let i = 0; i < this.rowCount; i++) {
			const row: Record<string, unknown> = {};
			for (let j = 0; j < this.columnCount; j++) {
				const name = columnNames[j];
				const col = this.columns[j];
				if (!name || !col) continue;

				if (this.isNull(j, i)) {
					row[name] = null;
				} else if (col.kind === DTypeKind.String && this.dictionary) {
					row[name] = this.getStringValue(j, i);
				} else {
					row[name] = this.getValue(j, i);
				}
			}
			yield row;
		}
	}

	/**
	 * Memory-efficient row iterator using flyweight pattern.
	 * Reuses the same row object, mutating it on each iteration.
	 *
	 * ⚠️ WARNING: The returned object is MUTATED on each iteration!
	 * DO NOT store references to it. If you need to keep a row, clone it:
	 *   const copy = { ...row };
	 *
	 * Use this for performance-critical iteration over large datasets
	 * where you process each row and don't need to keep references.
	 *
	 * Example:
	 *   for (const row of chunk.rowsReusable()) {
	 *     // Process row immediately
	 *     const sum = row.a + row.b;
	 *     // DON'T store: rows.push(row) ❌
	 *     // DO clone first: rows.push({ ...row }) ✅
	 *   }
	 */
	*rowsReusable(): IterableIterator<Record<string, unknown>> {
		const columnNames = getColumnNames(this.schema);
		const row: Record<string, unknown> = {}; // Reused object

		for (let i = 0; i < this.rowCount; i++) {
			// Mutate the same object
			for (let j = 0; j < this.columnCount; j++) {
				const name = columnNames[j];
				const col = this.columns[j];
				if (!name || !col) continue;

				if (this.isNull(j, i)) {
					row[name] = null;
				} else if (col.kind === DTypeKind.String && this.dictionary) {
					row[name] = this.getStringValue(j, i);
				} else {
					row[name] = this.getValue(j, i);
				}
			}
			yield row;
		}
	}

	/**
	 * Release columns for recycling and clear this chunk.
	 */
	dispose(): ColumnBuffer[] {
		// Release borrowed selection buffer if present
		this.releaseTransientResources();
		const cols = this.columns;
		// Clear references
		(this as unknown as { columns: ColumnBuffer[] }).columns = [];
		return cols;
	}
}

/**
 * Create an empty chunk with the given schema.
 */
export function createEmptyChunk(
	schema: Schema,
	capacity: number,
	dictionary: Dictionary | null = null,
): Result<Chunk> {
	if (capacity <= 0) {
		return err(ErrorCode.InvalidCapacity);
	}

	const columns: ColumnBuffer[] = [];

	for (const colDef of schema.columns) {
		const buffer = new ColumnBuffer(
			colDef.dtype.kind,
			capacity,
			colDef.dtype.nullable,
		);
		columns.push(buffer);
	}

	return ok(new Chunk(schema, columns, dictionary));
}

/**
 * Create a chunk from arrays of data.
 */
export function createChunkFromArrays(
	schema: Schema,
	data: TypedArray[],
	dictionary: Dictionary | null = null,
): Result<Chunk> {
	if (data.length !== schema.columnCount) {
		return err(ErrorCode.SchemaMismatch);
	}

	// Validate all columns have same length
	if (data.length > 0) {
		const firstCol = data[0];
		const length = firstCol ? firstCol.length : 0;
		for (let i = 1; i < data.length; i++) {
			if ((data[i]?.length ?? -1) !== length) {
				return err(ErrorCode.SchemaMismatch);
			}
		}
	}

	const columns: ColumnBuffer[] = [];

	for (let i = 0; i < schema.columns.length; i++) {
		const colDef = schema.columns[i];
		const arr = data[i];
		if (!colDef || !arr) continue;
		const buffer = new ColumnBuffer(
			colDef.dtype.kind,
			arr.length,
			colDef.dtype.nullable,
		);

		// Copy data
		// biome-ignore lint/suspicious/noExplicitAny: Generic casting
		(buffer.data as TypedArray).set(arr as any);
		(buffer as unknown as { _length: number })._length = arr.length;

		columns.push(buffer);
	}

	return ok(new Chunk(schema, columns, dictionary));
}
