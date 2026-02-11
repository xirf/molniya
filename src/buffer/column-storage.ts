/**
 * Enhanced column storage that can hold either ColumnBuffer or StringView.
 * 
 * This is an internal abstraction used by EnhancedChunk to support
 * multiple string encodings within the same chunk.
 */

import { ColumnBuffer, type TypedArray } from "./column-buffer.ts";
import { StringView } from "./string-view.ts";
import { DTypeKind } from "../types/dtypes.ts";
import type { Dictionary } from "./dictionary.ts";

/** Union type for column storage */
export type ColumnStorage = ColumnBuffer | StringView;

/**
 * Wrapper that provides unified interface over different column storage types.
 */
export class ColumnStorageAdapter {
	private readonly storage: ColumnStorage;
	private readonly kind: DTypeKind;
	private readonly dictionary: Dictionary | null;

	constructor(
		storage: ColumnStorage,
		kind: DTypeKind,
		dictionary: Dictionary | null = null
	) {
		this.storage = storage;
		this.kind = kind;
		this.dictionary = dictionary;
	}

	/** Get the underlying storage */
	getStorage(): ColumnStorage {
		return this.storage;
	}

	/** Check if this is a StringView column */
	isStringView(): boolean {
		return this.storage instanceof StringView;
	}

	/** Check if this is a ColumnBuffer */
	isColumnBuffer(): boolean {
		return this.storage instanceof ColumnBuffer;
	}

	/** Get number of rows */
	get length(): number {
		return this.storage.length;
	}

	/** Check if value at index is null */
	isNull(index: number): boolean {
		return this.storage.isNull(index);
	}

	/**
	 * Get string value at index.
	 * Handles both Dictionary and StringView encodings.
	 */
	getStringValue(index: number): string | undefined {
		if (this.isNull(index)) {
			return undefined;
		}

		if (this.storage instanceof StringView) {
			// StringView column - direct access
			return this.storage.getString(index);
		} else if (this.storage instanceof ColumnBuffer) {
			// ColumnBuffer - might be dictionary-encoded
			if (this.kind === DTypeKind.String || this.kind === DTypeKind.StringDict) {
				const dictIndex = this.storage.get(index) as number;
				if (this.dictionary) {
					return this.dictionary.getString(dictIndex);
				}
			}
		}

		return undefined;
	}

	/**
	 * Get raw value at index (for non-string columns).
	 */
	getValue(index: number): TypedArray[number] | undefined {
		if (this.storage instanceof ColumnBuffer) {
			return this.storage.get(index);
		}
		return undefined;
	}

	/**
	 * Get the DType kind of this column.
	 */
	getKind(): DTypeKind {
		return this.kind;
	}
}

/**
 * Create a column storage adapter from a ColumnBuffer.
 */
export function fromColumnBuffer(
	buffer: ColumnBuffer,
	dictionary: Dictionary | null = null
): ColumnStorageAdapter {
	return new ColumnStorageAdapter(buffer, buffer.kind, dictionary);
}

/**
 * Create a column storage adapter from a StringView.
 */
export function fromStringView(
	view: StringView,
	kind: DTypeKind = DTypeKind.StringView
): ColumnStorageAdapter {
	return new ColumnStorageAdapter(view, kind, null);
}