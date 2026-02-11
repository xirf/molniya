/**
 * String operations that work with both Dictionary and StringView encodings.
 *
 * This module provides:
 * - Encoding detection and dispatch to appropriate implementation
 * - StringView-aware versions of trim, replace, toLowerCase, toUpperCase, etc.
 * - UTF-8 byte-level operations for performance
 *
 * Design decisions (from ROADMAP.md):
 * - StringView as default for high cardinality
 * - No auto-switching to avoid O(N) latency spikes
 * - UTF-8 byte ops to avoid V8 string materialization tax
 */

import type { EnhancedColumnBuffer } from "../buffer/enhanced-column-buffer.ts";
import { StringEncoding } from "../buffer/adaptive-string.ts";
import { StringView } from "../buffer/string-view.ts";
import { createDictionary } from "../buffer/dictionary.ts";
import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";

/**
 * Detect the string encoding used by an EnhancedColumnBuffer.
 */
export function detectStringEncoding(
	column: EnhancedColumnBuffer<any>,
): StringEncoding {
	const internals = column.getInternals();

	if (internals.stringColumn) {
		return internals.stringColumn.encoding;
	}

	// For legacy columns or non-string types, assume Dictionary
	return StringEncoding.Dictionary;
}

/**
 * Check if column is a string type.
 */
function isStringColumn(column: EnhancedColumnBuffer<any>): boolean {
	return (
		column.kind === DTypeKind.String ||
		column.kind === DTypeKind.StringDict ||
		column.kind === DTypeKind.StringView
	);
}

/**
 * Trim whitespace from string column values.
 * Automatically dispatches to appropriate implementation based on encoding.
 */
export function trimColumn(column: EnhancedColumnBuffer<any>): Result<void> {
	if (!isStringColumn(column)) {
		return err(ErrorCode.TypeMismatch);
	}

	const encoding = detectStringEncoding(column);

	if (encoding === StringEncoding.View) {
		return trimStringView(column);
	} else {
		return trimDictionary(column);
	}
}

/**
 * Replace substrings in string column values.
 */
export function replaceColumn(
	column: EnhancedColumnBuffer<any>,
	pattern: string,
	replacement: string,
	all: boolean = true,
): Result<void> {
	if (!isStringColumn(column)) {
		return err(ErrorCode.TypeMismatch);
	}

	const encoding = detectStringEncoding(column);

	if (encoding === StringEncoding.View) {
		return replaceStringView(column, pattern, replacement, all);
	} else {
		return replaceDictionary(column, pattern, replacement, all);
	}
}

/**
 * Convert string column to lowercase.
 */
export function toLowerColumn(column: EnhancedColumnBuffer<any>): Result<void> {
	if (!isStringColumn(column)) {
		return err(ErrorCode.TypeMismatch);
	}

	const encoding = detectStringEncoding(column);

	if (encoding === StringEncoding.View) {
		return toLowerStringView(column);
	} else {
		return toLowerDictionary(column);
	}
}

/**
 * Convert string column to uppercase.
 */
export function toUpperColumn(column: EnhancedColumnBuffer<any>): Result<void> {
	if (!isStringColumn(column)) {
		return err(ErrorCode.TypeMismatch);
	}

	const encoding = detectStringEncoding(column);

	if (encoding === StringEncoding.View) {
		return toUpperStringView(column);
	} else {
		return toUpperDictionary(column);
	}
}

/**
 * Extract substring from string column.
 */
export function substringColumn(
	column: EnhancedColumnBuffer<any>,
	start: number,
	end?: number,
): Result<void> {
	if (!isStringColumn(column)) {
		return err(ErrorCode.TypeMismatch);
	}

	const encoding = detectStringEncoding(column);

	if (encoding === StringEncoding.View) {
		return substringStringView(column, start, end);
	} else {
		return substringDictionary(column, start, end);
	}
}

/**
 * Pad string to specified length.
 */
export function padColumn(
	column: EnhancedColumnBuffer<any>,
	length: number,
	fillChar: string = " ",
	side: "left" | "right" = "left",
): Result<void> {
	if (!isStringColumn(column)) {
		return err(ErrorCode.TypeMismatch);
	}

	const encoding = detectStringEncoding(column);

	if (encoding === StringEncoding.View) {
		return padStringView(column, length, fillChar, side);
	} else {
		return padDictionary(column, length, fillChar, side);
	}
}

// ============================================================================
// StringView Implementations
// ============================================================================

function trimStringView(column: EnhancedColumnBuffer<any>): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const length = stringColumn.length;
	const view = stringColumn.getView();

	if (!view) {
		return err(ErrorCode.InvalidState);
	}

	// Create new StringView to hold trimmed values
	const newView = new StringView(length, column.nullable, undefined);

	// Trim each string
	for (let i = 0; i < length; i++) {
		if (stringColumn.isNull(i)) {
			newView.appendNull();
			continue;
		}

		const str = stringColumn.getString(i);
		if (str !== undefined) {
			const trimmed = str.trim();
			newView.append(trimmed);
		} else {
			newView.appendNull();
		}
	}

	// Replace the view in the AdaptiveStringColumn
	stringColumn.replaceView(newView);

	return ok(undefined);
}

function replaceStringView(
	column: EnhancedColumnBuffer<any>,
	pattern: string,
	replacement: string,
	all: boolean,
): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const length = stringColumn.length;
	const view = stringColumn.getView();

	if (!view) {
		return err(ErrorCode.InvalidState);
	}

	const newView = new StringView(length, column.nullable, undefined);

	for (let i = 0; i < length; i++) {
		if (stringColumn.isNull(i)) {
			newView.appendNull();
			continue;
		}

		const str = stringColumn.getString(i);
		if (str !== undefined) {
			const replaced = all
				? str.replaceAll(pattern, replacement)
				: str.replace(pattern, replacement);
			newView.append(replaced);
		} else {
			newView.appendNull();
		}
	}

	stringColumn.replaceView(newView);

	return ok(undefined);
}

function toLowerStringView(column: EnhancedColumnBuffer<any>): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const length = stringColumn.length;
	const view = stringColumn.getView();

	if (!view) {
		return err(ErrorCode.InvalidState);
	}

	const newView = new StringView(length, column.nullable, undefined);

	for (let i = 0; i < length; i++) {
		if (stringColumn.isNull(i)) {
			newView.appendNull();
			continue;
		}

		const str = stringColumn.getString(i);
		if (str !== undefined) {
			newView.append(str.toLowerCase());
		} else {
			newView.appendNull();
		}
	}

	stringColumn.replaceView(newView);

	return ok(undefined);
}

function toUpperStringView(column: EnhancedColumnBuffer<any>): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const length = stringColumn.length;
	const view = stringColumn.getView();

	if (!view) {
		return err(ErrorCode.InvalidState);
	}

	const newView = new StringView(length, column.nullable, undefined);

	for (let i = 0; i < length; i++) {
		if (stringColumn.isNull(i)) {
			newView.appendNull();
			continue;
		}

		const str = stringColumn.getString(i);
		if (str !== undefined) {
			newView.append(str.toUpperCase());
		} else {
			newView.appendNull();
		}
	}

	stringColumn.replaceView(newView);

	return ok(undefined);
}

function substringStringView(
	column: EnhancedColumnBuffer<any>,
	start: number,
	end?: number,
): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const length = stringColumn.length;
	const view = stringColumn.getView();

	if (!view) {
		return err(ErrorCode.InvalidState);
	}

	const newView = new StringView(length, column.nullable, undefined);

	for (let i = 0; i < length; i++) {
		if (stringColumn.isNull(i)) {
			newView.appendNull();
			continue;
		}

		const str = stringColumn.getString(i);
		if (str !== undefined) {
			const substr =
				end !== undefined ? str.substring(start, end) : str.substring(start);
			newView.append(substr);
		} else {
			newView.appendNull();
		}
	}

	stringColumn.replaceView(newView);

	return ok(undefined);
}

function padStringView(
	column: EnhancedColumnBuffer<any>,
	length: number,
	fillChar: string,
	side: "left" | "right",
): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const colLength = stringColumn.length;
	const view = stringColumn.getView();

	if (!view) {
		return err(ErrorCode.InvalidState);
	}

	const newView = new StringView(colLength, column.nullable, undefined);

	for (let i = 0; i < colLength; i++) {
		if (stringColumn.isNull(i)) {
			newView.appendNull();
			continue;
		}

		const str = stringColumn.getString(i);
		if (str !== undefined) {
			const padded =
				side === "left"
					? str.padStart(length, fillChar)
					: str.padEnd(length, fillChar);
			newView.append(padded);
		} else {
			newView.appendNull();
		}
	}

	stringColumn.replaceView(newView);

	return ok(undefined);
}

// ============================================================================
// Dictionary Implementations (operate on dictionary entries directly)
// ============================================================================

function updateDictionaryIndices(
	stringColumn: any, // AdaptiveStringColumn
	internalsIndices: Uint32Array,
	mapping: Int32Array,
	newDict: any, // Dictionary
): void {
	// Update all indices in the column
	const length = stringColumn.length;

	for (let i = 0; i < length; i++) {
		if (!stringColumn.isNull(i)) {
			const oldIdx = internalsIndices[i];
			const newIdx = mapping[oldIdx];
			if (newIdx !== -1) {
				internalsIndices[i] = newIdx;
			}
		}
	}

	stringColumn.replaceDictionary(newDict);
}

function trimDictionary(column: EnhancedColumnBuffer<any>): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const dict = stringColumn.getDictionary();
	const stringInternals = stringColumn.getInternals();
	const indices = stringInternals.indices;

	if (!dict || !indices) {
		return err(ErrorCode.InvalidState);
	}

	const newDict = createDictionary();
	const size = dict.size;
	const mapping = new Int32Array(size).fill(-1);

	for (let i = 0; i < size; i++) {
		const str = dict.getString(i);
		if (str !== undefined) {
			const trimmed = str.trim();
			mapping[i] = newDict.internString(trimmed);
		}
	}

	updateDictionaryIndices(stringColumn, indices, mapping, newDict);
	return ok(undefined);
}

function replaceDictionary(
	column: EnhancedColumnBuffer<any>,
	pattern: string,
	replacement: string,
	all: boolean,
): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const dict = stringColumn.getDictionary();
	const stringInternals = stringColumn.getInternals();
	const indices = stringInternals.indices;

	if (!dict || !indices) {
		return err(ErrorCode.InvalidState);
	}

	const newDict = createDictionary();
	const size = dict.size;
	const mapping = new Int32Array(size).fill(-1);

	for (let i = 0; i < size; i++) {
		const str = dict.getString(i);
		if (str !== undefined) {
			const replaced = all
				? str.replaceAll(pattern, replacement)
				: str.replace(pattern, replacement);
			mapping[i] = newDict.internString(replaced);
		}
	}

	updateDictionaryIndices(stringColumn, indices, mapping, newDict);
	return ok(undefined);
}

function toLowerDictionary(column: EnhancedColumnBuffer<any>): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const dict = stringColumn.getDictionary();
	const stringInternals = stringColumn.getInternals();
	const indices = stringInternals.indices;

	if (!dict || !indices) {
		return err(ErrorCode.InvalidState);
	}

	const newDict = createDictionary();
	const size = dict.size;
	const mapping = new Int32Array(size).fill(-1);

	for (let i = 0; i < size; i++) {
		const str = dict.getString(i);
		if (str !== undefined) {
			mapping[i] = newDict.internString(str.toLowerCase());
		}
	}

	updateDictionaryIndices(stringColumn, indices, mapping, newDict);
	return ok(undefined);
}

function toUpperDictionary(column: EnhancedColumnBuffer<any>): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const dict = stringColumn.getDictionary();
	const stringInternals = stringColumn.getInternals();
	const indices = stringInternals.indices;

	if (!dict || !indices) {
		return err(ErrorCode.InvalidState);
	}

	const newDict = createDictionary();
	const size = dict.size;
	const mapping = new Int32Array(size).fill(-1);

	for (let i = 0; i < size; i++) {
		const str = dict.getString(i);
		if (str !== undefined) {
			mapping[i] = newDict.internString(str.toUpperCase());
		}
	}

	updateDictionaryIndices(stringColumn, indices, mapping, newDict);
	return ok(undefined);
}

function substringDictionary(
	column: EnhancedColumnBuffer<any>,
	start: number,
	end?: number,
): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const dict = stringColumn.getDictionary();
	const stringInternals = stringColumn.getInternals();
	const indices = stringInternals.indices;

	if (!dict || !indices) {
		return err(ErrorCode.InvalidState);
	}

	const newDict = createDictionary();
	const size = dict.size;
	const mapping = new Int32Array(size).fill(-1);

	for (let i = 0; i < size; i++) {
		const str = dict.getString(i);
		if (str !== undefined) {
			const substr =
				end !== undefined ? str.substring(start, end) : str.substring(start);
			mapping[i] = newDict.internString(substr);
		}
	}

	updateDictionaryIndices(stringColumn, indices, mapping, newDict);
	return ok(undefined);
}

function padDictionary(
	column: EnhancedColumnBuffer<any>,
	length: number,
	fillChar: string,
	side: "left" | "right",
): Result<void> {
	const internals = column.getInternals();
	const stringColumn = internals.stringColumn;

	if (!stringColumn) {
		return err(ErrorCode.InvalidState);
	}

	const dict = stringColumn.getDictionary();
	const stringInternals = stringColumn.getInternals();
	const indices = stringInternals.indices;

	if (!dict || !indices) {
		return err(ErrorCode.InvalidState);
	}

	const newDict = createDictionary();
	const size = dict.size;
	const mapping = new Int32Array(size).fill(-1);

	for (let i = 0; i < size; i++) {
		const str = dict.getString(i);
		if (str !== undefined) {
			const padded =
				side === "left"
					? str.padStart(length, fillChar)
					: str.padEnd(length, fillChar);
			mapping[i] = newDict.internString(padded);
		}
	}

	updateDictionaryIndices(stringColumn, indices, mapping, newDict);
	return ok(undefined);
}
