/**
 * UTF-8 aware string operations for StringView columns.
 * 
 * Provides operations that work directly on UTF-8 bytes to avoid string
 * materialization costs, while being aware of the distinction between
 * byte length and character (codepoint) length.
 * 
 * Key principle: Delay string materialization as long as possible.
 */

import { StringView } from "./string-view.ts";
import { AdaptiveStringColumn } from "./adaptive-string.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";

/** UTF-8 text encoder/decoder (shared instances) */
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

/**
 * UTF-8 byte utilities for working with raw string data.
 */
export class UTF8Utils {
	/**
	 * Count UTF-8 codepoints in a byte array.
	 * This is slower than byte length but gives proper character count.
	 */
	static countCodepoints(bytes: Uint8Array): number {
		let count = 0;
		for (let i = 0; i < bytes.length; ) {
			const byte = bytes[i];
			if ((byte & 0x80) === 0) {
				// ASCII: 1 byte
				i += 1;
			} else if ((byte & 0xE0) === 0xC0) {
				// 2-byte character
				i += 2;
			} else if ((byte & 0xF0) === 0xE0) {
				// 3-byte character
				i += 3;
			} else if ((byte & 0xF8) === 0xF0) {
				// 4-byte character
				i += 4;
			} else {
				// Invalid UTF-8, skip byte
				i += 1;
			}
			count++;
		}
		return count;
	}

	/**
	 * Get byte offset of the Nth codepoint in UTF-8 bytes.
	 * Returns -1 if N is out of bounds.
	 */
	static getCodepointOffset(bytes: Uint8Array, codepointIndex: number): number {
		let currentCodepoint = 0;
		for (let i = 0; i < bytes.length; ) {
			if (currentCodepoint === codepointIndex) {
				return i;
			}
			
			const byte = bytes[i];
			if ((byte & 0x80) === 0) {
				i += 1;
			} else if ((byte & 0xE0) === 0xC0) {
				i += 2;
			} else if ((byte & 0xF0) === 0xE0) {
				i += 3;
			} else if ((byte & 0xF8) === 0xF0) {
				i += 4;
			} else {
				i += 1; // Invalid UTF-8
			}
			currentCodepoint++;
		}
		
		return currentCodepoint === codepointIndex ? bytes.length : -1;
	}

	/**
	 * Extract substring by codepoint indices.
	 * Returns the UTF-8 bytes for the substring.
	 */
	static substringBytes(bytes: Uint8Array, start: number, end?: number): Uint8Array {
		const startOffset = this.getCodepointOffset(bytes, start);
		if (startOffset === -1) return new Uint8Array(0);
		
		let endOffset: number;
		if (end === undefined) {
			endOffset = bytes.length;
		} else {
			endOffset = this.getCodepointOffset(bytes, end);
			if (endOffset === -1) endOffset = bytes.length;
		}
		
		return bytes.subarray(startOffset, endOffset);
	}

	/**
	 * Check if bytes start with prefix bytes.
	 * This is faster than string comparison.
	 */
	static startsWith(bytes: Uint8Array, prefix: Uint8Array): boolean {
		if (prefix.length > bytes.length) return false;
		
		for (let i = 0; i < prefix.length; i++) {
			if (bytes[i] !== prefix[i]) return false;
		}
		return true;
	}

	/**
	 * Check if bytes end with suffix bytes.
	 */
	static endsWith(bytes: Uint8Array, suffix: Uint8Array): boolean {
		if (suffix.length > bytes.length) return false;
		
		const offset = bytes.length - suffix.length;
		for (let i = 0; i < suffix.length; i++) {
			if (bytes[offset + i] !== suffix[i]) return false;
		}
		return true;
	}

	/**
	 * Find first occurrence of pattern in bytes.
	 * Returns byte offset or -1 if not found.
	 */
	static indexOf(bytes: Uint8Array, pattern: Uint8Array): number {
		if (pattern.length === 0) return 0;
		if (pattern.length > bytes.length) return -1;
		
		for (let i = 0; i <= bytes.length - pattern.length; i++) {
			let match = true;
			for (let j = 0; j < pattern.length; j++) {
				if (bytes[i + j] !== pattern[j]) {
					match = false;
					break;
				}
			}
			if (match) return i;
		}
		return -1;
	}

	/**
	 * Compare two UTF-8 byte sequences lexicographically.
	 * Returns: -1 if a < b, 0 if a === b, 1 if a > b.
	 */
	static compare(a: Uint8Array, b: Uint8Array): number {
		const minLen = Math.min(a.length, b.length);
		
		for (let i = 0; i < minLen; i++) {
			const diff = a[i] - b[i];
			if (diff !== 0) return diff < 0 ? -1 : 1;
		}
		
		return a.length < b.length ? -1 : 
		       a.length > b.length ? 1 : 0;
	}
}

/**
 * String operations that work directly on StringView columns.
 */
export class StringViewOps {
	/**
	 * Filter StringView by prefix (without materializing strings).
	 */
	static filterByPrefix(view: StringView, prefix: string): number[] {
		const prefixBytes = encoder.encode(prefix);
		const results: number[] = [];
		
		for (let i = 0; i < view.length; i++) {
			if (view.isNull(i)) continue;
			
			const bytes = view.getBytes(i);
			if (bytes && UTF8Utils.startsWith(bytes, prefixBytes)) {
				results.push(i);
			}
		}
		
		return results;
	}

	/**
	 * Filter StringView by suffix.
	 */
	static filterBySuffix(view: StringView, suffix: string): number[] {
		const suffixBytes = encoder.encode(suffix);
		const results: number[] = [];
		
		for (let i = 0; i < view.length; i++) {
			if (view.isNull(i)) continue;
			
			const bytes = view.getBytes(i);
			if (bytes && UTF8Utils.endsWith(bytes, suffixBytes)) {
				results.push(i);
			}
		}
		
		return results;
	}

	/**
	 * Filter by byte length (faster than character length).
	 */
	static filterByByteLength(view: StringView, minLen: number, maxLen?: number): number[] {
		const results: number[] = [];
		const max = maxLen ?? Number.MAX_SAFE_INTEGER;
		
		for (let i = 0; i < view.length; i++) {
			if (view.isNull(i)) continue;
			
			const len = view.getByteLength(i);
			if (len !== undefined && len >= minLen && len <= max) {
				results.push(i);
			}
		}
		
		return results;
	}

	/**
	 * Filter by character length (slower but accurate for multi-byte chars).
	 */
	static filterByCharLength(view: StringView, minLen: number, maxLen?: number): number[] {
		const results: number[] = [];
		const max = maxLen ?? Number.MAX_SAFE_INTEGER;
		
		for (let i = 0; i < view.length; i++) {
			if (view.isNull(i)) continue;
			
			const len = view.getCharLength(i);
			if (len !== undefined && len >= minLen && len <= max) {
				results.push(i);
			}
		}
		
		return results;
	}

	/**
	 * Sort indices by string content (without materializing strings).
	 */
	static sortIndices(view: StringView, indices: number[], descending: boolean = false): number[] {
		const factor = descending ? -1 : 1;
		
		return indices.sort((a, b) => {
			// Handle nulls (sort to end)
			if (view.isNull(a) && view.isNull(b)) return 0;
			if (view.isNull(a)) return 1;
			if (view.isNull(b)) return -1;
			
			const bytesA = view.getBytes(a);
			const bytesB = view.getBytes(b);
			
			if (!bytesA && !bytesB) return 0;
			if (!bytesA) return 1;
			if (!bytesB) return -1;
			
			return UTF8Utils.compare(bytesA, bytesB) * factor;
		});
	}

	/**
	 * Group indices by string prefix (e.g., for prefix aggregation).
	 */
	static groupByPrefix(view: StringView, prefixLength: number): Map<string, number[]> {
		const groups = new Map<string, number[]>();
		
		for (let i = 0; i < view.length; i++) {
			if (view.isNull(i)) continue;
			
			const bytes = view.getBytes(i);
			if (!bytes) continue;
			
			// Get prefix bytes
			const prefixBytes = UTF8Utils.substringBytes(bytes, 0, prefixLength);
			const prefix = decoder.decode(prefixBytes);
			
			if (!groups.has(prefix)) {
				groups.set(prefix, []);
			}
			groups.get(prefix)!.push(i);
		}
		
		return groups;
	}

	/**
	 * Find duplicate indices (same string content).
	 */
	static findDuplicates(view: StringView): Map<string, number[]> {
		const groups = new Map<string, number[]>();
		
		for (let i = 0; i < view.length; i++) {
			if (view.isNull(i)) continue;
			
			// We need to materialize the string here for grouping
			const str = view.getString(i);
			if (str === undefined) continue;
			
			if (!groups.has(str)) {
				groups.set(str, []);
			}
			groups.get(str)!.push(i);
		}
		
		// Return only groups with duplicates
		const duplicates = new Map<string, number[]>();
		for (const [str, indices] of groups) {
			if (indices.length > 1) {
				duplicates.set(str, indices);
			}
		}
		
		return duplicates;
	}
}

/**
 * Operations on AdaptiveStringColumn that dispatch to appropriate implementation.
 */
export class AdaptiveStringOps {
	/**
	 * Filter adaptive string column by predicate.
	 */
	static filter(column: AdaptiveStringColumn, predicate: (str: string, index: number) => boolean): number[] {
		const results: number[] = [];
		
		for (let i = 0; i < column.length; i++) {
			if (column.isNull(i)) continue;
			
			const str = column.getString(i);
			if (str !== undefined && predicate(str, i)) {
				results.push(i);
			}
		}
		
		return results;
	}

	/**
	 * Fast prefix filter that uses StringView ops when available.
	 */
	static filterByPrefix(column: AdaptiveStringColumn, prefix: string): number[] {
		const internals = column.getInternals();
		
		if (internals.stringView) {
			// Use fast StringView operations
			return StringViewOps.filterByPrefix(internals.stringView, prefix);
		} else {
			// Fall back to string materialization
			return this.filter(column, (str) => str.startsWith(prefix));
		}
	}

	/**
	 * Get statistics about string lengths.
	 */
	static getLengthStats(column: AdaptiveStringColumn): {
		count: number;
		avgByteLength: number;
		avgCharLength: number;
		minLength: number;
		maxLength: number;
	} {
		let count = 0;
		let totalBytes = 0;
		let totalChars = 0;
		let minLen = Number.MAX_SAFE_INTEGER;
		let maxLen = 0;
		
		for (let i = 0; i < column.length; i++) {
			if (column.isNull(i)) continue;
			
			const str = column.getString(i);
			if (str === undefined) continue;
			
			count++;
			const byteLen = encoder.encode(str).length;
			const charLen = str.length; // JavaScript string length
			
			totalBytes += byteLen;
			totalChars += charLen;
			minLen = Math.min(minLen, charLen);
			maxLen = Math.max(maxLen, charLen);
		}
		
		return {
			count,
			avgByteLength: count > 0 ? totalBytes / count : 0,
			avgCharLength: count > 0 ? totalChars / count : 0,
			minLength: count > 0 ? minLen : 0,
			maxLength: count > 0 ? maxLen : 0
		};
	}
}

/**
 * Utility to determine if a string operation should use byte-level or character-level processing.
 */
export function shouldUseByteOps(text: string): boolean {
	// If all characters are ASCII, byte ops are equivalent and faster
	for (let i = 0; i < text.length; i++) {
		if (text.charCodeAt(i) > 127) {
			return false; // Has non-ASCII, need character-level ops
		}
	}
	return true;
}