/**
 * StringView implementation for high-cardinality string columns.
 * 
 * Stores strings as UTF-8 bytes in a contiguous buffer with separate
 * offset/length arrays. Avoids V8 string heap pressure and provides
 * O(1) append operations.
 * 
 * Key features:
 * - Zero-copy string access via views
 * - UTF-8 aware operations (byte vs codepoint distinction)
 * - Shared data buffers across columns
 * - Separate null bitmap for SIMD-friendly operations
 */

import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";

/** Initial capacity constants */
const INITIAL_DATA_SIZE = 64 * 1024;  // 64KB initial data buffer
const INITIAL_CAPACITY = 1000;        // Initial number of strings
const GROWTH_FACTOR = 1.5;            // Growth factor for arrays

/** UTF-8 text encoder/decoder (shared instances) */
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

/**
 * Shared string data buffer that can be used across multiple StringView columns.
 * This is particularly useful when multiple columns contain similar strings
 * (e.g., origin_city and dest_city).
 */
export class SharedStringBuffer {
	private data: Uint8Array;
	private offset: number;
	private readonly growthFactor: number;

	constructor(initialSize: number = INITIAL_DATA_SIZE, growthFactor: number = GROWTH_FACTOR) {
		this.data = new Uint8Array(initialSize);
		this.offset = 0;
		this.growthFactor = growthFactor;
	}

	/**
	 * Append UTF-8 bytes to the buffer.
	 * Returns the byte offset where data was written.
	 */
	append(bytes: Uint8Array): number {
		const startOffset = this.offset;
		
		// Grow if needed
		if (this.offset + bytes.length > this.data.length) {
			this.grow(bytes.length);
		}

		this.data.set(bytes, this.offset);
		this.offset += bytes.length;
		return startOffset;
	}

	/**
	 * Get a view into the data buffer.
	 * This is a zero-copy operation.
	 */
	getView(offset: number, length: number): Uint8Array {
		return this.data.subarray(offset, offset + length);
	}

	/**
	 * Get total bytes used.
	 */
	get size(): number {
		return this.offset;
	}

	/**
	 * Get total capacity.
	 */
	get capacity(): number {
		return this.data.length;
	}

	/**
	 * Compact the buffer by removing unused capacity.
	 * Call this periodically to reclaim memory.
	 */
	compact(): void {
		if (this.offset < this.data.length) {
			const newData = new Uint8Array(this.offset);
			newData.set(this.data.subarray(0, this.offset));
			this.data = newData;
		}
	}

	private grow(needed: number): void {
		const minSize = this.offset + needed;
		const newSize = Math.max(
			Math.ceil(this.data.length * this.growthFactor),
			minSize
		);
		
		const newData = new Uint8Array(newSize);
		newData.set(this.data);
		this.data = newData;
	}
}

/**
 * StringView column implementation.
 * 
 * Each string is stored as:
 * - offset: byte offset into shared data buffer (BigUint64Array)
 * - length: byte length of UTF-8 data (Uint32Array)
 * 
 * Supports both shared and private data buffers.
 */
export class StringView {
	/** String data buffer (can be shared across columns) */
	private readonly dataBuffer: SharedStringBuffer;
	/** Byte offsets into data buffer */
	private offsets: BigUint64Array;
	/** Byte lengths of strings */
	private lengths: Uint32Array;
	/** Number of strings stored */
	private count: number;
	/** Maximum capacity */
	private readonly capacity: number;
	/** Null bitmap (optional) */
	private nullBitmap: Uint8Array | null;

	constructor(
		capacity: number = INITIAL_CAPACITY,
		nullable: boolean = false,
		sharedBuffer?: SharedStringBuffer
	) {
		this.capacity = capacity;
		this.count = 0;
		
		// Use provided shared buffer or create private one
		this.dataBuffer = sharedBuffer ?? new SharedStringBuffer();
		
		this.offsets = new BigUint64Array(capacity);
		this.lengths = new Uint32Array(capacity);
		this.nullBitmap = nullable ? new Uint8Array(Math.ceil(capacity / 8)) : null;
	}

	/**
	 * Append a string to the view.
	 * Returns the index of the appended string.
	 */
	append(str: string): Result<number> {
		if (this.count >= this.capacity) {
			return err(ErrorCode.BufferFull);
		}

		const bytes = encoder.encode(str);
		const offset = this.dataBuffer.append(bytes);
		
		this.offsets[this.count] = BigInt(offset);
		this.lengths[this.count] = bytes.length;
		
		if (this.nullBitmap) {
			this.setNull(this.count, false);
		}
		
		return ok(this.count++);
	}

	/**
	 * Append a null value.
	 * Returns the index of the appended null.
	 */
	appendNull(): Result<number> {
		if (this.count >= this.capacity) {
			return err(ErrorCode.BufferFull);
		}

		if (!this.nullBitmap) {
			return err(ErrorCode.InvalidArgument, "Column is not nullable");
		}

		// Zero out offset/length for null
		this.offsets[this.count] = BigInt(0);
		this.lengths[this.count] = 0;
		
		this.setNull(this.count, true);
		
		return ok(this.count++);
	}

	/**
	 * Set a string at a specific index to null.
	 */
	setNull(index: number, isNull: boolean): void {
		if (!this.nullBitmap) return;
		
		const byteIdx = Math.floor(index / 8);
		const bitIdx = index % 8;
		
		if (isNull) {
			this.nullBitmap[byteIdx] |= (1 << bitIdx);
		} else {
			this.nullBitmap[byteIdx] &= ~(1 << bitIdx);
		}
	}

	/**
	 * Check if a string at index is null.
	 */
	isNull(index: number): boolean {
		if (!this.nullBitmap) return false;
		
		const byteIdx = Math.floor(index / 8);
		const bitIdx = index % 8;
		return (this.nullBitmap[byteIdx] & (1 << bitIdx)) !== 0;
	}

	/**
	 * Get string at index.
	 * Returns undefined for out-of-bounds or null values.
	 */
	getString(index: number): string | undefined {
		if (index >= this.count || this.isNull(index)) {
			return undefined;
		}

		const offset = Number(this.offsets[index]);
		const length = this.lengths[index];
		const bytes = this.dataBuffer.getView(offset, length);
		
		return decoder.decode(bytes);
	}

	/**
	 * Get raw UTF-8 bytes at index (zero-copy).
	 * Returns undefined for out-of-bounds or null values.
	 */
	getBytes(index: number): Uint8Array | undefined {
		if (index >= this.count || this.isNull(index)) {
			return undefined;
		}

		const offset = Number(this.offsets[index]);
		const length = this.lengths[index];
		return this.dataBuffer.getView(offset, length);
	}

	/**
	 * Get byte length at index without materializing string.
	 */
	getByteLength(index: number): number | undefined {
		if (index >= this.count || this.isNull(index)) {
			return undefined;
		}
		return this.lengths[index];
	}

	/**
	 * Get UTF-8 character length at index.
	 * Note: This requires scanning the UTF-8 bytes.
	 */
	getCharLength(index: number): number | undefined {
		const bytes = this.getBytes(index);
		if (!bytes) return undefined;
		
		// Count UTF-8 codepoints (not bytes)
		let charCount = 0;
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
			charCount++;
		}
		return charCount;
	}

	/**
	 * Compare string at index with target string.
	 * Returns: -1 if less, 0 if equal, 1 if greater.
	 * Operates on UTF-8 bytes to avoid string materialization.
	 */
	compareAt(index: number, target: string): number {
		const bytes = this.getBytes(index);
		if (!bytes) return -1; // null < any string
		
		const targetBytes = encoder.encode(target);
		const minLen = Math.min(bytes.length, targetBytes.length);
		
		for (let i = 0; i < minLen; i++) {
			const diff = bytes[i] - targetBytes[i];
			if (diff !== 0) return diff < 0 ? -1 : 1;
		}
		
		return bytes.length < targetBytes.length ? -1 :
		       bytes.length > targetBytes.length ? 1 : 0;
	}

	/**
	 * Check if string at index starts with prefix.
	 * Operates on UTF-8 bytes to avoid string materialization.
	 */
	startsWithAt(index: number, prefix: string): boolean {
		const bytes = this.getBytes(index);
		if (!bytes) return false;
		
		const prefixBytes = encoder.encode(prefix);
		if (prefixBytes.length > bytes.length) return false;
		
		for (let i = 0; i < prefixBytes.length; i++) {
			if (bytes[i] !== prefixBytes[i]) return false;
		}
		return true;
	}

	/**
	 * Check if string at index contains substring.
	 * Note: This requires full string materialization.
	 */
	containsAt(index: number, substring: string): boolean {
		const str = this.getString(index);
		return str ? str.includes(substring) : false;
	}

	/**
	 * Number of strings in the view.
	 */
	get length(): number {
		return this.count;
	}

	/**
	 * Whether this view supports null values.
	 */
	get isNullable(): boolean {
		return this.nullBitmap !== null;
	}

	/**
	 * Get remaining capacity.
	 */
	get available(): number {
		return this.capacity - this.count;
	}

	/**
	 * Get total data bytes used.
	 */
	get dataSize(): number {
		return this.dataBuffer.size;
	}

	/**
	 * Compact the underlying data buffer.
	 * Call this periodically to reclaim memory.
	 */
	compact(): void {
		this.dataBuffer.compact();
	}

	/**
	 * Create a new StringView that shares the same data buffer.
	 * Useful for columns with similar string patterns.
	 */
	createSibling(capacity: number = INITIAL_CAPACITY, nullable: boolean = false): StringView {
		return new StringView(capacity, nullable, this.dataBuffer);
	}
}

/**
 * Create a new StringView with default settings.
 */
export function createStringView(
	capacity?: number,
	nullable?: boolean,
	sharedBuffer?: SharedStringBuffer
): StringView {
	return new StringView(capacity, nullable, sharedBuffer);
}

/**
 * Create a shared string buffer for use across multiple columns.
 */
export function createSharedStringBuffer(initialSize?: number): SharedStringBuffer {
	return new SharedStringBuffer(initialSize);
}