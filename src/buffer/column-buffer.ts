/**
 * Column buffer for storing typed array data.
 *
 * A ColumnBuffer wraps a TypedArray and provides:
 * - Fixed capacity with bounds checking
 * - Type-safe read/write operations
 * - Integration with selection vectors
 */

import { DTYPE_ARRAY_CONSTRUCTORS, type DTypeKind } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";

/** TypedArray union type */
export type TypedArray =
	| Int8Array
	| Int16Array
	| Int32Array
	| BigInt64Array
	| Uint8Array
	| Uint16Array
	| Uint32Array
	| BigUint64Array
	| Float32Array
	| Float64Array;

/** Get TypedArray type for a DTypeKind */
export type TypedArrayFor<K extends DTypeKind> = K extends DTypeKind.Int8
	? Int8Array
	: K extends DTypeKind.Int16
	? Int16Array
	: K extends DTypeKind.Int32
	? Int32Array
	: K extends DTypeKind.Int64
	? BigInt64Array
	: K extends DTypeKind.UInt8
	? Uint8Array
	: K extends DTypeKind.UInt16
	? Uint16Array
	: K extends DTypeKind.UInt32
	? Uint32Array
	: K extends DTypeKind.UInt64
	? BigUint64Array
	: K extends DTypeKind.Float32
	? Float32Array
	: K extends DTypeKind.Float64
	? Float64Array
	: K extends DTypeKind.Boolean
	? Uint8Array
	: K extends DTypeKind.String | DTypeKind.StringDict
	? Uint32Array
	: K extends DTypeKind.StringView
	? Uint8Array  // Special: handled by AdaptiveStringColumn
	: K extends DTypeKind.Date
	? Int32Array
	: K extends DTypeKind.Timestamp
	? BigInt64Array
	: never;

/**
 * A column buffer storing values of a specific dtype.
 */
export class ColumnBuffer<K extends DTypeKind = DTypeKind> {
	/** The underlying typed array */
	readonly data: TypedArrayFor<K>;

	/** The DType kind */
	readonly kind: K;

	/** Maximum number of elements */
	readonly capacity: number;

	/** Current number of valid elements */
	private _length: number;

	/** Null bitmap (bit i = 1 means index i is null) */
	private nullBitmap: Uint8Array | null;

	constructor(kind: K, capacity: number, nullable: boolean = false) {
		this.kind = kind;
		this.capacity = capacity;
		this._length = 0;

		const Constructor = DTYPE_ARRAY_CONSTRUCTORS[kind];
		this.data = new Constructor(capacity) as TypedArrayFor<K>;

		this.nullBitmap = nullable ? new Uint8Array(Math.ceil(capacity / 8)) : null;
	}

	/** Current number of valid elements */
	get length(): number {
		return this._length;
	}

	/** Check if column is nullable */
	get isNullable(): boolean {
		return this.nullBitmap !== null;
	}

	/** Get remaining capacity */
	get available(): number {
		return this.capacity - this._length;
	}

	/** Get value at index (no bounds check for performance) */
	get(index: number): TypedArrayFor<K>[number] {
		return this.data[index]!;
	}

	/** Set value at index (no bounds check for performance) */
	set(index: number, value: TypedArrayFor<K>[number]): void {
		(this.data as TypedArray)[index] = value;
		if (index >= this._length) {
			this._length = index + 1;
		}
	}

	/** Check if value at index is null */
	isNull(index: number): boolean {
		if (this.nullBitmap === null) return false;
		const byteIndex = index >>> 3;
		const bitIndex = index & 7;
		return ((this.nullBitmap[byteIndex] ?? 0) & (1 << bitIndex)) !== 0;
	}

	/** Set null flag at index */
	setNull(index: number, isNull: boolean): void {
		if (this.nullBitmap === null) return;
		const byteIndex = index >>> 3;
		const bitIndex = index & 7;
		if (isNull) {
			this.nullBitmap[byteIndex] =
				(this.nullBitmap[byteIndex] ?? 0) | (1 << bitIndex);
		} else {
			this.nullBitmap[byteIndex] =
				(this.nullBitmap[byteIndex] ?? 0) & ~(1 << bitIndex);
		}
	}

	/** Append a value, return success or BufferFull error */
	append(value: TypedArrayFor<K>[number]): ErrorCode {
		if (this._length >= this.capacity) {
			return ErrorCode.BufferFull;
		}
		(this.data as TypedArray)[this._length] = value;
		this._length++;
		return ErrorCode.None;
	}

	/** Append a null value */
	appendNull(): ErrorCode {
		if (this._length >= this.capacity) {
			return ErrorCode.BufferFull;
		}
		if (this.nullBitmap !== null) {
			this.setNull(this._length, true);
		}
		this._length++;
		return ErrorCode.None;
	}

	/** Bulk set from typed array (fast path for parquet) */
	setFromTypedArray(source: TypedArray, count: number): void {
		// We can't strictly prove to TS that `this.data` matches `source`,
		// but in practice we know they are compatible numeric arrays or we misconfigured something.
		// The issue is Uint8Array vs BigInt64Array incompatibility in the union type.

		// Just suppressing via unknown-cast is cleaner than 'as any' but effectively same.
		// To be strictly correct we'd need to switch on type.
		// Given performance requirement, we'll keep it simple but safe-checked.

		if (source.length < count) throw new Error("Source buffer too small");

		// Use the native .set() method
		// biome-ignore lint/suspicious/noExplicitAny: Generic casting
		(this.data as TypedArray).set((source as any).subarray(0, count));
		this._length = count;
	}

	/** Set length directly (for bulk operations) */
	setLength(len: number): void {
		this._length = len;
	}

	/** Reset length to 0 (doesn't clear data) */
	clear(): void {
		this._length = 0;
		if (this.nullBitmap !== null) {
			this.nullBitmap.fill(0);
		}
	}

	/** Create a view over current valid data */
	view(): TypedArrayFor<K> {
		return this.data.subarray(0, this._length) as TypedArrayFor<K>;
	}

	/** Copy values from another buffer using a selection vector */
	copySelected(
		source: ColumnBuffer<K>,
		selection: Uint32Array,
		selectionLength: number,
	): ErrorCode {
		if (this._length + selectionLength > this.capacity) {
			return ErrorCode.BufferFull;
		}

		const srcData = source.data;
		const dstData = this.data as TypedArray;
		const dstStart = this._length;

		for (let i = 0; i < selectionLength; i++) {
			const srcIdx = selection[i] ?? 0;
			dstData[dstStart + i] = srcData[srcIdx] ?? 0;

			// Copy null bit if applicable
			if (this.nullBitmap !== null && source.nullBitmap !== null) {
				this.setNull(dstStart + i, source.isNull(srcIdx));
			}
		}

		this._length += selectionLength;
		return ErrorCode.None;
	}

	/** Recycle the buffer (clear efficiently) */
	recycle(): void {
		this._length = 0;
		// We don't need to zero the data array for internal reuse
		// But we MUST zero the null bitmap if it exists
		if (this.nullBitmap !== null) {
			this.nullBitmap.fill(0);
		}
	}

	/** Ensure capacity */
	ensureCapacity(minCapacity: number): void {
		if (this.capacity >= minCapacity) return;

		let newCap = this.capacity === 0 ? 16 : this.capacity;
		while (newCap < minCapacity) {
			newCap = Math.max(newCap * 2, 16);
		}

		const Constructor = DTYPE_ARRAY_CONSTRUCTORS[this.kind] as any;
		const newData = new Constructor(newCap);
		newData.set(this.data);
		(this as any).data = newData;
		(this as any).capacity = newCap;

		if (this.nullBitmap) {
			const newBitmap = new Uint8Array(Math.ceil(newCap / 8));
			newBitmap.set(this.nullBitmap);
			this.nullBitmap = newBitmap;
		}
	}
}

/**
 * Create a column buffer with the specified dtype and capacity.
 */
export function createColumnBuffer<K extends DTypeKind>(
	kind: K,
	capacity: number,
	nullable: boolean = false,
): Result<ColumnBuffer<K>> {
	if (capacity <= 0) {
		return err(ErrorCode.InvalidCapacity);
	}
	return ok(new ColumnBuffer(kind, capacity, nullable));
}

/**
 * Create a column buffer from existing data.
 */
export function columnBufferFromArray<K extends DTypeKind>(
	kind: K,
	data: TypedArrayFor<K>,
	nullable: boolean = false,
): ColumnBuffer<K> {
	const buffer = new ColumnBuffer(kind, data.length, nullable);
	(buffer.data as unknown as { set: (d: TypedArray) => void }).set(data);
	(buffer as unknown as { _length: number })._length = data.length;
	return buffer;
}
