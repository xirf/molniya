/**
 * Data types for Molniya DataFrame columns.
 *
 * Each DType maps to a specific binary representation:
 * - Fixed-size types use TypedArrays directly
 * - Strings use dictionary encoding (uint32 index into string table)
 * - Nullable types use a separate null bitmap
 */

export enum DTypeKind {
	Int8 = 0,
	Int16 = 1,
	Int32 = 2,
	Int64 = 3,
	UInt8 = 4,
	UInt16 = 5,
	UInt32 = 6,
	UInt64 = 7,
	Float32 = 8,
	Float64 = 9,
	Boolean = 10,
	String = 11,
	Date = 12, // Days since epoch (int32)
	Timestamp = 13, // Milliseconds since epoch (int64)
}

/** Byte sizes for each DType. String returns 4 (dictionary index size). */
export const DTYPE_SIZES: Record<DTypeKind, number> = {
	[DTypeKind.Int8]: 1,
	[DTypeKind.Int16]: 2,
	[DTypeKind.Int32]: 4,
	[DTypeKind.Int64]: 8,
	[DTypeKind.UInt8]: 1,
	[DTypeKind.UInt16]: 2,
	[DTypeKind.UInt32]: 4,
	[DTypeKind.UInt64]: 8,
	[DTypeKind.Float32]: 4,
	[DTypeKind.Float64]: 8,
	[DTypeKind.Boolean]: 1, // Stored as uint8, not bit-packed for simplicity
	[DTypeKind.String]: 4, // Dictionary index (uint32)
	[DTypeKind.Date]: 4, // Days since epoch (int32)
	[DTypeKind.Timestamp]: 8, // Milliseconds since epoch (int64)
};

/** TypedArray constructor for each numeric DType */
export const DTYPE_ARRAY_CONSTRUCTORS = {
	[DTypeKind.Int8]: Int8Array,
	[DTypeKind.Int16]: Int16Array,
	[DTypeKind.Int32]: Int32Array,
	[DTypeKind.Int64]: BigInt64Array,
	[DTypeKind.UInt8]: Uint8Array,
	[DTypeKind.UInt16]: Uint16Array,
	[DTypeKind.UInt32]: Uint32Array,
	[DTypeKind.UInt64]: BigUint64Array,
	[DTypeKind.Float32]: Float32Array,
	[DTypeKind.Float64]: Float64Array,
	[DTypeKind.Boolean]: Uint8Array,
	[DTypeKind.String]: Uint32Array, // Dictionary indices
	[DTypeKind.Date]: Int32Array,
	[DTypeKind.Timestamp]: BigInt64Array,
} as const;

/** Type-level mapping from DTypeKind to TypeScript primitive type */
export type DTypeToTS<T extends DTypeKind> = T extends
	| DTypeKind.Int8
	| DTypeKind.Int16
	| DTypeKind.Int32
	? number
	: T extends DTypeKind.UInt8 | DTypeKind.UInt16 | DTypeKind.UInt32
		? number
		: T extends DTypeKind.Float32 | DTypeKind.Float64
			? number
			: T extends DTypeKind.Int64 | DTypeKind.UInt64 | DTypeKind.Timestamp
				? bigint
				: T extends DTypeKind.Boolean
					? boolean
					: T extends DTypeKind.String
						? string
						: T extends DTypeKind.Date
							? Date
							: never;

/** DType descriptor with nullable flag */
export interface DType<K extends DTypeKind = DTypeKind> {
	readonly kind: K;
	readonly nullable: boolean;
}

/** Create a non-nullable DType */
function dtype<K extends DTypeKind>(kind: K): DType<K> {
	return { kind, nullable: false };
}

/** Create a nullable DType */
function nullableDtype<K extends DTypeKind>(kind: K): DType<K> {
	return { kind, nullable: true };
}

/**
 * DType factory with convenient accessors.
 *
 * Usage:
 *   DType.int32          // non-nullable int32
 *   DType.int32.nullable // nullable int32
 *   DType.string         // non-nullable string (dictionary encoded)
 */
export const DType = {
	int8: dtype(DTypeKind.Int8),
	int16: dtype(DTypeKind.Int16),
	int32: dtype(DTypeKind.Int32),
	int64: dtype(DTypeKind.Int64),
	uint8: dtype(DTypeKind.UInt8),
	uint16: dtype(DTypeKind.UInt16),
	uint32: dtype(DTypeKind.UInt32),
	uint64: dtype(DTypeKind.UInt64),
	float32: dtype(DTypeKind.Float32),
	float64: dtype(DTypeKind.Float64),
	boolean: dtype(DTypeKind.Boolean),
	string: dtype(DTypeKind.String),
	date: dtype(DTypeKind.Date),
	timestamp: dtype(DTypeKind.Timestamp),

	/** Create nullable variants */
	nullable: {
		int8: nullableDtype(DTypeKind.Int8),
		int16: nullableDtype(DTypeKind.Int16),
		int32: nullableDtype(DTypeKind.Int32),
		int64: nullableDtype(DTypeKind.Int64),
		uint8: nullableDtype(DTypeKind.UInt8),
		uint16: nullableDtype(DTypeKind.UInt16),
		uint32: nullableDtype(DTypeKind.UInt32),
		uint64: nullableDtype(DTypeKind.UInt64),
		float32: nullableDtype(DTypeKind.Float32),
		float64: nullableDtype(DTypeKind.Float64),
		boolean: nullableDtype(DTypeKind.Boolean),
		string: nullableDtype(DTypeKind.String),
		date: nullableDtype(DTypeKind.Date),
		timestamp: nullableDtype(DTypeKind.Timestamp),
	},
} as const;

/** Check if a DType is numeric (supports arithmetic) */
export function isNumericDType(dtype: DType): boolean {
	const kind = dtype.kind;
	return kind >= DTypeKind.Int8 && kind <= DTypeKind.Float64;
}

/** Check if a DType is integer (not floating point) */
export function isIntegerDType(dtype: DType): boolean {
	const kind = dtype.kind;
	return kind >= DTypeKind.Int8 && kind <= DTypeKind.UInt64;
}

/** Check if a DType uses BigInt representation */
export function isBigIntDType(dtype: DType): boolean {
	const kind = dtype.kind;
	return (
		kind === DTypeKind.Int64 ||
		kind === DTypeKind.UInt64 ||
		kind === DTypeKind.Timestamp
	);
}

/** Get the byte size of a DType */
export function getDTypeSize(dtype: DType): number {
	return DTYPE_SIZES[dtype.kind];
}

/** Create a nullable version of any DType */
export function toNullable<K extends DTypeKind>(dtype: DType<K>): DType<K> {
	if (dtype.nullable) return dtype;
	return { kind: dtype.kind, nullable: true };
}

/** Get readable name for DTypeKind */
export function getDTypeName(kind: DTypeKind): string {
	switch (kind) {
		case DTypeKind.Int8:
			return "Int8";
		case DTypeKind.Int16:
			return "Int16";
		case DTypeKind.Int32:
			return "Int32";
		case DTypeKind.Int64:
			return "Int64";
		case DTypeKind.UInt8:
			return "UInt8";
		case DTypeKind.UInt16:
			return "UInt16";
		case DTypeKind.UInt32:
			return "UInt32";
		case DTypeKind.UInt64:
			return "UInt64";
		case DTypeKind.Float32:
			return "Float32";
		case DTypeKind.Float64:
			return "Float64";
		case DTypeKind.Boolean:
			return "Boolean";
		case DTypeKind.String:
			return "String";
		case DTypeKind.Date:
			return "Date";
		case DTypeKind.Timestamp:
			return "Timestamp";
		default:
			return "Unknown";
	}
}
