/**
 * Type conversion for parquet values
 * Ported from hyparquet's convert.js with TypeScript types
 */

const decoder = new TextDecoder();

/**
 * Parse big-endian DECIMAL bytes to number.
 */
export function parseDecimal(bytes: Uint8Array): number {
	if (!bytes.length) return 0;

	let value = 0n;
	for (const byte of bytes) {
		value = value * 256n + BigInt(byte);
	}

	// handle signed (two's complement)
	const bits = bytes.length * 8;
	if (value >= 2n ** BigInt(bits - 1)) {
		value -= 2n ** BigInt(bits);
	}

	return Number(value);
}

/**
 * Convert bytes to string.
 */
export function bytesToString(
	bytes: Uint8Array | undefined,
): string | undefined {
	return bytes && decoder.decode(bytes);
}

/**
 * Convert days since epoch to Date.
 */
export function daysToDate(days: number): Date {
	return new Date(days * 86400000);
}

/**
 * Convert milliseconds since epoch to Date.
 */
export function millisToDate(millis: number | bigint): Date {
	return new Date(Number(millis));
}

/**
 * Convert microseconds since epoch to Date.
 */
export function microsToDate(micros: bigint): Date {
	return new Date(Number(micros / 1000n));
}

/**
 * Convert nanoseconds since epoch to Date.
 */
export function nanosToDate(nanos: bigint): Date {
	return new Date(Number(nanos / 1000000n));
}

/**
 * Convert INT96 (days in high 32-bit, nanos in low 64-bit) to Date.
 */
export function int96ToDate(value: bigint): Date {
	const days = (value >> 64n) - 2440588n; // Julian to Unix epoch
	const nano = value & 0xffffffffffffffffn;
	const nanos = days * 86400000000000n + nano;
	return nanosToDate(nanos);
}

export interface SchemaElement {
	type?: number;
	type_length?: number;
	converted_type?: number;
	scale?: number;
	precision?: number;
	repetition_type?: number;
	name: string;
}

// Converted type constants (from parquet thrift spec)
const CONVERTED_TYPE = {
	UTF8: 0,
	MAP: 1,
	MAP_KEY_VALUE: 2,
	LIST: 3,
	ENUM: 4,
	DECIMAL: 5,
	DATE: 6,
	TIME_MILLIS: 7,
	TIME_MICROS: 8,
	TIMESTAMP_MILLIS: 9,
	TIMESTAMP_MICROS: 10,
	UINT_8: 11,
	UINT_16: 12,
	UINT_32: 13,
	UINT_64: 14,
	INT_8: 15,
	INT_16: 16,
	INT_32: 17,
	INT_64: 18,
};

// Parquet type constants
const PARQUET_TYPE = {
	BOOLEAN: 0,
	INT32: 1,
	INT64: 2,
	INT96: 3,
	FLOAT: 4,
	DOUBLE: 5,
	BYTE_ARRAY: 6,
	FIXED_LEN_BYTE_ARRAY: 7,
};

/**
 * Convert dictionary indices to actual values.
 */
export function dereferDictionary<T = unknown>(
	indices: number[],
	dictionary?: T[],
): (T | undefined)[] {
	if (!dictionary) return indices as unknown as (T | undefined)[];

	const output = new Array<T | undefined>(indices.length);
	for (let i = 0; i < indices.length; i++) {
		output[i] = dictionary[indices[i] || 0];
	}
	return output;
}

/**
 * Convert column values based on schema element's type info.
 */
export function convertColumn(
	data: unknown[],
	element: SchemaElement,
	options: { utf8?: boolean; keepBytes?: boolean } = {},
): unknown[] {
	const { type, converted_type: ctype, scale } = element;
	const { utf8 = true, keepBytes = false } = options;

	// DECIMAL
	if (ctype === CONVERTED_TYPE.DECIMAL) {
		const factor = 10 ** -(scale || 0);
		const result = new Array(data.length);
		for (let i = 0; i < data.length; i++) {
			const v = data[i];
			if (v instanceof Uint8Array) {
				result[i] = parseDecimal(v) * factor;
			} else {
				result[i] = Number(v) * factor;
			}
		}
		return result;
	}

	// INT96 (timestamp)
	if (type === PARQUET_TYPE.INT96) {
		const result = new Array(data.length);
		for (let i = 0; i < data.length; i++) {
			result[i] = int96ToDate(data[i] as bigint);
		}
		return result;
	}

	// DATE
	if (ctype === CONVERTED_TYPE.DATE) {
		const result = new Array(data.length);
		for (let i = 0; i < data.length; i++) {
			result[i] = daysToDate(data[i] as number);
		}
		return result;
	}

	// TIMESTAMP_MILLIS
	if (ctype === CONVERTED_TYPE.TIMESTAMP_MILLIS) {
		const result = new Array(data.length);
		for (let i = 0; i < data.length; i++) {
			result[i] = millisToDate(data[i] as number | bigint);
		}
		return result;
	}

	// TIMESTAMP_MICROS
	if (ctype === CONVERTED_TYPE.TIMESTAMP_MICROS) {
		const result = new Array(data.length);
		for (let i = 0; i < data.length; i++) {
			result[i] = microsToDate(data[i] as bigint);
		}
		return result;
	}

	// UTF8 / BYTE_ARRAY - keep as bytes for interning if requested
	if (
		ctype === CONVERTED_TYPE.UTF8 ||
		(utf8 && type === PARQUET_TYPE.BYTE_ARRAY)
	) {
		if (keepBytes) {
			return data; // Return raw Uint8Array for direct byte interning
		}
		return data.map((v) => bytesToString(v as Uint8Array));
	}

	return data;
}
