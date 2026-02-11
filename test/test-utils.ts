/**
 * Test utilities for integration tests.
 * 
 * Provides helper functions to simplify test setup.
 */

import { fromRecords as coreFromRecords } from "../src/dataframe/dataframe.ts";
import { DType, DTypeKind } from "../src/types/dtypes.ts";
import type { SchemaSpec } from "../src/types/schema.ts";

/**
 * Create DataFrame from records with automatic schema inference.
 * Analyzes first record to determine column types.
 */
export function fromRecords(records: Record<string, unknown>[]) {
	if (records.length === 0) {
		throw new Error("Cannot infer schema from empty array");
	}

	// Infer schema from first record
	const firstRecord = records[0]!;
	const schema: SchemaSpec = {};

	for (const [key, value] of Object.entries(firstRecord)) {
		if (value === null || value === undefined) {
			// Check other records to infer type
			let inferredType: DType | null = null;
			for (let i = 1; i < Math.min(records.length, 100); i++) {
				const val = records[i]?.[key];
				if (val !== null && val !== undefined) {
					inferredType = inferType(val);
					break;
				}
			}
			schema[key] = inferredType ?? DType.nullable.string;
		} else {
			schema[key] = inferType(value);
		}
	}

	return coreFromRecords(records, schema);
}

/**
 * Infer DType from a JavaScript value.
 */
function inferType(value: unknown): DType {
	if (typeof value === "string") {
		return DType.string;
	} else if (typeof value === "number") {
		// Use int32 for integers, float64 for floats
		if (Number.isInteger(value) && value >= -2147483648 && value <= 2147483647) {
			return DType.int32;
		}
		return DType.float64;
	} else if (typeof value === "boolean") {
		return DType.boolean;
	} else if (typeof value === "bigint") {
		return DType.int64;
	} else if (value instanceof Date) {
		return DType.timestamp;
	} else {
		// Default to string for unknown types
		return DType.string;
	}
}
