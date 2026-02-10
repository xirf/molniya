/* DATAFRAME PUBLIC API
/*-----------------------------------------------------
/* Main DataFrame export with all methods attached
/* ==================================================== */

// Core class
export { DataFrame } from "./core.ts";

import { addAggMethods } from "./aggregation.ts";
import { addCleaningMethods } from "./cleaning.ts";
import { addConcatMethods } from "./concatenation.ts";
import { DataFrame } from "./core.ts";
import { addDedupMethods } from "./dedup.ts";
import { addExecutionMethods } from "./execution.ts";
// Method modules
import { addFilteringMethods } from "./filtering.ts";
import { addInspectionMethods } from "./inspection.ts";
import { addJoinMethods } from "./joins.ts";
import { addLimitingMethods } from "./limiting.ts";
import { addProjectionMethods } from "./projection.ts";
import { addSortingMethods } from "./sorting.ts";
import { addStringMethods } from "./string-ops.ts";
import { addTransformMethods } from "./transformation.ts";

/* ATTACH METHODS TO PROTOTYPE
/*-----------------------------------------------------
/* Wire all modular methods to DataFrame class
/* ==================================================== */

addFilteringMethods(DataFrame.prototype);
addProjectionMethods(DataFrame.prototype);
addTransformMethods(DataFrame.prototype);
addCleaningMethods(DataFrame.prototype);
addDedupMethods(DataFrame.prototype);
addStringMethods(DataFrame.prototype);
addLimitingMethods(DataFrame.prototype);
addAggMethods(DataFrame.prototype);
addSortingMethods(DataFrame.prototype);
addJoinMethods(DataFrame.prototype);
addConcatMethods(DataFrame.prototype);
addExecutionMethods(DataFrame.prototype);
addInspectionMethods(DataFrame.prototype);

/* HELPER FUNCTIONS
/*-----------------------------------------------------
/* Convenience functions for creating DataFrames
/* ==================================================== */

import { ColumnBuffer } from "../buffer/column-buffer.ts";
import { Chunk } from "../buffer/chunk.ts";
import { createDictionary } from "../buffer/dictionary.ts";
import type { Expr } from "../expr/ast.ts";
import type { ColumnRef } from "../expr/builders.ts";
import { type CsvOptions, type CsvSchemaSpec, CsvSource } from "../io/index.ts";
import { DType, DTypeKind } from "../types/dtypes.ts";
import { unwrap } from "../types/error.ts";
import { createSchema, type SchemaSpec } from "../types/schema.ts";

export type CsvReadOptions = CsvOptions & {
	filter?: Expr | ColumnRef;
	offload?: boolean;
	offloadFile?: string;
};

export interface ParquetReadOptions {
	projection?: string[];
	filter?: Expr | ColumnRef;
	offload?: boolean;
	offloadFile?: string;
}

/**
 * Create DataFrame from records (array of objects).
 * Directly converts to columnar format without CSV intermediate step.
 */
export function fromRecords<T = Record<string, unknown>>(
	records: Record<string, unknown>[],
	schema: SchemaSpec,
): DataFrame<T> {
	if (records.length === 0) {
		const s = unwrap(createSchema(schema));
		return DataFrame.empty<T>(s, createDictionary());
	}

	const s = unwrap(createSchema(schema));
	const dictionary = createDictionary();
	const rowCount = records.length;

	// Create column buffers directly from records
	const columnBuffers: ColumnBuffer<DTypeKind>[] = [];

	for (const colDef of s.columns) {
		const kind = colDef.dtype.kind;
		const nullable = colDef.dtype.nullable;
		const buffer = new ColumnBuffer(kind, rowCount, nullable);
		const colName = colDef.name;

		for (let i = 0; i < rowCount; i++) {
			const value = records[i]?.[colName];

			if (value === null || value === undefined) {
				buffer.setNull(i, true);
			} else if (kind === DTypeKind.String) {
				const dictIdx = dictionary.internString(String(value));
				buffer.set(i, dictIdx);
			} else if (kind === DTypeKind.Boolean) {
				buffer.set(i, value ? 1 : 0);
			} else if (kind === DTypeKind.Timestamp) {
				// Convert timestamp to BigInt (milliseconds since epoch)
				if (value instanceof Date) {
					buffer.set(i, BigInt(value.getTime()));
				} else if (typeof value === "bigint") {
					buffer.set(i, value);
				} else {
					buffer.set(i, BigInt(value as string | number | bigint | boolean));
				}
			} else {
				buffer.set(i, value as number | bigint);
			}
		}

		buffer.setLength(rowCount);
		columnBuffers.push(buffer);
	}

	const chunk = new Chunk(s, columnBuffers, dictionary);
	return DataFrame.fromChunks<T>([chunk], s, dictionary);
}

/**
 * Create DataFrame from CSV string.
 */
export function fromCsvString<T = Record<string, unknown>>(
	csvString: string,
	schema: CsvSchemaSpec,
	options?: CsvReadOptions,
): DataFrame<T> {
	const source = unwrap(CsvSource.fromString(csvString, schema, options));
	const chunks = source.parseSync();
	const df = DataFrame.fromChunks<T>(
		chunks,
		source.getSchema(),
		source.getDictionary(),
	);
	return options?.filter ? df.filter(options.filter) : df;
}

/**
 * Read CSV file and create DataFrame using true streaming.
 * Memory-bounded: processes file in chunks without loading all data.
 */
export async function readCsv<T = Record<string, unknown>>(
	path: string,
	schema: CsvSchemaSpec,
	options?: CsvReadOptions,
): Promise<DataFrame<T>> {
	// Import offload utilities
	const { OffloadManager } = await import("../io/offload-manager.ts");
	const { writeToMbf, isCacheValid } = await import("../io/offload-utils.ts");
	const { readMbf } = await import("../io/index.ts");

	// Handle offloading if requested
	if (options?.offload || options?.offloadFile) {
		// Determine cache path
		let cachePath: string;
		let isTemp = false;

		if (options.offloadFile) {
			// User-specified persistent cache
			cachePath = options.offloadFile;
		} else {
			// Auto-managed temp cache
			await OffloadManager.ensureCacheDir();
			const cacheKey = await OffloadManager.generateCacheKey(path);
			cachePath = OffloadManager.getCachePath(cacheKey);
			isTemp = true;
		}

		// Check if valid cache exists
		if (await isCacheValid(cachePath, path)) {
			// Use cached MBF
			const mbfResult = unwrap(await readMbf(cachePath));
			const df = DataFrame.fromStream<T>(
				mbfResult,
				mbfResult.getSchema(),
				null, // MBF chunks have their own dictionaries
			);
			return options?.filter ? df.filter(options.filter) : df;
		}

		// No valid cache: read CSV → write to MBF → return MBF DataFrame
		const source = unwrap(CsvSource.fromFile(path, schema, options));
		let df = DataFrame.fromStream<T>(
			source as unknown as AsyncIterable<Chunk>,
			source.getSchema(),
			source.getDictionary(),
		);

		// Apply filter if specified
		if (options?.filter) {
			df = df.filter(options.filter);
		}

		// Write to MBF
		await writeToMbf(df as DataFrame, cachePath);

		// Register for cleanup if temp
		if (isTemp) {
			OffloadManager.registerTempFile(cachePath);
		}

		// Return DataFrame backed by MBF
		const mbfResult = unwrap(await readMbf(cachePath));
		return DataFrame.fromStream<T>(
			mbfResult,
			mbfResult.getSchema(),
			null, // MBF chunks have their own dictionaries
		);
	}

	// No offload: normal streaming path
	const source = unwrap(CsvSource.fromFile(path, schema, options));

	// Return lazy DataFrame immediately
	// source implements AsyncIterable, so it creates a new stream on iteration
	const df = DataFrame.fromStream<T>(
		source as unknown as AsyncIterable<Chunk>,
		source.getSchema(),
		source.getDictionary(),
	);
	return options?.filter ? df.filter(options.filter) : df;
}

/**
 * Read Parquet file and create DataFrame.
 * Note: Currently loads entirely into memory.
 */
import { readParquet as ioReadParquet } from "../io/index.ts";

export async function readParquet<T = Record<string, unknown>>(
	path: string,
	options?: ParquetReadOptions,
): Promise<DataFrame<T>> {
	// Import offload utilities
	const { OffloadManager } = await import("../io/offload-manager.ts");
	const { writeToMbf, isCacheValid } = await import("../io/offload-utils.ts");
	const { readMbf } = await import("../io/index.ts");

	// Handle offloading if requested
	if (options?.offload || options?.offloadFile) {
		// Determine cache path
		let cachePath: string;
		let isTemp = false;

		if (options.offloadFile) {
			// User-specified persistent cache
			cachePath = options.offloadFile;
		} else {
			// Auto-managed temp cache
			await OffloadManager.ensureCacheDir();
			const cacheKey = await OffloadManager.generateCacheKey(path);
			cachePath = OffloadManager.getCachePath(cacheKey);
			isTemp = true;
		}

		// Check if valid cache exists
		if (await isCacheValid(cachePath, path)) {
			// Use cached MBF
			const mbfResult = unwrap(await readMbf(cachePath));
			const df = DataFrame.fromStream<T>(
				mbfResult,
				mbfResult.getSchema(),
				null, // MBF chunks have their own dictionaries
			);
			const projected = options?.projection?.length
				? df.select(...(options.projection as (keyof T & string)[]))
				: df;
			return options?.filter ? projected.filter(options.filter) : projected;
		}

		// No valid cache: read Parquet → write to MBF → return MBF DataFrame
		let df = (await ioReadParquet(path)) as DataFrame<T>;
		
		// Apply projection and filter if specified
		const projected = options?.projection?.length
			? df.select(...(options.projection as (keyof T & string)[]))
			: df;
		df = options?.filter ? projected.filter(options.filter) : projected;

		// Write to MBF
		await writeToMbf(df as DataFrame, cachePath);

		// Register for cleanup if temp
		if (isTemp) {
			OffloadManager.registerTempFile(cachePath);
		}

		// Return DataFrame backed by MBF
		const mbfResult = unwrap(await readMbf(cachePath));
		return DataFrame.fromStream<T>(
			mbfResult,
			mbfResult.getSchema(),
			null, // MBF chunks have their own dictionaries
		);
	}

	// No offload: normal path
	const df = (await ioReadParquet(path)) as DataFrame<T>;
	const projected = options?.projection?.length
		? df.select(...(options.projection as (keyof T & string)[]))
		: df;
	return options?.filter ? projected.filter(options.filter) : projected;
}

/**
 * Infer DType from a column's values.
 */
function inferColumnType(values: ArrayLike<unknown>): DType {
	// Find first non-null value
	for (let i = 0; i < values.length; i++) {
		const v = values[i];
		if (v === null || v === undefined) continue;

		if (typeof v === "number") {
			if (Number.isInteger(v) && v >= -2147483648 && v <= 2147483647) {
				return DType.int32;
			}
			return DType.float64;
		}
		if (typeof v === "bigint") return DType.int64;
		if (typeof v === "string") return DType.string;
		if (typeof v === "boolean") return DType.boolean;
		if (v instanceof Date) return DType.timestamp;
	}

	// Default to nullable string if all null
	return DType.nullable.string;
}

/**
 * Create DataFrame from column-oriented data.
 * @param columns Object mapping column names to arrays of values
 * @param schema Optional schema specification (inferred if not provided)
 */
export function fromColumns<T = Record<string, unknown>>(
	columns: Record<string, ArrayLike<unknown>>,
	schema?: SchemaSpec,
): DataFrame<T> {
	const columnNames = Object.keys(columns);
	if (columnNames.length === 0) {
		throw new Error("Cannot create DataFrame from empty columns");
	}

	const rowCount = columns[columnNames[0]!]!.length;

	// Validate all columns have same length
	for (const name of columnNames) {
		if (columns[name]!.length !== rowCount) {
			throw new Error(
				`Column '${name}' has different length than other columns`,
			);
		}
	}

	// Infer schema if not provided
	const actualSchema: SchemaSpec = schema ?? {};
	if (!schema) {
		for (const name of columnNames) {
			actualSchema[name] = inferColumnType(columns[name]!);
		}
	}

	const s = unwrap(createSchema(actualSchema));
	const dictionary = createDictionary();

	// Create column buffers
	const columnBuffers: ColumnBuffer<DTypeKind>[] = [];

	for (const colDef of s.columns) {
		const values = columns[colDef.name]!;
		const kind = colDef.dtype.kind;
		const nullable = colDef.dtype.nullable;
		const buffer = new ColumnBuffer(kind, rowCount, nullable);

		for (let i = 0; i < rowCount; i++) {
			const value = values[i];
			if (value === null || value === undefined) {
				buffer.setNull(i, true);
			} else if (kind === DTypeKind.String) {
				const dictIdx = dictionary.internString(String(value));
				buffer.set(i, dictIdx);
			} else if (kind === DTypeKind.Boolean) {
				buffer.set(i, value ? 1 : 0);
			} else if (kind === DTypeKind.Timestamp) {
				// Convert timestamp to BigInt (milliseconds since epoch)
				if (value instanceof Date) {
					buffer.set(i, BigInt(value.getTime()));
				} else if (typeof value === "bigint") {
					buffer.set(i, value);
				} else {
					buffer.set(i, BigInt(value as string | number | bigint | boolean));
				}
			} else {
				buffer.set(i, value as number | bigint);
			}
		}

		buffer.setLength(rowCount);
		columnBuffers.push(buffer);
	}

	const chunk = new Chunk(s, columnBuffers, dictionary);
	return DataFrame.fromChunks<T>([chunk], s, dictionary);
}

/**
 * Create DataFrame from parallel arrays (alias for fromColumns with ArrayLike support).
 * @param arrays Object with column names as keys and arrays as values
 * @param schema Optional schema specification
 */
export function fromArrays<T = Record<string, unknown>>(
	arrays: Record<string, ArrayLike<unknown>>,
	schema?: SchemaSpec,
): DataFrame<T> {
	return fromColumns(arrays, schema);
}

/**
 * Create DataFrame with a sequence of numbers.
 * @param start Starting value (inclusive)
 * @param end Ending value (exclusive)
 * @param step Step size (default 1)
 * @param columnName Column name (default "value")
 */
export function range(
	start: number,
	end: number,
	step: number = 1,
	columnName: string = "value",
): DataFrame {
	const values: number[] = [];
	if (step > 0) {
		for (let i = start; i < end; i += step) {
			values.push(i);
		}
	} else if (step < 0) {
		for (let i = start; i > end; i += step) {
			values.push(i);
		}
	} else {
		throw new Error("Step cannot be zero");
	}

	return fromColumns({ [columnName]: values }, { [columnName]: DType.float64 });
}

/* EXPRESSION BUILDERS
/*-----------------------------------------------------
/* Re-export expression builders for convenience
/* ==================================================== */

export {
	add,
	avg,
	between,
	coalesce,
	col,
	count,
	countDistinct,
	div,
	lit,
	max,
	median,
	min,
	mul,
	std,
	sub,
	sum,
	variance,
	when,
	WhenBuilder,
} from "../expr/builders.ts";