/* DATAFRAME PUBLIC API
/*-----------------------------------------------------
/* Main DataFrame export with all methods attached
/* ==================================================== */

// Core class
export { DataFrame } from "./core.ts";

import type { Chunk } from "../buffer/chunk.ts";
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

import { createDictionary } from "../buffer/dictionary.ts";
import { type CsvOptions, type CsvSchemaSpec, CsvSource } from "../io/index.ts";
import { unwrap } from "../types/error.ts";
import { createSchema, type SchemaSpec } from "../types/schema.ts";

/**
 * Create DataFrame from records (array of objects).
 */
/**
 * Create DataFrame from records (array of objects).
 */
export function fromRecords<T = Record<string, unknown>>(
	records: Record<string, unknown>[],
	schema: SchemaSpec,
): DataFrame<T> {
	if (records.length === 0) {
		const s = unwrap(createSchema(schema));
		return DataFrame.empty<T>(s, createDictionary());
	}

	// Parse records manually into CSV-like format and use parser
	const headers = Object.keys(schema);
	const csvLines = [headers.join(",")];
	for (const record of records) {
		const values = headers.map((h) => String(record[h] ?? ""));
		csvLines.push(values.join(","));
	}
	const csvString = csvLines.join("\n");

	const source = unwrap(CsvSource.fromString(csvString, schema));
	const chunks = source.parseSync();
	const s = unwrap(createSchema(schema));
	return DataFrame.fromChunks<T>(chunks, s, source.getDictionary());
}

/**
 * Create DataFrame from CSV string.
 */
export function fromCsvString<T = Record<string, unknown>>(
	csvString: string,
	schema: CsvSchemaSpec,
	options?: CsvOptions,
): DataFrame<T> {
	const source = unwrap(CsvSource.fromString(csvString, schema, options));
	const chunks = source.parseSync();
	return DataFrame.fromChunks<T>(
		chunks,
		source.getSchema(),
		source.getDictionary(),
	);
}

/**
 * Read CSV file and create DataFrame using true streaming.
 * Memory-bounded: processes file in chunks without loading all data.
 */
export async function readCsv<T = Record<string, unknown>>(
	path: string,
	schema: CsvSchemaSpec,
	options?: CsvOptions,
): Promise<DataFrame<T>> {
	const source = unwrap(CsvSource.fromFile(path, schema, options));

	// Return lazy DataFrame immediately
	// source implements AsyncIterable, so it creates a new stream on iteration
	return DataFrame.fromStream<T>(
		source as unknown as AsyncIterable<Chunk>,
		source.getSchema(),
		source.getDictionary(),
	);
}

/**
 * Read Parquet file and create DataFrame.
 * Note: Currently loads entirely into memory.
 */
import { readParquet as ioReadParquet } from "../io/index.ts";

export async function readParquet<T = Record<string, unknown>>(
	path: string,
): Promise<DataFrame<T>> {
	return ioReadParquet(path) as Promise<DataFrame<T>>;
}

/* EXPRESSION BUILDERS
/*-----------------------------------------------------
/* Re-export expression builders for convenience
/* ==================================================== */

export {
	add,
	avg,
	col,
	count,
	div,
	lit,
	max,
	min,
	mul,
	sub,
	sum,
} from "../expr/builders.ts";
