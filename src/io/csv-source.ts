/**
 * CSV source for reading CSV files.
 *
 * Provides both streaming (async iterator) and collect modes.
 */
/** biome-ignore-all lint/style/noNonNullAssertion: It imposible to be null */

import type { Chunk } from "../buffer/chunk.ts";
import type { Dictionary } from "../buffer/dictionary.ts";
import type { DType } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import { createSchema, type Schema } from "../types/schema.ts";
import {
	type CsvOptions,
	type CsvParser,
	createCsvParser,
} from "./csv-parser.ts";

/** Schema specification for CSV reading */
export type CsvSchemaSpec = Record<string, DType>;

/**
 * CSV source that reads from a file or string.
 */
export class CsvSource {
	private readonly parser: CsvParser;
	private readonly source: string | Bun.BlobPart;
	private readonly isFile: boolean;

	private constructor(
		source: string | Bun.BlobPart,
		parser: CsvParser,
		isFile: boolean,
	) {
		this.source = source;
		this.parser = parser;
		this.isFile = isFile;
	}

	/**
	 * Create a CSV source from a file path.
	 */
	static fromFile(
		path: string,
		schemaSpec: CsvSchemaSpec,
		options?: CsvOptions,
	): Result<CsvSource> {
		// If projection is present, we must subset the schema spec
		// because CsvParser will map projected columns to 0, 1, 2...
		// and the DataFrame schema must match that.
		let effectiveSpec = schemaSpec;
		if (options?.projection && options.projection.length > 0) {
			const keys = Object.keys(schemaSpec);
			const newSpec: CsvSchemaSpec = {};
			for (let i = 0; i < options.projection.length; i++) {
				const idx: number = options.projection[i]!;
				if (idx !== undefined && idx < keys.length) {
					const key = keys[idx];
					const spec = schemaSpec[key ?? ""];
					if (key && spec) {
						newSpec[key] = spec;
					}
				}
			}
			effectiveSpec = newSpec;
		}

		const schemaResult = createSchema(effectiveSpec);
		if (schemaResult.error !== ErrorCode.None) {
			return err(schemaResult.error);
		}

		const parser = createCsvParser(schemaResult.value, options);
		return ok(new CsvSource(path, parser, true));
	}

	/**
	 * Create a CSV source from a string.
	 */
	static fromString(
		content: string,
		schemaSpec: CsvSchemaSpec,
		options?: CsvOptions,
	): Result<CsvSource> {
		// If projection is present, we must subset the schema spec
		let effectiveSpec = schemaSpec;
		if (options?.projection && options.projection.length > 0) {
			const keys = Object.keys(schemaSpec);
			const newSpec: CsvSchemaSpec = {};
			for (let i = 0; i < options.projection.length; i++) {
				const idx: number = options.projection[i]!;
				if (idx !== undefined && idx < keys.length) {
					const key = keys[idx];
					const spec = schemaSpec[key ?? ""];
					if (key && spec) {
						newSpec[key] = spec;
					}
				}
			}
			effectiveSpec = newSpec;
		}

		const schemaResult = createSchema(effectiveSpec);
		if (schemaResult.error !== ErrorCode.None) {
			return err(schemaResult.error);
		}

		const parser = createCsvParser(schemaResult.value, options);
		return ok(new CsvSource(content, parser, false));
	}

	/**
	 * Get the dictionary for string values.
	 */
	getDictionary(): Dictionary {
		return this.parser.getDictionary();
	}

	/**
	 * Get the schema used for parsing (reflects projection if any).
	 */
	getSchema(): Schema {
		return this.parser.getSchema();
	}

	/**
	 * Parse string content synchronously (for string sources only).
	 */
	parseSync(): Chunk[] {
		if (this.isFile) {
			throw new Error(
				"parseSync() only works for string sources, use collectChunks() for files",
			);
		}

		const encoder = new TextEncoder();
		const bytes = encoder.encode(this.source as string);
		const chunks = this.parser.parse(bytes);
		const final = this.parser.finish();
		if (final) {
			chunks.push(final);
		}
		return chunks;
	}

	/**
	 * Read all chunks (blocking).
	 * Uses streaming internally to avoid loading entire file into memory at once.
	 */
	async collectChunks(): Promise<Result<Chunk[]>> {
		const chunks: Chunk[] = [];

		try {
			if (this.isFile) {
				// Stream the file in chunks rather than loading entire file
				// into memory with file.arrayBuffer()
				for await (const chunk of this.stream()) {
					chunks.push(chunk);
				}
			} else {
				const encoder = new TextEncoder();
				const bytes = encoder.encode(this.source as string);
				chunks.push(...this.parser.parse(bytes));

				const final = this.parser.finish();
				if (final) {
					chunks.push(final);
				}
			}

			return ok(chunks);
		} catch {
			return err(ErrorCode.ReadError);
		}
	}

	[Symbol.asyncIterator](): AsyncIterator<Chunk> {
		// Return the generator from stream()
		// Note: This relies on stream() returning an AsyncGenerator which is an AsyncIterator
		return this.stream();
	}

	/**
	 * Stream chunks (async iterator).
	 */
	async *stream(_: number = 512 * 1024): AsyncGenerator<Chunk> {
		this.parser.reset();
		if (this.isFile) {
			const file = Bun.file(this.source as string);
			const stream = file.stream();
			// Use raw reader
			const reader = stream.getReader();

			try {
				while (true) {
					const { done, value } = await reader.read();
					if (done) break;

					// value is Uint8Array
					const chunks = this.parser.parse(value);
					for (const chunk of chunks) {
						yield chunk;
					}
				}

				const final = this.parser.finish();
				if (final) {
					yield final;
				}
			} finally {
				reader.releaseLock();
			}
		} else {
			// For string source, encode to Uint8Array once
			const encoder = new TextEncoder();
			const bytes = encoder.encode(this.source as string);
			const chunks = this.parser.parse(bytes);
			for (const chunk of chunks) {
				yield chunk;
			}

			const final = this.parser.finish();
			if (final) {
				yield final;
			}
		}
	}
}

/**
 * Convenience function to read CSV from a file.
 */
export function readCsvFile(
	path: string,
	schema: CsvSchemaSpec,
	options?: CsvOptions,
): Result<CsvSource> {
	return CsvSource.fromFile(path, schema, options);
}

/**
 * Convenience function to read CSV from a string.
 */
export function readCsvString(
	content: string,
	schema: CsvSchemaSpec,
	options?: CsvOptions,
): Result<CsvSource> {
	return CsvSource.fromString(content, schema, options);
}
