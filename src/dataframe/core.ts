/* DATAFRAME CORE
/*-----------------------------------------------------
/* Base DataFrame class with core structure and utilities
/* ==================================================== */

import type { Chunk } from "../buffer/chunk.ts";
import type { Dictionary } from "../buffer/dictionary.ts";
import {
	type ComputedColumn,
	type Operator,
	Pipeline,
	type PipelineResult,
} from "../ops/index.ts";
import { unwrap } from "../types/error.ts";
import { getColumnNames, type Schema } from "../types/schema.ts";

/**
 * DataFrame - columnar data structure with lazy evaluation.
 *
 * Internal fields are public for module access but shouldn't be used externally.
 * T: Compile-time schema definition (Phantom Type)
 */
export class DataFrame<T = Record<string, unknown>> {
	/** @internal */ source: Iterable<Chunk> | AsyncIterable<Chunk>;
	/** @internal */ _schema: Schema;
	/** @internal */ _dictionary: Dictionary | null;
	/** @internal */ operators: Operator[] = [];

	/** @internal */
	constructor(
		source: Iterable<Chunk> | AsyncIterable<Chunk>,
		schema: Schema,
		dictionary: Dictionary | null,
		operators?: Operator[],
	) {
		this.source = source;
		this._schema = schema;
		this._dictionary = dictionary;
		if (operators) {
			this.operators.push(...operators);
		}
	}

	/* STATIC CONSTRUCTORS
  /*-----------------------------------------------------
  /* Factory methods to create DataFrames
  /* ==================================================== */

	static fromChunks<T = Record<string, unknown>>(
		chunks: Chunk[],
		schema: Schema,
		dictionary: Dictionary | null,
	): DataFrame<T> {
		return new DataFrame<T>(chunks, schema, dictionary);
	}

	static fromStream<T = Record<string, unknown>>(
		stream: AsyncIterable<Chunk>,
		schema: Schema,
		dictionary: Dictionary | null,
	): DataFrame<T> {
		return new DataFrame<T>(stream, schema, dictionary);
	}

	static empty<T = Record<string, unknown>>(
		schema: Schema,
		dictionary: Dictionary | null,
	): DataFrame<T> {
		return new DataFrame<T>([], schema, dictionary);
	}

	/* PROPERTIES
  /*-----------------------------------------------------
  /* Read-only properties for schema and column access
  /* ==================================================== */

	get schema(): Schema {
		return this.currentSchema();
	}

	get columnNames(): string[] {
		return getColumnNames(this.currentSchema());
	}

	/* INTERNAL UTILITIES
  /*-----------------------------------------------------
  /* Private methods for DataFrame operations
  /* ==================================================== */

	/** @internal Get current schema after all operators */
	currentSchema(): Schema {
		if (this.operators.length === 0) {
			return this._schema;
		}
		const lastOp = this.operators[this.operators.length - 1];
		// This should not happen due to length check above, but for type safety:
		if (!lastOp) throw new Error("Invariant failed: Operator missing");
		return lastOp.outputSchema;
	}

	/** @internal Add operator to chain */
	withOperator<U = T>(op: Operator): DataFrame<U> {
		return new DataFrame<U>(this.source, this._schema, this._dictionary, [
			...this.operators,
			op,
		]);
	}

	/* OPERATORS (Typed Definitions)
  /*-----------------------------------------------------
  /* Methods are implemented via mixins but defined here for TS
  /* ==================================================== */

	/** Select specific columns */
	select<K extends keyof T>(
		..._columns: (K & string)[]
	): DataFrame<Pick<T, K>> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Drop specific columns */
	drop<K extends keyof T>(..._columns: (K & string)[]): DataFrame<Omit<T, K>> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Filter rows based on expression */
	filter(_expr: unknown): DataFrame<T> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Rename columns */
	rename(
		_mapping: Partial<Record<keyof T, string>>,
	): DataFrame<Record<string, unknown>> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Group By */
	groupBy<K extends keyof T>(..._keys: (K & string)[]): unknown {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Sort By */
	orderBy(_expr: unknown, _ascending?: boolean): DataFrame<T> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Limit/Take */
	limit(_n: number): DataFrame<T> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Count rows */
	async count(): Promise<number> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Show first N rows */
	async show(_n?: number): Promise<void> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/* TRANSFORMATION & CLEANING (Typed Definitions)
  /*----------------------------------------------------- */

	/** Add a computed column */
	withColumn<K extends string>(
		_name: K,
		_expr: unknown,
	): DataFrame<T & Record<K, unknown>> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Add multiple computed columns */
	withColumns(
		_columns: ComputedColumn[] | Record<string, unknown>,
	): DataFrame<Record<string, unknown>> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Cast column type */
	cast(_column: keyof T, _targetDType: unknown): DataFrame<T> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Fill null values */
	fillNull(
		_column: keyof T,
		_fillValue: number | bigint | string | boolean,
	): DataFrame<T> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Drop rows with null values */
	dropNull(_columns?: keyof T | (keyof T)[]): DataFrame<T> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	// Set Operations
	crossJoin<U>(
		_other: DataFrame<U>,
		_suffix?: string,
	): Promise<DataFrame<T & U>> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}
	antiJoin<U>(
		_other: DataFrame<U>,
		_on: keyof T | (keyof T)[],
	): Promise<DataFrame<T>> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}
	semiJoin<U>(
		_other: DataFrame<U>,
		_on: keyof T | (keyof T)[],
	): Promise<DataFrame<T>> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	// Transformation
	explode<K extends keyof T>(_column: K): DataFrame<T> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	// Aggregation Shortcuts
	min(_column: keyof T): Promise<number | string | null> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}
	max(_column: keyof T): Promise<number | string | null> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}
	mean(_column: keyof T): Promise<number | null> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	// Inspection
	printSchema(): void {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}
	explain(): string {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	// Utils
	isEmpty(): Promise<boolean> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** @internal Execute pipeline */
	async collect(): Promise<DataFrame<T>> {
		if (this.operators.length === 0) {
			// If source is already materialized (array), return this
			if (Array.isArray(this.source)) {
				return this;
			}
			// Materialize async source
			const chunks: Chunk[] = [];
			for await (const chunk of this.source) {
				chunks.push(chunk);
			}
			return new DataFrame<T>(chunks, this._schema, this._dictionary);
		}

		const pipeline = new Pipeline(this.operators);
		let result: PipelineResult;

		if (Symbol.asyncIterator in this.source) {
			result = unwrap(
				await pipeline.executeAsync(this.source as AsyncIterable<Chunk>),
			);
		} else {
			result = unwrap(pipeline.execute(this.source as Iterable<Chunk>));
		}

		// Use dictionary from result chunks if available (e.g. from GroupBy)
		// Otherwise fall back to current dictionary
		let newDictionary = this._dictionary;
		const firstChunk = result.chunks[0];
		if (firstChunk?.dictionary) {
			newDictionary = firstChunk.dictionary;
		}

		return new DataFrame<T>(
			result.chunks,
			this.currentSchema(),
			newDictionary,
			[],
		);
	}
}
