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
} from "../ops/index.ts";
import { FilterOperator } from "../ops/filter.ts";
import { compilePushdownFilter } from "../ops/pushdown.ts";
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

	/**
	 * @internal Attempt to push filter predicates down to the CSV source.
	 * Returns the operators that remain after pushdown (may remove the first FilterOperator).
	 * Only works when the source has a setFilter method (CsvSource/CsvParser).
	 */
	private tryPushdownFilter(): {
		remainingOps: Operator[];
		clearPushdown: () => void;
	} {
		const noPushdown = {
			remainingOps: this.operators,
			clearPushdown: () => { },
		};

		if (this.operators.length === 0) return noPushdown;

		const firstOp = this.operators[0];
		if (!(firstOp instanceof FilterOperator) || firstOp.expr === null) {
			return noPushdown;
		}

		// Check if source supports setFilter (CsvSource exposes parser)
		const source = this.source as any;
		const parser = source?.parser ?? source;
		if (typeof parser?.setFilter !== "function") {
			return noPushdown;
		}

		// Attempt to compile the expression as a pushdown predicate
		const predicate = compilePushdownFilter(firstOp.expr, this._schema);
		if (predicate === null) {
			return noPushdown;
		}

		// Apply pushdown to the parser
		parser.setFilter(predicate);

		return {
			remainingOps: this.operators.slice(1),
			clearPushdown: () => {
				parser.setFilter(null);
			},
		};
	}

	/** @internal Add operator to chain */
	withOperator<U = T>(op: Operator): DataFrame<U> {
		return new DataFrame<U>(this.source, this._schema, this._dictionary, [
			...this.operators,
			op,
		]);
	}

	/** @internal Replace operators and add a new one (used by optimizer for filter fusion) */
	withOperators<U = T>(ops: Operator[], extra: Operator): DataFrame<U> {
		return new DataFrame<U>(this.source, this._schema, this._dictionary, [
			...ops,
			extra,
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
	shuffle(): Promise<DataFrame<T>> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}
	sample(_fraction: number): Promise<DataFrame<T>> {
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

	/** Export as columnar data (zero-copy for numeric columns) */
	async toColumns(): Promise<Record<string, unknown>> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Streaming forEach — O(chunk) memory */
	async forEach(_fn: (row: T) => void): Promise<void> {
		throw new Error("Method not implemented. Ensure mixins are loaded.");
	}

	/** Streaming reduce — O(1) memory */
	async reduce<R>(_fn: (acc: R, row: T) => R, _initial: R): Promise<R> {
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
	explode<K extends keyof T>(_column: K): Promise<DataFrame<T>> {
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

	/** @internal Execute pipeline and return stream */
	async *stream(): AsyncGenerator<Chunk> {
		if (this.operators.length === 0) {
			if (Symbol.asyncIterator in this.source) {
				for await (const chunk of this.source as AsyncIterable<Chunk>) {
					yield chunk;
				}
			} else {
				for (const chunk of this.source as Iterable<Chunk>) {
					yield chunk;
				}
			}
			return;
		}

		const pushdown = this.tryPushdownFilter();
		const remainingOps = pushdown.remainingOps;

		if (remainingOps.length === 0) {
			// All operators were pushed down
			try {
				if (Symbol.asyncIterator in this.source) {
					for await (const chunk of this.source as AsyncIterable<Chunk>) {
						yield chunk;
					}
				} else {
					for (const chunk of this.source as Iterable<Chunk>) {
						yield chunk;
					}
				}
			} finally {
				pushdown.clearPushdown();
			}
			return;
		}

		const pipeline = new Pipeline(remainingOps);

		try {
			if (Symbol.asyncIterator in this.source) {
				yield* pipeline.streamAsync(this.source as AsyncIterable<Chunk>);
			} else {
				// pipeline.stream() returns AsyncGenerator (supports operators that may spill)
				for await (const chunk of pipeline.stream(this.source as Iterable<Chunk>)) {
					yield chunk;
				}
			}
		} finally {
			pushdown.clearPushdown();
		}
	}

	/** @internal Execute pipeline using streaming to avoid double-buffering */
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

		const pushdown = this.tryPushdownFilter();
		const remainingOps = pushdown.remainingOps;

		if (remainingOps.length === 0) {
			// All operators pushed down, just materialize source
			const chunks: Chunk[] = [];
			try {
				if (Symbol.asyncIterator in this.source) {
					for await (const chunk of this.source as AsyncIterable<Chunk>) {
						chunks.push(chunk);
					}
				} else {
					for (const chunk of this.source as Iterable<Chunk>) {
						chunks.push(chunk);
					}
				}
			} finally {
				pushdown.clearPushdown();
			}
			let newDictionary = this._dictionary;
			const firstChunk = chunks[0];
			if (firstChunk?.dictionary) {
				newDictionary = firstChunk.dictionary;
			}
			return new DataFrame<T>(chunks, this.currentSchema(), newDictionary, []);
		}

		// Use streaming generators instead of accumulating execute()/executeAsync()
		// This avoids the PipelineResult double-buffer overhead
		const pipeline = new Pipeline(remainingOps);
		const chunks: Chunk[] = [];

		try {
			if (Symbol.asyncIterator in this.source) {
				for await (const chunk of pipeline.streamAsync(this.source as AsyncIterable<Chunk>)) {
					chunks.push(chunk);
				}
			} else {
				// pipeline.stream() is AsyncGenerator (operators may be async during spill)
				for await (const chunk of pipeline.stream(this.source as Iterable<Chunk>)) {
					chunks.push(chunk);
				}
			}
		} finally {
			pushdown.clearPushdown();
		}

		// Use dictionary from result chunks if available (e.g. from GroupBy)
		// Otherwise fall back to current dictionary
		let newDictionary = this._dictionary;
		const firstChunk = chunks[0];
		if (firstChunk?.dictionary) {
			newDictionary = firstChunk.dictionary;
		}

		return new DataFrame<T>(
			chunks,
			this.currentSchema(),
			newDictionary,
			[],
		);
	}
}
