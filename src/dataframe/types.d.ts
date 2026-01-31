/* DATAFRAME TYPE DECLARATIONS
/*-----------------------------------------------------
/* Type definitions for all DataFrame methods
/* ==================================================== */

import type { AggExpr, Expr } from "../expr/ast.ts";
import type { ComputedColumn, SortKey } from "../ops/index.ts";
import type { ColumnRef } from "../expr/builders.ts";
import type { DType } from "../types/dtypes.ts";

// Type mapping for aggregation results based on aggregation type
type SumResult = number | null;
type CountResult = bigint;

/** Infer result type from an aggregation expression */
type InferAggResult<E extends Expr> = E extends AggExpr
	? E["type"] extends "sum" | "avg"
		? SumResult
		: E["type"] extends "count"
			? CountResult
			: E["type"] extends "min" | "max" | "first" | "last"
				? unknown
				: unknown
	: unknown;

/** Build result type from aggregation specs */
type BuildAggResult<Specs extends readonly TypedAggSpec[]> = {
	[K in Specs[number] as K["name"]]: InferAggResult<K["expr"]>;
};

declare module "./core.ts" {
	interface DataFrame<T = Record<string, unknown>> {
		// Filtering
		filter(expr: Expr | ColumnRef): DataFrame<T>;
		where(expr: Expr): DataFrame<T>;

		// Projection
		select<K extends keyof T>(
			...columns: (K & string)[]
		): DataFrame<Pick<T, K>>;
		drop<K extends keyof T>(...columns: (K & string)[]): DataFrame<Omit<T, K>>;

		// Improved rename with Mapped Types
		rename<M extends Partial<Record<keyof T, string>>>(
			mapping: M,
		): DataFrame<{
			[K in keyof T as K extends keyof M
				? M[K] extends string
					? M[K]
					: K
				: K]: T[K];
		}>;

		// Transformation
		withColumn<K extends string, V = unknown>(
			name: K,
			expr: Expr | ColumnRef,
		): DataFrame<T & Record<K, V>>;

		// Overload for multiple columns map
		withColumns(
			columns: ComputedColumn[] | Record<string, Expr | ColumnRef>,
		): DataFrame<Record<string, unknown>>;

		// Explode array columns
		explode<K extends keyof T>(column: K): DataFrame<T>; // TODO: Refine return type if T[K] is known array

		// Data Cleaning
		cast(column: keyof T, targetDType: DType): DataFrame<T>;
		fillNull(
			column: keyof T,
			fillValue: number | bigint | string | boolean,
		): DataFrame<T>;
		dropNull(columns?: keyof T | (keyof T)[]): DataFrame<T>;

		// Deduplication
		unique(columns?: keyof T | (keyof T)[], keepFirst?: boolean): DataFrame<T>;
		dropDuplicates(
			columns?: keyof T | (keyof T)[],
			keepFirst?: boolean,
		): DataFrame<T>;
		distinct(columns?: keyof T | (keyof T)[]): DataFrame<T>;

		// String Operations
		trim(column: keyof T): DataFrame<T>;
		upper(column: keyof T): DataFrame<T>;
		lower(column: keyof T): DataFrame<T>;
		replace(
			column: keyof T,
			pattern: string,
			replacement: string,
			all?: boolean,
		): DataFrame<T>;
		substring(column: keyof T, start: number, length?: number): DataFrame<T>;
		pad(
			column: keyof T,
			length: number,
			fillChar?: string,
			side?: "left" | "right" | "both",
		): DataFrame<T>;

		// Limiting
		limit(count: number): DataFrame<T>;
		head(count?: number): DataFrame<T>;
		tail(count?: number): DataFrame<T>;
		slice(start: number, count: number): DataFrame<T>;

		// Aggregation
		// Returns a single row with aggregation results - type-safe version with TypedAggSpec
		agg<const Specs extends readonly TypedAggSpec[]>(
			specs: Specs,
		): DataFrame<BuildAggResult<Specs>>;

		// Improved GroupBy returning RelationalGroupedDataset
		groupBy<K extends keyof T>(
			keyColumns: (K & string) | (K & string)[],
		): import("./aggregation.ts").RelationalGroupedDataset<T>;

		// Global stats shortcuts
		min(column: keyof T): Promise<number | string | null>;
		max(column: keyof T): Promise<number | string | null>;
		mean(column: keyof T): Promise<number | null>;

		// Sorting
		sort(keys: keyof T | (keyof T)[] | SortKey[]): DataFrame<T>;
		orderBy(keys: keyof T | (keyof T)[] | SortKey[]): DataFrame<T>;

		// Joins
		innerJoin<U, L extends keyof T = keyof T, R extends keyof U = keyof U>(
			other: DataFrame<U>,
			leftOn: L,
			rightOn?: R,
			suffix?: string,
		): Promise<DataFrame<T & U>>;
		leftJoin<U, L extends keyof T = keyof T, R extends keyof U = keyof U>(
			other: DataFrame<U>,
			leftOn: L,
			rightOn?: R,
			suffix?: string,
		): Promise<DataFrame<T & Partial<U>>>;
		join<U, L extends keyof T = keyof T, R extends keyof U = keyof U>(
			other: DataFrame<U>,
			leftOn: L,
			rightOn?: R,
			how?: "inner" | "left" | "cross" | "anti" | "semi",
			suffix?: string,
		): Promise<DataFrame<T & (U | Partial<U>)>>;

		// Set Operations
		crossJoin<U>(
			other: DataFrame<U>,
			suffix?: string,
		): Promise<DataFrame<T & U>>;
		antiJoin<U>(
			other: DataFrame<U>,
			on: keyof T | (keyof T)[],
		): Promise<DataFrame<T>>;
		semiJoin<U>(
			other: DataFrame<U>,
			on: keyof T | (keyof T)[],
		): Promise<DataFrame<T>>;

		// Concatenation
		concat(other: DataFrame<T>): Promise<DataFrame<T>>;
		union(other: DataFrame<T>): Promise<DataFrame<T>>;

		// Execution
		collect(): Promise<DataFrame<T>>;
		toChunks(): Promise<import("../buffer/chunk.ts").Chunk[]>;
		count(): Promise<number>;
		toArray(): Promise<T[]>;
		isEmpty(): Promise<boolean>;
		show(maxRows?: number): Promise<void>;
		dtypes(): Record<string, DType>;

		// Inspection
		printSchema(): void;
		explain(): string;

		// Statistics
		describe(): Promise<
			Record<
				string,
				{ count: number; mean: number; std: number; min: number; max: number }
			>
		>;
	}
}
