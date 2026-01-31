/* AGGREGATION METHODS
/*-----------------------------------------------------
/* Group and aggregate column values
/* ==================================================== */

import type { AggExpr, CountExpr, Expr } from "../expr/ast.ts";
import { avg, col, count, max, min, sum } from "../expr/builders.ts";
import { type AggSpec, aggregate, groupBy } from "../ops/index.ts";
import { ErrorCode } from "../types/error.ts";
import type { DataFrame } from "./core.ts";

// Type-safe aggregation spec builders with output type tracking
type AggOutputType = "number" | "bigint" | "string" | "boolean" | "unknown";

/** Type-safe aggregation specification */
export interface TypedAggSpec<
	N extends string = string,
	T extends AggOutputType = AggOutputType,
> {
	/** Output column name */
	name: N;
	/** Aggregation expression */
	expr: Expr;
	/** Type hint for the output (for type inference) */
	_outputType?: T;
}

/** Helper to create a typed aggregation spec */
export function agg<N extends string>(name: N, expr: Expr): TypedAggSpec<N> {
	return { name, expr };
}

/** Type mapping for aggregation results based on aggregation type */
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

/** Build groupby result type: preserves key columns + aggregation results */
type BuildGroupByResult<
	T,
	K extends readonly string[],
	Specs extends readonly TypedAggSpec[],
> = Pick<T, K[number] & keyof T> & BuildAggResult<Specs>;

export class RelationalGroupedDataset<T> {
	constructor(
		private df: DataFrame<T>,
		private keys: string[],
	) {}

	agg<const Specs extends readonly TypedAggSpec[]>(
		specs: Specs,
	): DataFrame<BuildGroupByResult<T, typeof this.keys, Specs>> {
		const result = groupBy(
			this.df.currentSchema(),
			this.keys,
			specs as unknown as AggSpec[],
		);
		if (result.error !== ErrorCode.None) {
			throw new Error(`GroupBy error: ${result.error}`);
		}
		return this.df.withOperator(result.value) as DataFrame<
			BuildGroupByResult<T, typeof this.keys, Specs>
		>;
	}

	count(): DataFrame<
		BuildGroupByResult<
			T,
			typeof this.keys,
			[{ name: "count"; expr: CountExpr }]
		>
	> {
		return this.agg([{ name: "count", expr: count() }]);
	}

	min<K extends string>(
		column: K,
	): DataFrame<
		BuildGroupByResult<
			T,
			typeof this.keys,
			[{ name: `min(${K})`; expr: AggExpr }]
		>
	> {
		return this.agg([{ name: `min(${column})`, expr: min(col(column)) }]);
	}

	max<K extends string>(
		column: K,
	): DataFrame<
		BuildGroupByResult<
			T,
			typeof this.keys,
			[{ name: `max(${K})`; expr: AggExpr }]
		>
	> {
		return this.agg([{ name: `max(${column})`, expr: max(col(column)) }]);
	}

	mean<K extends string>(
		column: K,
	): DataFrame<
		BuildGroupByResult<
			T,
			typeof this.keys,
			[{ name: `mean(${K})`; expr: AggExpr }]
		>
	> {
		return this.agg([{ name: `mean(${column})`, expr: avg(col(column)) }]);
	}

	sum<K extends string>(
		column: K,
	): DataFrame<
		BuildGroupByResult<
			T,
			typeof this.keys,
			[{ name: `sum(${K})`; expr: AggExpr }]
		>
	> {
		return this.agg([{ name: `sum(${column})`, expr: sum(col(column)) }]);
	}
}

export function addAggMethods(df: typeof DataFrame.prototype) {
	df.agg = function <const Specs extends readonly TypedAggSpec[]>(
		specs: Specs,
	): DataFrame<BuildAggResult<Specs>> {
		const result = aggregate(
			this.currentSchema(),
			specs as unknown as AggSpec[],
		);
		if (result.error !== ErrorCode.None) {
			throw new Error(`Agg error: ${result.error}`);
		}
		return this.withOperator(result.value) as DataFrame<BuildAggResult<Specs>>;
	};

	df.groupBy = function (
		keyColumns: string | string[],
	): RelationalGroupedDataset<Record<string, unknown>> {
		const keys = Array.isArray(keyColumns) ? keyColumns : [keyColumns];
		return new RelationalGroupedDataset(this, keys);
	};

	// Method shortcuts on DataFrame
	df.min = async function (column: string): Promise<number | string | null> {
		const res = await this.agg([
			{ name: "res", expr: min(col(column)) },
		]).collect();
		if (
			Array.isArray(res.source) &&
			res.source.length > 0 &&
			res.source[0].rowCount > 0
		) {
			const chunk = res.source[0];
			return chunk.getValue(0, 0) as number | string | null;
		}
		return null;
	};

	df.max = async function (column: string): Promise<number | string | null> {
		const res = await this.agg([
			{ name: "res", expr: max(col(column)) },
		]).collect();
		if (
			Array.isArray(res.source) &&
			res.source.length > 0 &&
			res.source[0].rowCount > 0
		) {
			const chunk = res.source[0];
			return chunk.getValue(0, 0) as number | string | null;
		}
		return null;
	};

	df.mean = async function (column: string): Promise<number | null> {
		const res = await this.agg([
			{ name: "res", expr: avg(col(column)) },
		]).collect();
		if (
			Array.isArray(res.source) &&
			res.source.length > 0 &&
			res.source[0].rowCount > 0
		) {
			const chunk = res.source[0];
			return chunk.getValue(0, 0) as number | null;
		}
		return null;
	};
}
