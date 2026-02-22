/**
 * Predicate pushdown for CSV sources.
 *
 * Compiles simple filter expressions into functions suitable for
 * CsvParser.setFilter(). Only pushes down predicates that can be
 * evaluated on raw parsed column values (comparisons, null checks,
 * string ops, logical AND/OR/NOT).
 *
 * Complex predicates (arithmetic, aggregations, casts) are NOT pushed down
 * and remain as regular FilterOperators in the pipeline.
 */

import { type Expr, ExprType } from "../expr/ast.ts";
import { getColumnIndex, type Schema } from "../types/schema.ts";

/** Type of the getValue callback passed to pushdown filters */
export type GetColumnValue = (schemaIdx: number) => unknown;

/** A compiled pushdown predicate */
export type PushdownPredicate = (getValue: GetColumnValue) => boolean;

/**
 * Attempt to compile an expression into a pushdown predicate.
 * Returns null if the expression cannot be pushed down.
 *
 * Supported expressions:
 * - Column comparisons: col == lit, col != lit, col < lit, etc.
 * - Null checks: isNull(col), isNotNull(col)
 * - String ops: contains(col, str), startsWith(col, str), endsWith(col, str)
 * - Logical: AND, OR, NOT of pushdown-safe sub-expressions
 * - Between: col BETWEEN low AND high
 */
export function compilePushdownFilter(
	expr: Expr,
	schema: Schema,
): PushdownPredicate | null {
	switch (expr.type) {
		case ExprType.Eq:
		case ExprType.Neq:
		case ExprType.Lt:
		case ExprType.Lte:
		case ExprType.Gt:
		case ExprType.Gte: {
			// Column vs Literal comparison
			const { left, right } = expr;
			const colIdx = resolveColumnIndex(left, schema) ?? resolveColumnIndex(right, schema);
			const litValue = resolveLiteral(left) ?? resolveLiteral(right);

			if (colIdx === null || litValue === undefined) return null;

			const cmpType = expr.type;
			return (getValue) => {
				const val = getValue(colIdx);
				if (val === null || val === undefined) return false;
				switch (cmpType) {
					case ExprType.Eq: return val === litValue;
					case ExprType.Neq: return val !== litValue;
					case ExprType.Lt: return (val as number) < (litValue as number);
					case ExprType.Lte: return (val as number) <= (litValue as number);
					case ExprType.Gt: return (val as number) > (litValue as number);
					case ExprType.Gte: return (val as number) >= (litValue as number);
					default: return true;
				}
			};
		}

		case ExprType.IsNull: {
			const colIdx = resolveColumnIndex(expr.expr, schema);
			if (colIdx === null) return null;
			return (getValue) => getValue(colIdx) === null || getValue(colIdx) === undefined;
		}

		case ExprType.IsNotNull: {
			const colIdx = resolveColumnIndex(expr.expr, schema);
			if (colIdx === null) return null;
			return (getValue) => getValue(colIdx) !== null && getValue(colIdx) !== undefined;
		}

		case ExprType.Contains: {
			const colIdx = resolveColumnIndex(expr.expr, schema);
			if (colIdx === null) return null;
			const pattern = expr.pattern;
			return (getValue) => {
				const val = getValue(colIdx);
				if (typeof val !== "string") return false;
				return val.includes(pattern);
			};
		}

		case ExprType.StartsWith: {
			const colIdx = resolveColumnIndex(expr.expr, schema);
			if (colIdx === null) return null;
			const pattern = expr.pattern;
			return (getValue) => {
				const val = getValue(colIdx);
				if (typeof val !== "string") return false;
				return val.startsWith(pattern);
			};
		}

		case ExprType.EndsWith: {
			const colIdx = resolveColumnIndex(expr.expr, schema);
			if (colIdx === null) return null;
			const pattern = expr.pattern;
			return (getValue) => {
				const val = getValue(colIdx);
				if (typeof val !== "string") return false;
				return val.endsWith(pattern);
			};
		}

		case ExprType.Between: {
			const colIdx = resolveColumnIndex(expr.expr, schema);
			const low = resolveLiteral(expr.low);
			const high = resolveLiteral(expr.high);
			if (colIdx === null || low === undefined || high === undefined) return null;
			return (getValue) => {
				const val = getValue(colIdx) as number;
				if (val === null || val === undefined) return false;
				return val >= (low as number) && val <= (high as number);
			};
		}

		case ExprType.And: {
			const subs = expr.exprs.map((e) => compilePushdownFilter(e, schema));
			if (subs.some((s) => s === null)) return null;
			const filters = subs as PushdownPredicate[];
			return (getValue) => filters.every((f) => f(getValue));
		}

		case ExprType.Or: {
			const subs = expr.exprs.map((e) => compilePushdownFilter(e, schema));
			if (subs.some((s) => s === null)) return null;
			const filters = subs as PushdownPredicate[];
			return (getValue) => filters.some((f) => f(getValue));
		}

		case ExprType.Not: {
			const inner = compilePushdownFilter(expr.expr, schema);
			if (inner === null) return null;
			return (getValue) => !inner(getValue);
		}

		default:
			return null;
	}
}

function resolveColumnIndex(expr: Expr, schema: Schema): number | null {
	if (expr.type !== ExprType.Column) return null;
	const result = getColumnIndex(schema, expr.name);
	if (result.error !== 0) return null;
	return result.value;
}

function resolveLiteral(expr: Expr): unknown | undefined {
	if (expr.type !== ExprType.Literal) return undefined;
	return expr.value;
}
