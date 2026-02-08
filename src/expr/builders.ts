/**
 * Expression builders for fluent DSL.
 *
 * These functions create expression AST nodes with a chainable API.
 * The ColumnRef class provides method chaining for column operations.
 */

import type { DTypeKind } from "../types/dtypes.ts";
import {
	type AggExpr,
	type AliasExpr,
	type ArithmeticExpr,
	type BetweenExpr,
	type CaseConversionExpr,
	type CastExpr,
	type CoalesceExpr,
	type ColumnExpr,
	type ComparisonExpr,
	type CountDistinctExpr,
	type CountExpr,
		type DateAddExpr,
		type DateParseExpr,
	type DateExtractExpr,
	type Expr,
	ExprType,
		type DiffDaysExpr,
		type FormatDateExpr,
	type IsInExpr,
	type LiteralExpr,
	type LogicalExpr,
	type NegExpr,
	type NotExpr,
	type NullCheckExpr,
		type ParseJsonExpr,
		type TruncateDateExpr,
	type PowExpr,
	type ReplaceExpr,
	type RoundExpr,
	type StringLengthExpr,
	type StringOpExpr,
	type SubstringExpr,
	type TrimExpr,
	type UnaryMathExpr,
	type WhenClause,
	type WhenExpr,
} from "./ast.ts";

/**
 * Wrapper class providing fluent API for column expressions.
 * Methods return Expr objects, not new ColumnRef instances.
 */
export class ColumnRef {
	private readonly expr: ColumnExpr;

	constructor(name: string) {
		this.expr = { type: ExprType.Column, name };
	}

	/** Get the underlying expression */
	toExpr(): ColumnExpr {
		return this.expr;
	}

	// Comparison operators
	eq(other: Expr | number | string | boolean): ComparisonExpr {
		return { type: ExprType.Eq, left: this.expr, right: toExpr(other) };
	}

	neq(other: Expr | number | string | boolean): ComparisonExpr {
		return { type: ExprType.Neq, left: this.expr, right: toExpr(other) };
	}

	lt(other: Expr | number): ComparisonExpr {
		return { type: ExprType.Lt, left: this.expr, right: toExpr(other) };
	}

	lte(other: Expr | number): ComparisonExpr {
		return { type: ExprType.Lte, left: this.expr, right: toExpr(other) };
	}

	gt(other: Expr | number): ComparisonExpr {
		return { type: ExprType.Gt, left: this.expr, right: toExpr(other) };
	}

	gte(other: Expr | number): ComparisonExpr {
		return { type: ExprType.Gte, left: this.expr, right: toExpr(other) };
	}

	between(low: Expr | number, high: Expr | number): BetweenExpr {
		return {
			type: ExprType.Between,
			expr: this.expr,
			low: toExpr(low),
			high: toExpr(high),
		};
	}

	isNull(): NullCheckExpr {
		return { type: ExprType.IsNull, expr: this.expr };
	}

	isNotNull(): NullCheckExpr {
		return { type: ExprType.IsNotNull, expr: this.expr };
	}

	// Arithmetic operators
	add(other: Expr | number): ArithmeticExpr {
		return { type: ExprType.Add, left: this.expr, right: toExpr(other) };
	}

	sub(other: Expr | number): ArithmeticExpr {
		return { type: ExprType.Sub, left: this.expr, right: toExpr(other) };
	}

	mul(other: Expr | number): ArithmeticExpr {
		return { type: ExprType.Mul, left: this.expr, right: toExpr(other) };
	}

	div(other: Expr | number): ArithmeticExpr {
		return { type: ExprType.Div, left: this.expr, right: toExpr(other) };
	}

	mod(other: Expr | number): ArithmeticExpr {
		return { type: ExprType.Mod, left: this.expr, right: toExpr(other) };
	}

	neg(): NegExpr {
		return { type: ExprType.Neg, expr: this.expr };
	}

	// String operations
	contains(pattern: string): StringOpExpr {
		return { type: ExprType.Contains, expr: this.expr, pattern };
	}

	startsWith(pattern: string): StringOpExpr {
		return { type: ExprType.StartsWith, expr: this.expr, pattern };
	}

	endsWith(pattern: string): StringOpExpr {
		return { type: ExprType.EndsWith, expr: this.expr, pattern };
	}

	// Alias
	alias(name: string): AliasExpr {
		return { type: ExprType.Alias, expr: this.expr, alias: name };
	}

	// Type casting
	cast(targetDType: DTypeKind): CastExpr {
		return { type: ExprType.Cast, expr: this.expr, targetDType };
	}

	// Math functions
	round(decimals: number = 0): RoundExpr {
		return { type: ExprType.Round, expr: this.expr, decimals };
	}

	floor(): UnaryMathExpr {
		return { type: ExprType.Floor, expr: this.expr };
	}

	ceil(): UnaryMathExpr {
		return { type: ExprType.Ceil, expr: this.expr };
	}

	abs(): UnaryMathExpr {
		return { type: ExprType.Abs, expr: this.expr };
	}

	sqrt(): UnaryMathExpr {
		return { type: ExprType.Sqrt, expr: this.expr };
	}

	pow(exponent: number): PowExpr {
		return { type: ExprType.Pow, expr: this.expr, exponent };
	}

	// String operations
	length(): StringLengthExpr {
		return { type: ExprType.StringLength, expr: this.expr };
	}

	substring(start: number, length?: number): SubstringExpr {
		return { type: ExprType.Substring, expr: this.expr, start, length };
	}

	upper(): CaseConversionExpr {
		return { type: ExprType.Upper, expr: this.expr };
	}

	lower(): CaseConversionExpr {
		return { type: ExprType.Lower, expr: this.expr };
	}

	trim(): TrimExpr {
		return { type: ExprType.Trim, expr: this.expr };
	}

	replace(pattern: string, replacement: string, all: boolean = true): ReplaceExpr {
		return { type: ExprType.Replace, expr: this.expr, pattern, replacement, all };
	}

	// Date/time extraction
	year(): DateExtractExpr {
		return { type: ExprType.Year, expr: this.expr };
	}

	month(): DateExtractExpr {
		return { type: ExprType.Month, expr: this.expr };
	}

	day(): DateExtractExpr {
		return { type: ExprType.Day, expr: this.expr };
	}

	dayOfWeek(): DateExtractExpr {
		return { type: ExprType.DayOfWeek, expr: this.expr };
	}

	quarter(): DateExtractExpr {
		return { type: ExprType.Quarter, expr: this.expr };
	}

	hour(): DateExtractExpr {
		return { type: ExprType.Hour, expr: this.expr };
	}

	minute(): DateExtractExpr {
		return { type: ExprType.Minute, expr: this.expr };
	}

	second(): DateExtractExpr {
		return { type: ExprType.Second, expr: this.expr };
	}

	// Date arithmetic
	addDays(days: number): DateAddExpr {
		return { type: ExprType.AddDays, expr: this.expr, days };
	}

	subDays(days: number): DateAddExpr {
		return { type: ExprType.SubDays, expr: this.expr, days };
	}

	diffDays(other: Expr | ColumnRef): DiffDaysExpr {
		const rhs = other instanceof ColumnRef ? other.toExpr() : other;
		return { type: ExprType.DiffDays, left: this.expr, right: rhs };
	}

	truncateDate(period: "day" | "week" | "month" | "quarter" | "year"): TruncateDateExpr {
		return { type: ExprType.TruncateDate, expr: this.expr, period };
	}

	// Utility
	isIn(values: (number | bigint | string | boolean | null)[]): IsInExpr {
		return { type: ExprType.IsIn, expr: this.expr, values };
	}
}

/**
 * Create a column reference expression.
 *
 * Usage:
 *   col("name")              // Returns ColumnRef with fluent methods
 *   col("age").gt(18)        // Returns ComparisonExpr
 *   col("price").mul(1.1)    // Returns ArithmeticExpr
 */
export function col(name: string): ColumnRef {
	return new ColumnRef(name);
}

/**
 * Create a literal expression.
 */
export function lit(
	value: number | bigint | string | boolean | null,
): LiteralExpr {
	return { type: ExprType.Literal, value };
}

/**
 * Create a typed literal (with explicit dtype hint).
 */
export function typedLit(
	value: number | bigint | string | boolean | null,
	dtype: DTypeKind,
): LiteralExpr {
	return { type: ExprType.Literal, value, dtype };
}

/**
 * Convert a value or ColumnRef to an Expr.
 */
function toExpr(
	value: Expr | ColumnRef | number | string | boolean | bigint | null,
): Expr {
	if (value === null) {
		return lit(null);
	}
	if (
		typeof value === "number" ||
		typeof value === "string" ||
		typeof value === "boolean" ||
		typeof value === "bigint"
	) {
		return lit(value);
	}
	if (value instanceof ColumnRef) {
		return value.toExpr();
	}
	return value;
}

// Logical combinators

/**
 * Logical AND of multiple expressions.
 */
export function and(...exprs: (Expr | ColumnRef)[]): LogicalExpr {
	return {
		type: ExprType.And,
		exprs: exprs.map((e) => (e instanceof ColumnRef ? e.toExpr() : e)),
	};
}

/**
 * Logical OR of multiple expressions.
 */
export function or(...exprs: (Expr | ColumnRef)[]): LogicalExpr {
	return {
		type: ExprType.Or,
		exprs: exprs.map((e) => (e instanceof ColumnRef ? e.toExpr() : e)),
	};
}

/**
 * Logical NOT of an expression.
 */
export function not(expr: Expr | ColumnRef): NotExpr {
	return {
		type: ExprType.Not,
		expr: expr instanceof ColumnRef ? expr.toExpr() : expr,
	};
}

/**
 * Check if value is between low and high (inclusive).
 */
export function between(
	expr: Expr | ColumnRef | string,
	low: Expr | ColumnRef | number,
	high: Expr | ColumnRef | number,
): BetweenExpr {
	const exprVal =
		typeof expr === "string"
			? col(expr).toExpr()
			: expr instanceof ColumnRef
				? expr.toExpr()
				: expr;
	return {
		type: ExprType.Between,
		expr: exprVal,
		low: toExpr(low),
		high: toExpr(high),
	};
}

// Aggregation functions

/**
 * Sum aggregation.
 */
export function sum(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Sum, expr };
}

/**
 * Average aggregation.
 */
export function avg(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Avg, expr };
}

/**
 * Minimum aggregation.
 */
export function min(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Min, expr };
}

/**
 * Maximum aggregation.
 */
export function max(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Max, expr };
}

/**
 * First value aggregation.
 */
export function first(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.First, expr };
}

/**
 * Last value aggregation.
 */
export function last(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Last, expr };
}

/**
 * Count aggregation.
 * count() - count all rows
 * count(column) - count non-null values in column
 */
export function count(column?: string | Expr | ColumnRef): CountExpr {
	if (column === undefined) {
		return { type: ExprType.Count, expr: null };
	}
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Count, expr };
}

// Arithmetic on expressions (not just columns)

/**
 * Add two expressions.
 */
export function add(
	left: Expr | ColumnRef | number,
	right: Expr | ColumnRef | number,
): ArithmeticExpr {
	return { type: ExprType.Add, left: toExpr(left), right: toExpr(right) };
}

/**
 * Subtract two expressions.
 */
export function sub(
	left: Expr | ColumnRef | number,
	right: Expr | ColumnRef | number,
): ArithmeticExpr {
	return { type: ExprType.Sub, left: toExpr(left), right: toExpr(right) };
}

/**
 * Multiply two expressions.
 */
export function mul(
	left: Expr | ColumnRef | number,
	right: Expr | ColumnRef | number,
): ArithmeticExpr {
	return { type: ExprType.Mul, left: toExpr(left), right: toExpr(right) };
}

/**
 * Divide two expressions.
 */
export function div(
	left: Expr | ColumnRef | number,
	right: Expr | ColumnRef | number,
): ArithmeticExpr {
	return { type: ExprType.Div, left: toExpr(left), right: toExpr(right) };
}

/**
 * Modulo two expressions.
 */
export function mod(
	left: Expr | ColumnRef | number,
	right: Expr | ColumnRef | number,
): ArithmeticExpr {
	return { type: ExprType.Mod, left: toExpr(left), right: toExpr(right) };
}

/**
 * Negate an expression.
 */
export function neg(expr: Expr | ColumnRef | number): NegExpr {
	return { type: ExprType.Neg, expr: toExpr(expr) };
}

// Null handling

/**
 * Coalesce multiple expressions (returns first non-null).
 */
export function coalesce(
	...exprs: (Expr | ColumnRef | number | string | boolean | null)[]
): CoalesceExpr {
	return {
		type: ExprType.Coalesce,
		exprs: exprs.map(toExpr),
	};
}

// Conditional expressions

/**
 * Builder for conditional when/otherwise expressions.
 */
export class WhenBuilder {
	private clauses: WhenClause[] = [];

	constructor(condition: Expr, thenValue: Expr) {
		this.clauses.push({ condition, then: thenValue });
	}

	/**
	 * Add another condition/then clause.
	 */
	when(
		condition: Expr | ColumnRef,
		thenValue: Expr | ColumnRef | number | string | boolean | null,
	): WhenBuilder {
		const cond = condition instanceof ColumnRef ? condition.toExpr() : condition;
		this.clauses.push({ condition: cond, then: toExpr(thenValue) });
		return this;
	}

	/**
	 * Set the default value when no conditions match.
	 */
	otherwise(value: Expr | ColumnRef | number | string | boolean | null): WhenExpr {
		return {
			type: ExprType.When,
			clauses: this.clauses,
			otherwise: toExpr(value),
		};
	}
}

/**
 * Start a conditional expression.
 * Usage: when(condition, thenValue).when(condition2, thenValue2).otherwise(defaultValue)
 */
export function when(
	condition: Expr | ColumnRef,
	thenValue: Expr | ColumnRef | number | string | boolean | null,
): WhenBuilder {
	const cond = condition instanceof ColumnRef ? condition.toExpr() : condition;
	return new WhenBuilder(cond, toExpr(thenValue));
}

// Additional aggregation functions

/**
 * Standard deviation aggregation (sample).
 */
export function std(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Std, expr };
}

/**
 * Variance aggregation (sample).
 */
export function variance(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Var, expr };
}

/**
 * Median aggregation.
 */
export function median(column: string | Expr | ColumnRef): AggExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.Median, expr };
}

/**
 * Count distinct values aggregation.
 */
export function countDistinct(column: string | Expr | ColumnRef): CountDistinctExpr {
	const expr =
		typeof column === "string"
			? col(column).toExpr()
			: column instanceof ColumnRef
				? column.toExpr()
				: column;
	return { type: ExprType.CountDistinct, expr };
}

/**
 * Add days to a date expression.
 */
export function addDays(
	expr: Expr | ColumnRef,
	days: number,
): DateAddExpr {
	const e = expr instanceof ColumnRef ? expr.toExpr() : expr;
	return { type: ExprType.AddDays, expr: e, days };
}

/**
 * Subtract days from a date expression.
 */
export function subDays(
	expr: Expr | ColumnRef,
	days: number,
): DateAddExpr {
	const e = expr instanceof ColumnRef ? expr.toExpr() : expr;
	return { type: ExprType.SubDays, expr: e, days };
}

/**
 * Difference in days between two date expressions.
 */
export function diffDays(
	left: Expr | ColumnRef,
	right: Expr | ColumnRef,
): DiffDaysExpr {
	const l = left instanceof ColumnRef ? left.toExpr() : left;
	const r = right instanceof ColumnRef ? right.toExpr() : right;
	return { type: ExprType.DiffDays, left: l, right: r };
}

/**
 * Truncate date to a period start.
 */
export function truncateDate(
	expr: Expr | ColumnRef,
	period: "day" | "week" | "month" | "quarter" | "year",
): TruncateDateExpr {
	const e = expr instanceof ColumnRef ? expr.toExpr() : expr;
	return { type: ExprType.TruncateDate, expr: e, period };
}

		/**
		 * Parse string to Date (days since epoch).
		 */
		export function toDate(expr: Expr | ColumnRef, format?: string): DateParseExpr {
			const e = expr instanceof ColumnRef ? expr.toExpr() : expr;
			return { type: ExprType.ToDate, expr: e, format };
		}

		/**
		 * Parse string to Timestamp (milliseconds since epoch as bigint).
		 */
		export function toTimestamp(
			expr: Expr | ColumnRef,
			format?: string,
		): DateParseExpr {
			const e = expr instanceof ColumnRef ? expr.toExpr() : expr;
			return { type: ExprType.ToTimestamp, expr: e, format };
		}

		/**
		 * Format Date/Timestamp into string.
		 */
		export function formatDate(
			expr: Expr | ColumnRef,
			format: string,
		): FormatDateExpr {
			const e = expr instanceof ColumnRef ? expr.toExpr() : expr;
			return { type: ExprType.FormatDate, expr: e, format };
		}

		/**
		 * Parse JSON string and return normalized JSON string.
		 */
		export function parseJson(expr: Expr | ColumnRef): ParseJsonExpr {
			const e = expr instanceof ColumnRef ? expr.toExpr() : expr;
			return { type: ExprType.ParseJson, expr: e };
		}
