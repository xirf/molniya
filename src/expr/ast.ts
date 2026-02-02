/**
 * Expression AST for the Molniya query language.
 *
 * Expressions are plain data objects (no class instances).
 * They are compiled to binary predicates at pipeline build time.
 */

import type { DTypeKind } from "../types/dtypes.ts";

/** Expression node types */
export enum ExprType {
	// Leaf nodes
	Column = "col",
	Literal = "lit",

	// Comparison operators
	Eq = "eq",
	Neq = "neq",
	Lt = "lt",
	Lte = "lte",
	Gt = "gt",
	Gte = "gte",
	Between = "between",
	IsNull = "is_null",
	IsNotNull = "is_not_null",

	// Logical operators
	And = "and",
	Or = "or",
	Not = "not",

	// Arithmetic operators
	Add = "add",
	Sub = "sub",
	Mul = "mul",
	Div = "div",
	Mod = "mod",
	Neg = "neg",

	// String operators
	Contains = "contains",
	StartsWith = "starts_with",
	EndsWith = "ends_with",

	// Aggregation (used in agg context)
	Sum = "sum",
	Avg = "avg",
	Min = "min",
	Max = "max",
	Count = "count",
	First = "first",
	Last = "last",

	// Alias (for renaming)
	Alias = "alias",

	// Type conversion
	Cast = "cast",

	// Null handling
	Coalesce = "coalesce",

	// Math functions
	Round = "round",
	Floor = "floor",
	Ceil = "ceil",
	Abs = "abs",
	Sqrt = "sqrt",
	Pow = "pow",

	// String operations
	StringLength = "str_length",
	Substring = "substring",
	Upper = "upper",
	Lower = "lower",
	Trim = "trim",
	Replace = "replace",

	// Date/time extraction
	Year = "year",
	Month = "month",
	Day = "day",
	DayOfWeek = "day_of_week",
	Quarter = "quarter",
	Hour = "hour",
	Minute = "minute",
	Second = "second",

	// Conditional
	When = "when",

	// New aggregations
	Std = "std",
	Var = "var",
	Median = "median",
	CountDistinct = "count_distinct",

	// Utility
	IsIn = "is_in",
}

/** Column reference */
export interface ColumnExpr {
	readonly type: ExprType.Column;
	readonly name: string;
}

/** Literal value */
export interface LiteralExpr {
	readonly type: ExprType.Literal;
	readonly value: number | bigint | string | boolean | null;
	readonly dtype?: DTypeKind; // Optional hint for type inference
}

/** Binary comparison */
export interface ComparisonExpr {
	readonly type:
		| ExprType.Eq
		| ExprType.Neq
		| ExprType.Lt
		| ExprType.Lte
		| ExprType.Gt
		| ExprType.Gte;
	readonly left: Expr;
	readonly right: Expr;
}

/** Between (inclusive range) */
export interface BetweenExpr {
	readonly type: ExprType.Between;
	readonly expr: Expr;
	readonly low: Expr;
	readonly high: Expr;
}

/** Null check */
export interface NullCheckExpr {
	readonly type: ExprType.IsNull | ExprType.IsNotNull;
	readonly expr: Expr;
}

/** Logical AND/OR */
export interface LogicalExpr {
	readonly type: ExprType.And | ExprType.Or;
	readonly exprs: readonly Expr[];
}

/** Logical NOT */
export interface NotExpr {
	readonly type: ExprType.Not;
	readonly expr: Expr;
}

/** Binary arithmetic */
export interface ArithmeticExpr {
	readonly type:
		| ExprType.Add
		| ExprType.Sub
		| ExprType.Mul
		| ExprType.Div
		| ExprType.Mod;
	readonly left: Expr;
	readonly right: Expr;
}

/** Unary negation */
export interface NegExpr {
	readonly type: ExprType.Neg;
	readonly expr: Expr;
}

/** String operations */
export interface StringOpExpr {
	readonly type: ExprType.Contains | ExprType.StartsWith | ExprType.EndsWith;
	readonly expr: Expr;
	readonly pattern: string;
}

/** Aggregation */
export interface AggExpr {
	readonly type:
		| ExprType.Sum
		| ExprType.Avg
		| ExprType.Min
		| ExprType.Max
		| ExprType.First
		| ExprType.Last
		| ExprType.Std
		| ExprType.Var
		| ExprType.Median;
	readonly expr: Expr;
}

/** Count distinct aggregation */
export interface CountDistinctExpr {
	readonly type: ExprType.CountDistinct;
	readonly expr: Expr;
}

/** Count aggregation (special case - can be count() or count(expr)) */
export interface CountExpr {
	readonly type: ExprType.Count;
	readonly expr: Expr | null; // null means count(*)
}

/** Alias expression */
export interface AliasExpr {
	readonly type: ExprType.Alias;
	readonly expr: Expr;
	readonly alias: string;
}

/** Cast expression for type conversion */
export interface CastExpr {
	readonly type: ExprType.Cast;
	readonly expr: Expr;
	readonly targetDType: DTypeKind;
}

/** Coalesce expression */
export interface CoalesceExpr {
	readonly type: ExprType.Coalesce;
	readonly exprs: readonly Expr[];
}

/** Round expression */
export interface RoundExpr {
	readonly type: ExprType.Round;
	readonly expr: Expr;
	readonly decimals: number;
}

/** Unary math expression (floor, ceil, abs, sqrt) */
export interface UnaryMathExpr {
	readonly type: ExprType.Floor | ExprType.Ceil | ExprType.Abs | ExprType.Sqrt;
	readonly expr: Expr;
}

/** Power expression */
export interface PowExpr {
	readonly type: ExprType.Pow;
	readonly expr: Expr;
	readonly exponent: number;
}

/** String length expression */
export interface StringLengthExpr {
	readonly type: ExprType.StringLength;
	readonly expr: Expr;
}

/** Substring expression */
export interface SubstringExpr {
	readonly type: ExprType.Substring;
	readonly expr: Expr;
	readonly start: number;
	readonly length?: number;
}

/** Case conversion expression (upper/lower) */
export interface CaseConversionExpr {
	readonly type: ExprType.Upper | ExprType.Lower;
	readonly expr: Expr;
}

/** Trim expression */
export interface TrimExpr {
	readonly type: ExprType.Trim;
	readonly expr: Expr;
}

/** Replace expression */
export interface ReplaceExpr {
	readonly type: ExprType.Replace;
	readonly expr: Expr;
	readonly pattern: string;
	readonly replacement: string;
	readonly all: boolean;
}

/** Date extraction expression */
export interface DateExtractExpr {
	readonly type:
		| ExprType.Year
		| ExprType.Month
		| ExprType.Day
		| ExprType.DayOfWeek
		| ExprType.Quarter
		| ExprType.Hour
		| ExprType.Minute
		| ExprType.Second;
	readonly expr: Expr;
}

/** When clause for conditional expression */
export interface WhenClause {
	readonly condition: Expr;
	readonly then: Expr;
}

/** When expression (conditional) */
export interface WhenExpr {
	readonly type: ExprType.When;
	readonly clauses: readonly WhenClause[];
	readonly otherwise: Expr;
}

/** IsIn expression */
export interface IsInExpr {
	readonly type: ExprType.IsIn;
	readonly expr: Expr;
	readonly values: readonly (number | bigint | string | boolean | null)[];
}

/** Union of all expression types */
export type Expr =
	| ColumnExpr
	| LiteralExpr
	| ComparisonExpr
	| BetweenExpr
	| NullCheckExpr
	| LogicalExpr
	| NotExpr
	| ArithmeticExpr
	| NegExpr
	| StringOpExpr
	| AggExpr
	| CountExpr
	| CountDistinctExpr
	| AliasExpr
	| CastExpr
	| CoalesceExpr
	| RoundExpr
	| UnaryMathExpr
	| PowExpr
	| StringLengthExpr
	| SubstringExpr
	| CaseConversionExpr
	| TrimExpr
	| ReplaceExpr
	| DateExtractExpr
	| WhenExpr
	| IsInExpr;

/** Check if expression is a column reference */
export function isColumnExpr(expr: Expr): expr is ColumnExpr {
	return expr.type === ExprType.Column;
}

/** Check if expression is a literal */
export function isLiteralExpr(expr: Expr): expr is LiteralExpr {
	return expr.type === ExprType.Literal;
}

/** Check if expression is a comparison */
export function isComparisonExpr(expr: Expr): expr is ComparisonExpr {
	return (
		expr.type === ExprType.Eq ||
		expr.type === ExprType.Neq ||
		expr.type === ExprType.Lt ||
		expr.type === ExprType.Lte ||
		expr.type === ExprType.Gt ||
		expr.type === ExprType.Gte
	);
}

/** Check if expression is arithmetic */
export function isArithmeticExpr(expr: Expr): expr is ArithmeticExpr {
	return (
		expr.type === ExprType.Add ||
		expr.type === ExprType.Sub ||
		expr.type === ExprType.Mul ||
		expr.type === ExprType.Div ||
		expr.type === ExprType.Mod
	);
}

/** Check if expression is an aggregation */
export function isAggExpr(
	expr: Expr,
): expr is AggExpr | CountExpr | CountDistinctExpr {
	return (
		expr.type === ExprType.Sum ||
		expr.type === ExprType.Avg ||
		expr.type === ExprType.Min ||
		expr.type === ExprType.Max ||
		expr.type === ExprType.First ||
		expr.type === ExprType.Last ||
		expr.type === ExprType.Count ||
		expr.type === ExprType.Std ||
		expr.type === ExprType.Var ||
		expr.type === ExprType.Median ||
		expr.type === ExprType.CountDistinct
	);
}

/** Check if expression is a cast */
export function isCastExpr(expr: Expr): expr is CastExpr {
	return expr.type === ExprType.Cast;
}

/** Format expression as string (for debugging) */
export function formatExpr(expr: Expr): string {
	switch (expr.type) {
		case ExprType.Column:
			return `col("${expr.name}")`;
		case ExprType.Literal:
			if (typeof expr.value === "string") {
				return `lit("${expr.value}")`;
			}
			return `lit(${expr.value})`;
		case ExprType.Eq:
		case ExprType.Neq:
		case ExprType.Lt:
		case ExprType.Lte:
		case ExprType.Gt:
		case ExprType.Gte:
			return `(${formatExpr(expr.left)} ${expr.type} ${formatExpr(expr.right)})`;
		case ExprType.Between:
			return `(${formatExpr(expr.expr)} between ${formatExpr(expr.low)} and ${formatExpr(expr.high)})`;
		case ExprType.IsNull:
			return `(${formatExpr(expr.expr)} is null)`;
		case ExprType.IsNotNull:
			return `(${formatExpr(expr.expr)} is not null)`;
		case ExprType.And:
			return `(${expr.exprs.map(formatExpr).join(" and ")})`;
		case ExprType.Or:
			return `(${expr.exprs.map(formatExpr).join(" or ")})`;
		case ExprType.Not:
			return `(not ${formatExpr(expr.expr)})`;
		case ExprType.Add:
		case ExprType.Sub:
		case ExprType.Mul:
		case ExprType.Div:
		case ExprType.Mod:
			return `(${formatExpr(expr.left)} ${expr.type} ${formatExpr(expr.right)})`;
		case ExprType.Neg:
			return `(-${formatExpr(expr.expr)})`;
		case ExprType.Contains:
		case ExprType.StartsWith:
		case ExprType.EndsWith:
			return `(${formatExpr(expr.expr)}.${expr.type}("${expr.pattern}"))`;
		case ExprType.Sum:
		case ExprType.Avg:
		case ExprType.Min:
		case ExprType.Max:
		case ExprType.First:
		case ExprType.Last:
			return `${expr.type}(${formatExpr(expr.expr)})`;
		case ExprType.Count:
			return expr.expr ? `count(${formatExpr(expr.expr)})` : "count(*)";
		case ExprType.Alias:
			return `${formatExpr(expr.expr)}.alias("${expr.alias}")`;
		case ExprType.Cast:
			return `${formatExpr(expr.expr)}.cast(${expr.targetDType})`;
		case ExprType.Coalesce:
			return `coalesce(${expr.exprs.map(formatExpr).join(", ")})`;
		case ExprType.Round:
			return `${formatExpr(expr.expr)}.round(${expr.decimals})`;
		case ExprType.Floor:
			return `${formatExpr(expr.expr)}.floor()`;
		case ExprType.Ceil:
			return `${formatExpr(expr.expr)}.ceil()`;
		case ExprType.Abs:
			return `${formatExpr(expr.expr)}.abs()`;
		case ExprType.Sqrt:
			return `${formatExpr(expr.expr)}.sqrt()`;
		case ExprType.Pow:
			return `${formatExpr(expr.expr)}.pow(${expr.exponent})`;
		case ExprType.StringLength:
			return `${formatExpr(expr.expr)}.length()`;
		case ExprType.Substring:
			return expr.length !== undefined
				? `${formatExpr(expr.expr)}.substring(${expr.start}, ${expr.length})`
				: `${formatExpr(expr.expr)}.substring(${expr.start})`;
		case ExprType.Upper:
			return `${formatExpr(expr.expr)}.upper()`;
		case ExprType.Lower:
			return `${formatExpr(expr.expr)}.lower()`;
		case ExprType.Trim:
			return `${formatExpr(expr.expr)}.trim()`;
		case ExprType.Replace:
			return `${formatExpr(expr.expr)}.replace("${expr.pattern}", "${expr.replacement}")`;
		case ExprType.Year:
		case ExprType.Month:
		case ExprType.Day:
		case ExprType.DayOfWeek:
		case ExprType.Quarter:
		case ExprType.Hour:
		case ExprType.Minute:
		case ExprType.Second:
			return `${formatExpr(expr.expr)}.${expr.type}()`;
		case ExprType.When:
			return `when(${expr.clauses.map((c) => `${formatExpr(c.condition)} => ${formatExpr(c.then)}`).join(", ")}).otherwise(${formatExpr(expr.otherwise)})`;
		case ExprType.Std:
			return `std(${formatExpr(expr.expr)})`;
		case ExprType.Var:
			return `var(${formatExpr(expr.expr)})`;
		case ExprType.Median:
			return `median(${formatExpr(expr.expr)})`;
		case ExprType.CountDistinct:
			return `countDistinct(${formatExpr(expr.expr)})`;
		case ExprType.IsIn:
			return `${formatExpr(expr.expr)}.isIn([${expr.values.map((v) => (typeof v === "string" ? `"${v}"` : v)).join(", ")}])`;
	}
}
