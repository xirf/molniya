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
		| ExprType.Last;
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
	| AliasExpr
	| CastExpr
	| CoalesceExpr;

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
export function isAggExpr(expr: Expr): expr is AggExpr | CountExpr {
	return (
		expr.type === ExprType.Sum ||
		expr.type === ExprType.Avg ||
		expr.type === ExprType.Min ||
		expr.type === ExprType.Max ||
		expr.type === ExprType.First ||
		expr.type === ExprType.Last ||
		expr.type === ExprType.Count
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
	}
}
