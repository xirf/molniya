/**
 * Expression type inference.
 *
 * Infers the result DType of an expression given a schema.
 * Used by the compiler to determine output types and validate operations.
 */

import {
	type DType,
	DType as DTypeFactory,
	DTypeKind,
	isNumericDType,
} from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import { getColumn, type Schema } from "../types/schema.ts";
import { type Expr, ExprType } from "./ast.ts";

/** Result of type inference */
export interface InferredType {
	readonly dtype: DType;
	readonly isAggregate: boolean;
}

/**
 * Infer the result type of an expression.
 */
export function inferExprType(
	expr: Expr,
	schema: Schema,
): Result<InferredType> {
	switch (expr.type) {
		case ExprType.Column: {
			const colResult = getColumn(schema, expr.name);
			if (colResult.error !== ErrorCode.None) {
				return err(ErrorCode.ColumnNotFound);
			}
			return ok({ dtype: colResult.value.dtype, isAggregate: false });
		}

		case ExprType.Literal: {
			const dtype = inferLiteralType(expr.value, expr.dtype);
			return ok({ dtype, isAggregate: false });
		}

		// Comparison operators return boolean, but need to validate operands
		case ExprType.Eq:
		case ExprType.Neq:
		case ExprType.Lt:
		case ExprType.Lte:
		case ExprType.Gt:
		case ExprType.Gte: {
			const leftResult = inferExprType(expr.left, schema);
			if (leftResult.error !== ErrorCode.None) return leftResult;
			const rightResult = inferExprType(expr.right, schema);
			if (rightResult.error !== ErrorCode.None) return rightResult;
			return ok({ dtype: DTypeFactory.boolean, isAggregate: false });
		}

		case ExprType.Between: {
			const exprResult = inferExprType(expr.expr, schema);
			if (exprResult.error !== ErrorCode.None) return exprResult;
			const lowResult = inferExprType(expr.low, schema);
			if (lowResult.error !== ErrorCode.None) return lowResult;
			const highResult = inferExprType(expr.high, schema);
			if (highResult.error !== ErrorCode.None) return highResult;
			return ok({ dtype: DTypeFactory.boolean, isAggregate: false });
		}

		case ExprType.IsNull:
		case ExprType.IsNotNull: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			return ok({ dtype: DTypeFactory.boolean, isAggregate: false });
		}

		case ExprType.Contains:
		case ExprType.StartsWith:
		case ExprType.EndsWith: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			return ok({ dtype: DTypeFactory.boolean, isAggregate: false });
		}

		// Logical operators return boolean
		case ExprType.And:
		case ExprType.Or:
		case ExprType.Not:
			return ok({ dtype: DTypeFactory.boolean, isAggregate: false });

		// Arithmetic operators - infer from operands
		case ExprType.Add:
		case ExprType.Sub:
		case ExprType.Mul:
		case ExprType.Div:
		case ExprType.Mod: {
			const leftResult = inferExprType(expr.left, schema);
			const rightResult = inferExprType(expr.right, schema);
			if (leftResult.error !== ErrorCode.None) return leftResult;
			if (rightResult.error !== ErrorCode.None) return rightResult;

			const resultDtype = promoteNumericTypes(
				leftResult.value.dtype,
				rightResult.value.dtype,
			);
			if (resultDtype === null) {
				return err(ErrorCode.TypeIncompatible);
			}
			return ok({ dtype: resultDtype, isAggregate: false });
		}

		case ExprType.Neg: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			if (!isNumericDType(innerResult.value.dtype)) {
				return err(ErrorCode.TypeIncompatible);
			}
			return ok(innerResult.value);
		}

		// Aggregations
		case ExprType.Sum:
		case ExprType.Avg: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			// Sum/Avg always return float64 for precision
			return ok({ dtype: DTypeFactory.float64, isAggregate: true });
		}

		case ExprType.Min:
		case ExprType.Max:
		case ExprType.First:
		case ExprType.Last: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			return ok({ dtype: innerResult.value.dtype, isAggregate: true });
		}

		case ExprType.Count:
			// Count returns int64
			return ok({ dtype: DTypeFactory.int64, isAggregate: true });

		case ExprType.Alias: {
			// Alias doesn't change type
			return inferExprType(expr.expr, schema);
		}

		// Math functions - all return float64
		case ExprType.Round:
		case ExprType.Floor:
		case ExprType.Ceil:
		case ExprType.Abs:
		case ExprType.Sqrt:
		case ExprType.Pow: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			return ok({ dtype: DTypeFactory.float64, isAggregate: false });
		}

		// String length returns int32
		case ExprType.StringLength: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			return ok({ dtype: DTypeFactory.int32, isAggregate: false });
		}

		// String operations return string
		case ExprType.Substring:
		case ExprType.Upper:
		case ExprType.Lower:
		case ExprType.Trim:
		case ExprType.Replace: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			return ok({ dtype: DTypeFactory.string, isAggregate: false });
		}

		// Date extraction returns int32
		case ExprType.Year:
		case ExprType.Month:
		case ExprType.Day:
		case ExprType.DayOfWeek:
		case ExprType.Quarter:
		case ExprType.Hour:
		case ExprType.Minute:
		case ExprType.Second: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			return ok({ dtype: DTypeFactory.int32, isAggregate: false });
		}

		// When expression - type of first then clause
		case ExprType.When: {
			if (expr.clauses.length === 0) {
				return err(ErrorCode.InvalidExpression);
			}
			const firstThenResult = inferExprType(expr.clauses[0]!.then, schema);
			if (firstThenResult.error !== ErrorCode.None) return firstThenResult;
			return ok({ dtype: firstThenResult.value.dtype, isAggregate: false });
		}

		// IsIn returns boolean
		case ExprType.IsIn: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			return ok({ dtype: DTypeFactory.boolean, isAggregate: false });
		}

		// New aggregations
		case ExprType.Std:
		case ExprType.Var:
		case ExprType.Median: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			return ok({ dtype: DTypeFactory.float64, isAggregate: true });
		}

		case ExprType.CountDistinct: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			return ok({ dtype: DTypeFactory.int64, isAggregate: true });
		}

		// Coalesce - type of first non-null expression
		case ExprType.Coalesce: {
			if (expr.exprs.length === 0) {
				return err(ErrorCode.InvalidExpression);
			}
			const firstResult = inferExprType(expr.exprs[0]!, schema);
			if (firstResult.error !== ErrorCode.None) return firstResult;
			return ok({ dtype: firstResult.value.dtype, isAggregate: false });
		}

		// Cast expression
		case ExprType.Cast: {
			const innerResult = inferExprType(expr.expr, schema);
			if (innerResult.error !== ErrorCode.None) return innerResult;
			return ok({
				dtype: { kind: expr.targetDType, nullable: innerResult.value.dtype.nullable },
				isAggregate: false,
			});
		}

		default:
			return err(ErrorCode.InvalidExpression);
	}
}

/**
 * Infer type from a literal value.
 */
function inferLiteralType(value: unknown, hint?: DTypeKind): DType {
	if (hint !== undefined) {
		return { kind: hint, nullable: value === null };
	}

	if (value === null) {
		// Default null to nullable int32
		return DTypeFactory.nullable.int32;
	}

	switch (typeof value) {
		case "number":
			// Check if integer or float
			if (
				Number.isInteger(value) &&
				value >= -2147483648 &&
				value <= 2147483647
			) {
				return DTypeFactory.int32;
			}
			return DTypeFactory.float64;

		case "bigint":
			return DTypeFactory.int64;

		case "string":
			return DTypeFactory.string;

		case "boolean":
			return DTypeFactory.boolean;

		default:
			return DTypeFactory.int32;
	}
}

/**
 * Promote two numeric types to a common type.
 * Returns null if types are incompatible.
 */
function promoteNumericTypes(left: DType, right: DType): DType | null {
	if (!isNumericDType(left) || !isNumericDType(right)) {
		return null;
	}

	const leftKind = left.kind;
	const rightKind = right.kind;

	// Float64 is the widest, always wins
	if (leftKind === DTypeKind.Float64 || rightKind === DTypeKind.Float64) {
		return DTypeFactory.float64;
	}

	// Float32 next
	if (leftKind === DTypeKind.Float32 || rightKind === DTypeKind.Float32) {
		return DTypeFactory.float32;
	}

	// BigInt types
	if (leftKind === DTypeKind.Int64 || rightKind === DTypeKind.Int64) {
		return DTypeFactory.int64;
	}
	if (leftKind === DTypeKind.UInt64 || rightKind === DTypeKind.UInt64) {
		return DTypeFactory.uint64;
	}

	// 32-bit integers
	if (leftKind === DTypeKind.Int32 || rightKind === DTypeKind.Int32) {
		return DTypeFactory.int32;
	}
	if (leftKind === DTypeKind.UInt32 || rightKind === DTypeKind.UInt32) {
		return DTypeFactory.uint32;
	}

	// 16-bit integers
	if (leftKind === DTypeKind.Int16 || rightKind === DTypeKind.Int16) {
		return DTypeFactory.int16;
	}
	if (leftKind === DTypeKind.UInt16 || rightKind === DTypeKind.UInt16) {
		return DTypeFactory.uint16;
	}

	// 8-bit integers - promote to int16 to avoid overflow
	return DTypeFactory.int16;
}

/**
 * Check if an expression is a predicate (returns boolean).
 */
export function isPredicateExpr(expr: Expr): boolean {
	switch (expr.type) {
		case ExprType.Eq:
		case ExprType.Neq:
		case ExprType.Lt:
		case ExprType.Lte:
		case ExprType.Gt:
		case ExprType.Gte:
		case ExprType.Between:
		case ExprType.IsNull:
		case ExprType.IsNotNull:
		case ExprType.And:
		case ExprType.Or:
		case ExprType.Not:
		case ExprType.Contains:
		case ExprType.StartsWith:
		case ExprType.EndsWith:
		case ExprType.IsIn:
			return true;
		default:
			return false;
	}
}

/**
 * Validate that an expression is valid for the given schema.
 */
export function validateExpr(expr: Expr, schema: Schema): Result<void> {
	const typeResult = inferExprType(expr, schema);
	if (typeResult.error !== ErrorCode.None) {
		return err(typeResult.error);
	}
	return ok(undefined);
}
