/**
 * Expression compiler.
 *
 * Compiles Expr AST nodes into executable functions that operate
 * directly on Chunk columnar data without object allocation.
 *
 * Two types of compiled functions:
 * 1. Predicates: (chunk, rowIndex) => boolean
 * 2. Values: (chunk, rowIndex) => number | bigint | boolean
 */

/** biome-ignore-all lint/complexity/useOptionalChain: Performance optimization */
/** biome-ignore-all lint/style/noNonNullAssertion: Intentional */

import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import type { Schema } from "../types/schema.ts";
import {
	type ArithmeticExpr,
	type BetweenExpr,
	type CaseConversionExpr,
	type CastExpr,
	type CoalesceExpr,
	type ColumnExpr,
	type ComparisonExpr,
	type DateExtractExpr,
	type Expr,
	ExprType,
	type IsInExpr,
	type LiteralExpr,
	type LogicalExpr,
	type NotExpr,
	type NullCheckExpr,
	type PowExpr,
	type ReplaceExpr,
	type RoundExpr,
	type StringLengthExpr,
	type StringOpExpr,
	type SubstringExpr,
	type TrimExpr,
	type UnaryMathExpr,
	type WhenExpr,
} from "./ast.ts";
import type { CompiledPredicate, CompiledValue } from "./compile-types.ts";

// Re-export apply functions
export { applyPredicate, applyValue, countMatching } from "./apply.ts";
// Re-export types
export type { CompiledPredicate, CompiledValue } from "./compile-types.ts";

/**
 * Compilation context containing schema and column indices.
 */
interface CompileContext {
	schema: Schema;
	columnIndices: Map<string, number>;
}

/**
 * Create compilation context from schema.
 */
function createContext(schema: Schema): CompileContext {
	const columnIndices = new Map<string, number>();
	for (let i = 0; i < schema.columns.length; i++) {
		columnIndices.set(schema.columns[i]!.name, i);
	}
	return { schema, columnIndices };
}

/**
 * Compile an expression to a predicate function.
 * The expression must evaluate to a boolean.
 */
export function compilePredicate(
	expr: Expr,
	schema: Schema,
): Result<CompiledPredicate> {
	const ctx = createContext(schema);

	try {
		const predicate = compilePredicateInternal(expr, ctx);
		return ok(predicate);
	} catch {
		return err(ErrorCode.InvalidExpression);
	}
}

/**
 * Compile an expression to a value function.
 */
export function compileValue(
	expr: Expr,
	schema: Schema,
): Result<CompiledValue> {
	const ctx = createContext(schema);

	try {
		const value = compileValueInternal(expr, ctx);
		return ok(value);
	} catch {
		return err(ErrorCode.InvalidExpression);
	}
}

/**
 * Internal predicate compiler.
 */
function compilePredicateInternal(
	expr: Expr,
	ctx: CompileContext,
): CompiledPredicate {
	switch (expr.type) {
		case ExprType.Eq:
		case ExprType.Neq:
		case ExprType.Lt:
		case ExprType.Lte:
		case ExprType.Gt:
		case ExprType.Gte:
			return compileComparison(expr, ctx);

		case ExprType.Between:
			return compileBetween(expr, ctx);

		case ExprType.IsNull:
		case ExprType.IsNotNull:
			return compileNullCheck(expr, ctx);

		case ExprType.And:
			return compileAnd(expr, ctx);

		case ExprType.Or:
			return compileOr(expr, ctx);

		case ExprType.Not:
			return compileNot(expr, ctx);

		case ExprType.Contains:
		case ExprType.StartsWith:
		case ExprType.EndsWith:
			return compileStringOp(expr, ctx);

		case ExprType.Column:
			return compileColumnAsPredicate(expr, ctx);

		case ExprType.Literal:
			return compileLiteralAsPredicate(expr);

		case ExprType.IsIn:
			return compileIsIn(expr, ctx);

		default:
			throw new Error(`Cannot compile ${expr.type} as predicate`);
	}
}

/** Compile comparison expression. */
function compileComparison(
	expr: ComparisonExpr,
	ctx: CompileContext,
): CompiledPredicate {
	const leftValue = compileValueInternal(expr.left, ctx);
	const rightValue = compileValueInternal(expr.right, ctx);

	switch (expr.type) {
		case ExprType.Eq:
			return (chunk, row) => leftValue(chunk, row) === rightValue(chunk, row);
		case ExprType.Neq:
			return (chunk, row) => leftValue(chunk, row) !== rightValue(chunk, row);
		case ExprType.Lt:
			return (chunk, row) => {
				const l = leftValue(chunk, row);
				const r = rightValue(chunk, row);
				if (l === null || r === null) return false;
				return (
					(l as number | bigint | string) < (r as number | bigint | string)
				);
			};
		case ExprType.Lte:
			return (chunk, row) => {
				const l = leftValue(chunk, row);
				const r = rightValue(chunk, row);
				if (l === null || r === null) return false;
				return (
					(l as number | bigint | string) <= (r as number | bigint | string)
				);
			};
		case ExprType.Gt:
			return (chunk, row) => {
				const l = leftValue(chunk, row);
				const r = rightValue(chunk, row);
				if (l === null || r === null) return false;
				return (
					(l as number | bigint | string) > (r as number | bigint | string)
				);
			};
		case ExprType.Gte:
			return (chunk, row) => {
				const l = leftValue(chunk, row);
				const r = rightValue(chunk, row);
				if (l === null || r === null) return false;
				return (
					(l as number | bigint | string) >= (r as number | bigint | string)
				);
			};
		default:
			throw new Error(`Unknown comparison type: ${expr.type}`);
	}
}

/** Compile between expression. */
function compileBetween(
	expr: BetweenExpr,
	ctx: CompileContext,
): CompiledPredicate {
	const value = compileValueInternal(expr.expr, ctx);
	const low = compileValueInternal(expr.low, ctx);
	const high = compileValueInternal(expr.high, ctx);

	return (chunk, row) => {
		const v = value(chunk, row);
		const l = low(chunk, row);
		const h = high(chunk, row);
		if (v === null || l === null || h === null) return false;
		return (v as number) >= (l as number) && (v as number) <= (h as number);
	};
}

/** Compile null check expression. */
function compileNullCheck(
	expr: NullCheckExpr,
	ctx: CompileContext,
): CompiledPredicate {
	if (expr.expr.type === ExprType.Column) {
		const colIdx = ctx.columnIndices.get(expr.expr.name);
		if (colIdx === undefined) {
			throw new Error(`Unknown column: ${expr.expr.name}`);
		}

		if (expr.type === ExprType.IsNull) {
			return (chunk, row) => chunk.isNull(colIdx, row);
		} else {
			return (chunk, row) => !chunk.isNull(colIdx, row);
		}
	}

	const value = compileValueInternal(expr.expr, ctx);
	if (expr.type === ExprType.IsNull) {
		return (chunk, row) => value(chunk, row) === null;
	} else {
		return (chunk, row) => value(chunk, row) !== null;
	}
}

/** Compile AND expression. */
function compileAnd(expr: LogicalExpr, ctx: CompileContext): CompiledPredicate {
	const predicates = expr.exprs.map((e) => compilePredicateInternal(e, ctx));

	if (predicates.length === 2) {
		const [p1, p2] = predicates;
		return (chunk, row) => p1!(chunk, row) && p2!(chunk, row);
	}

	return (chunk, row) => {
		for (const pred of predicates) {
			if (!pred(chunk, row)) return false;
		}
		return true;
	};
}

/** Compile OR expression. */
function compileOr(expr: LogicalExpr, ctx: CompileContext): CompiledPredicate {
	const predicates = expr.exprs.map((e) => compilePredicateInternal(e, ctx));

	if (predicates.length === 2) {
		const [p1, p2] = predicates;
		return (chunk, row) => p1!(chunk, row) || p2!(chunk, row);
	}

	return (chunk, row) => {
		for (const pred of predicates) {
			if (pred(chunk, row)) return true;
		}
		return false;
	};
}

/** Compile NOT expression. */
function compileNot(expr: NotExpr, ctx: CompileContext): CompiledPredicate {
	const inner = compilePredicateInternal(expr.expr, ctx);
	return (chunk, row) => !inner(chunk, row);
}

/** Compile string operation. */
function compileStringOp(
	expr: StringOpExpr,
	ctx: CompileContext,
): CompiledPredicate {
	if (expr.expr.type !== ExprType.Column) {
		throw new Error("String operations only supported on column references");
	}

	const colIdx = ctx.columnIndices.get(expr.expr.name);
	if (colIdx === undefined) {
		throw new Error(`Unknown column: ${expr.expr.name}`);
	}

	const pattern = expr.pattern;

	switch (expr.type) {
		case ExprType.Contains:
			return (chunk, row) => {
				const str = chunk.getStringValue(colIdx, row);
				return str !== undefined && str.includes(pattern);
			};
		case ExprType.StartsWith:
			return (chunk, row) => {
				const str = chunk.getStringValue(colIdx, row);
				return str !== undefined && str.startsWith(pattern);
			};
		case ExprType.EndsWith:
			return (chunk, row) => {
				const str = chunk.getStringValue(colIdx, row);
				return str !== undefined && str.endsWith(pattern);
			};
		default:
			throw new Error(`Unknown string op: ${expr.type}`);
	}
}

/** Compile column reference as predicate (for boolean columns). */
function compileColumnAsPredicate(
	expr: ColumnExpr,
	ctx: CompileContext,
): CompiledPredicate {
	const colIdx = ctx.columnIndices.get(expr.name);
	if (colIdx === undefined) {
		throw new Error(`Unknown column: ${expr.name}`);
	}

	return (chunk, row) => {
		if (chunk.isNull(colIdx, row)) return false;
		const value = chunk.getValue(colIdx, row);
		return value === 1; // Boolean stored as 0/1 in Uint8Array
	};
}

/** Compile literal as predicate. */
function compileLiteralAsPredicate(expr: LiteralExpr): CompiledPredicate {
	const value = expr.value;
	if (typeof value === "boolean") {
		return () => value;
	}
	if (value === null) {
		return () => false;
	}
	return () => !!value;
}

/** Internal value compiler. */
function compileValueInternal(expr: Expr, ctx: CompileContext): CompiledValue {
	switch (expr.type) {
		case ExprType.Column:
			return compileColumn(expr, ctx);
		case ExprType.Literal:
			return compileLiteral(expr);
		case ExprType.Add:
		case ExprType.Sub:
		case ExprType.Mul:
		case ExprType.Div:
		case ExprType.Mod:
			return compileArithmetic(expr, ctx);
		case ExprType.Neg:
			return compileNeg(expr, ctx);
		case ExprType.Alias:
			return compileValueInternal(expr.expr, ctx);
		case ExprType.Cast:
			return compileCast(expr, ctx);
		case ExprType.Coalesce:
			return compileCoalesce(expr, ctx);
		case ExprType.Round:
			return compileRound(expr, ctx);
		case ExprType.Floor:
		case ExprType.Ceil:
		case ExprType.Abs:
		case ExprType.Sqrt:
			return compileUnaryMath(expr, ctx);
		case ExprType.Pow:
			return compilePow(expr, ctx);
		case ExprType.StringLength:
			return compileStringLength(expr, ctx);
		case ExprType.Substring:
			return compileSubstring(expr, ctx);
		case ExprType.Upper:
		case ExprType.Lower:
			return compileCaseConversion(expr, ctx);
		case ExprType.Trim:
			return compileTrim(expr, ctx);
		case ExprType.Replace:
			return compileReplace(expr, ctx);
		case ExprType.Year:
		case ExprType.Month:
		case ExprType.Day:
		case ExprType.DayOfWeek:
		case ExprType.Quarter:
		case ExprType.Hour:
		case ExprType.Minute:
		case ExprType.Second:
			return compileDateExtract(expr, ctx);
		case ExprType.When:
			return compileWhen(expr, ctx);
		default:
			throw new Error(`Cannot compile ${expr.type} as value`);
	}
}

/** Compile column reference. */
function compileColumn(expr: ColumnExpr, ctx: CompileContext): CompiledValue {
	const colIdx = ctx.columnIndices.get(expr.name);
	if (colIdx === undefined) {
		throw new Error(`Unknown column: ${expr.name}`);
	}

	const colDef = ctx.schema.columns[colIdx]!;
	const kind = colDef.dtype.kind;

	switch (kind) {
		case DTypeKind.String:
			return (chunk, row) => {
				if (chunk.isNull(colIdx, row)) return null;
				const s = chunk.getStringValue(colIdx, row);
				return s === undefined ? null : s;
			};

		case DTypeKind.Int32:
		case DTypeKind.Float64:
		case DTypeKind.Float32:
		case DTypeKind.Int16:
		case DTypeKind.Int8:
		case DTypeKind.UInt32:
		case DTypeKind.UInt16:
		case DTypeKind.UInt8:
			return (chunk, row) => {
				if (chunk.isNull(colIdx, row)) return null;
				return chunk.getValue(colIdx, row) as number;
			};

		case DTypeKind.Int64:
		case DTypeKind.UInt64:
		case DTypeKind.Timestamp:
			return (chunk, row) => {
				if (chunk.isNull(colIdx, row)) return null;
				return chunk.getValue(colIdx, row) as bigint;
			};

		case DTypeKind.Boolean:
			return (chunk, row) => {
				if (chunk.isNull(colIdx, row)) return null;
				const v = chunk.getValue(colIdx, row);
				return v === 1;
			};

		default:
			return (chunk, row) => {
				if (chunk.isNull(colIdx, row)) return null;
				return chunk.getValue(colIdx, row) as number;
			};
	}
}

/** Compile literal value. */
function compileLiteral(expr: LiteralExpr): CompiledValue {
	const value = expr.value;

	if (value === null) return () => null;
	if (typeof value === "number") return () => value;
	if (typeof value === "bigint") return () => value;
	if (typeof value === "boolean") return () => value;
	if (typeof value === "string") return () => value;

	return () => null;
}

/** Compile arithmetic expression. */
function compileArithmetic(
	expr: ArithmeticExpr,
	ctx: CompileContext,
): CompiledValue {
	const left = compileValueInternal(expr.left, ctx);
	const right = compileValueInternal(expr.right, ctx);

	switch (expr.type) {
		case ExprType.Add:
			return (chunk, row) => {
				const l = left(chunk, row);
				const r = right(chunk, row);
				if (l === null || r === null) return null;
				if (typeof l === "bigint" && typeof r === "bigint") return l + r;
				return (l as number) + (r as number);
			};

		case ExprType.Sub:
			return (chunk, row) => {
				const l = left(chunk, row);
				const r = right(chunk, row);
				if (l === null || r === null) return null;
				if (typeof l === "bigint" && typeof r === "bigint") return l - r;
				return (l as number) - (r as number);
			};

		case ExprType.Mul:
			return (chunk, row) => {
				const l = left(chunk, row);
				const r = right(chunk, row);
				if (l === null || r === null) return null;
				if (typeof l === "bigint" && typeof r === "bigint") return l * r;
				return (l as number) * (r as number);
			};

		case ExprType.Div:
			return (chunk, row) => {
				const l = left(chunk, row);
				const r = right(chunk, row);
				if (l === null || r === null) return null;
				if (r === 0 || r === 0n) return null;
				if (typeof l === "bigint" && typeof r === "bigint") return l / r;
				return (l as number) / (r as number);
			};

		case ExprType.Mod:
			return (chunk, row) => {
				const l = left(chunk, row);
				const r = right(chunk, row);
				if (l === null || r === null) return null;
				if (r === 0 || r === 0n) return null;
				if (typeof l === "bigint" && typeof r === "bigint") return l % r;
				return (l as number) % (r as number);
			};

		default:
			throw new Error(`Unknown arithmetic op: ${expr.type}`);
	}
}

/** Compile negation. */
function compileNeg(
	expr: { type: ExprType.Neg; expr: Expr },
	ctx: CompileContext,
): CompiledValue {
	const inner = compileValueInternal(expr.expr, ctx);

	return (chunk, row) => {
		const v = inner(chunk, row);
		if (v === null) return null;
		if (typeof v === "bigint") return -v;
		if (typeof v === "number") return -v;
		return null;
	};
}

/** Compile cast expression. */
function compileCast(expr: CastExpr, ctx: CompileContext): CompiledValue {
	const inner = compileValueInternal(expr.expr, ctx);
	const targetType = expr.targetDType;

	return (chunk, row) => {
		const v = inner(chunk, row);
		if (v === null) return null;

		switch (targetType) {
			case DTypeKind.String:
				return String(v);
			case DTypeKind.Int32:
			case DTypeKind.Int16:
			case DTypeKind.Int8:
			case DTypeKind.UInt32:
			case DTypeKind.UInt16:
			case DTypeKind.UInt8:
				return typeof v === "bigint" ? Number(v) | 0 : Number(v) | 0;
			case DTypeKind.Float64:
			case DTypeKind.Float32:
				return typeof v === "bigint" ? Number(v) : Number(v);
			case DTypeKind.Boolean:
				return Boolean(v);
			case DTypeKind.Int64:
			case DTypeKind.UInt64:
				return BigInt(v);
			case DTypeKind.Timestamp:
				return typeof v === "string" ? BigInt(Date.parse(v)) : BigInt(v);
			default:
				return v as number | bigint | string | boolean;
		}
	};
}

/** Compile coalesce expression. */
function compileCoalesce(
	expr: CoalesceExpr,
	ctx: CompileContext,
): CompiledValue {
	const compiledExprs = expr.exprs.map((e) => compileValueInternal(e, ctx));

	return (chunk, row) => {
		for (const compiled of compiledExprs) {
			const v = compiled(chunk, row);
			if (v !== null) return v;
		}
		return null;
	};
}

/** Compile round expression. */
function compileRound(expr: RoundExpr, ctx: CompileContext): CompiledValue {
	const inner = compileValueInternal(expr.expr, ctx);
	const decimals = expr.decimals;
	const factor = Math.pow(10, decimals);

	return (chunk, row) => {
		const v = inner(chunk, row);
		if (v === null) return null;
		const num = typeof v === "bigint" ? Number(v) : (v as number);
		return Math.round(num * factor) / factor;
	};
}

/** Compile unary math expression (floor, ceil, abs, sqrt). */
function compileUnaryMath(
	expr: UnaryMathExpr,
	ctx: CompileContext,
): CompiledValue {
	const inner = compileValueInternal(expr.expr, ctx);

	switch (expr.type) {
		case ExprType.Floor:
			return (chunk, row) => {
				const v = inner(chunk, row);
				if (v === null) return null;
				const num = typeof v === "bigint" ? Number(v) : (v as number);
				return Math.floor(num);
			};
		case ExprType.Ceil:
			return (chunk, row) => {
				const v = inner(chunk, row);
				if (v === null) return null;
				const num = typeof v === "bigint" ? Number(v) : (v as number);
				return Math.ceil(num);
			};
		case ExprType.Abs:
			return (chunk, row) => {
				const v = inner(chunk, row);
				if (v === null) return null;
				const num = typeof v === "bigint" ? Number(v) : (v as number);
				return Math.abs(num);
			};
		case ExprType.Sqrt:
			return (chunk, row) => {
				const v = inner(chunk, row);
				if (v === null) return null;
				const num = typeof v === "bigint" ? Number(v) : (v as number);
				return Math.sqrt(num);
			};
	}
}

/** Compile power expression. */
function compilePow(expr: PowExpr, ctx: CompileContext): CompiledValue {
	const inner = compileValueInternal(expr.expr, ctx);
	const exponent = expr.exponent;

	return (chunk, row) => {
		const v = inner(chunk, row);
		if (v === null) return null;
		const num = typeof v === "bigint" ? Number(v) : (v as number);
		return Math.pow(num, exponent);
	};
}

/** Compile string length expression. */
function compileStringLength(
	expr: StringLengthExpr,
	ctx: CompileContext,
): CompiledValue {
	const inner = compileValueInternal(expr.expr, ctx);

	return (chunk, row) => {
		const v = inner(chunk, row);
		if (v === null) return null;
		if (typeof v === "string") return v.length;
		return null;
	};
}

/** Compile substring expression. */
function compileSubstring(
	expr: SubstringExpr,
	ctx: CompileContext,
): CompiledValue {
	const inner = compileValueInternal(expr.expr, ctx);
	const start = expr.start;
	const length = expr.length;

	return (chunk, row) => {
		const v = inner(chunk, row);
		if (v === null) return null;
		if (typeof v === "string") {
			return length !== undefined ? v.substring(start, start + length) : v.substring(start);
		}
		return null;
	};
}

/** Compile case conversion expression (upper/lower). */
function compileCaseConversion(
	expr: CaseConversionExpr,
	ctx: CompileContext,
): CompiledValue {
	const inner = compileValueInternal(expr.expr, ctx);

	if (expr.type === ExprType.Upper) {
		return (chunk, row) => {
			const v = inner(chunk, row);
			if (v === null) return null;
			if (typeof v === "string") return v.toUpperCase();
			return null;
		};
	} else {
		return (chunk, row) => {
			const v = inner(chunk, row);
			if (v === null) return null;
			if (typeof v === "string") return v.toLowerCase();
			return null;
		};
	}
}

/** Compile trim expression. */
function compileTrim(expr: TrimExpr, ctx: CompileContext): CompiledValue {
	const inner = compileValueInternal(expr.expr, ctx);

	return (chunk, row) => {
		const v = inner(chunk, row);
		if (v === null) return null;
		if (typeof v === "string") return v.trim();
		return null;
	};
}

/** Compile replace expression. */
function compileReplace(expr: ReplaceExpr, ctx: CompileContext): CompiledValue {
	const inner = compileValueInternal(expr.expr, ctx);
	const pattern = expr.pattern;
	const replacement = expr.replacement;
	const all = expr.all;

	return (chunk, row) => {
		const v = inner(chunk, row);
		if (v === null) return null;
		if (typeof v === "string") {
			if (all) {
				return v.split(pattern).join(replacement);
			} else {
				return v.replace(pattern, replacement);
			}
		}
		return null;
	};
}

/** Compile date extraction expression. */
function compileDateExtract(
	expr: DateExtractExpr,
	ctx: CompileContext,
): CompiledValue {
	const inner = compileValueInternal(expr.expr, ctx);

	return (chunk, row) => {
		const v = inner(chunk, row);
		if (v === null) return null;

		// Convert to Date object
		// Timestamp is stored as bigint (ms since epoch)
		// Date is stored as int32 (days since epoch)
		let date: Date;
		if (typeof v === "bigint") {
			date = new Date(Number(v));
		} else if (typeof v === "number") {
			// Assume it's days since epoch for Date type, or ms for others
			// Check if it's a small number (days) or large number (ms)
			if (v < 100000) {
				// Days since epoch
				date = new Date(v * 86400000);
			} else {
				date = new Date(v);
			}
		} else {
			return null;
		}

		switch (expr.type) {
			case ExprType.Year:
				return date.getUTCFullYear();
			case ExprType.Month:
				return date.getUTCMonth() + 1; // 1-indexed
			case ExprType.Day:
				return date.getUTCDate();
			case ExprType.DayOfWeek:
				return date.getUTCDay();
			case ExprType.Quarter:
				return Math.floor(date.getUTCMonth() / 3) + 1;
			case ExprType.Hour:
				return date.getUTCHours();
			case ExprType.Minute:
				return date.getUTCMinutes();
			case ExprType.Second:
				return date.getUTCSeconds();
		}
	};
}

/** Compile when/otherwise expression. */
function compileWhen(expr: WhenExpr, ctx: CompileContext): CompiledValue {
	const compiledClauses = expr.clauses.map((clause) => ({
		condition: compilePredicateInternal(clause.condition, ctx),
		then: compileValueInternal(clause.then, ctx),
	}));
	const otherwise = compileValueInternal(expr.otherwise, ctx);

	return (chunk, row) => {
		for (const clause of compiledClauses) {
			if (clause.condition(chunk, row)) {
				return clause.then(chunk, row);
			}
		}
		return otherwise(chunk, row);
	};
}

/** Compile isIn expression as predicate. */
function compileIsIn(expr: IsInExpr, ctx: CompileContext): CompiledPredicate {
	const inner = compileValueInternal(expr.expr, ctx);
	const valueSet = new Set(expr.values);

	return (chunk, row) => {
		const v = inner(chunk, row);
		return valueSet.has(v as number | bigint | string | boolean | null);
	};
}
