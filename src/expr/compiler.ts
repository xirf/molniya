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
	type DateParseExpr,
	type Expr,
	ExprType,
	type FormatDateExpr,
		type DateAddExpr,
	type IsInExpr,
		type DiffDaysExpr,
	type LiteralExpr,
	type LogicalExpr,
	type NotExpr,
	type NullCheckExpr,
	type ParseJsonExpr,
	type PowExpr,
	type ReplaceExpr,
	type RoundExpr,
	type StringLengthExpr,
	type StringOpExpr,
		type DateParseExpr,
		type FormatDateExpr,
		type ParseJsonExpr,
	type SubstringExpr,
	type TrimExpr,
	type UnaryMathExpr,
	type WhenExpr,
		type TruncateDateExpr,
} from "./ast.ts";
import type { CompiledPredicate, CompiledValue, VectorizedPredicate } from "./compile-types.ts";

// Re-export apply functions
export { applyPredicate, applyValue, countMatching } from "./apply.ts";
// Re-export types
export type { CompiledPredicate, CompiledValue, VectorizedPredicate } from "./compile-types.ts";

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
 * Attempt to compile a vectorized predicate for the expression.
 * Returns null if the expression cannot be vectorized.
 *
 * Vectorizable patterns:
 * - col(numeric) op literal  (gt, gte, lt, lte, eq, neq)
 * - col(string) eq/neq literal (dictionary index comparison)
 * - col(string) isIn([literals])
 * - col(string) contains/startsWith/endsWith(literal)
 * - AND(vectorizable, vectorizable)
 * - NOT(vectorizable)
 */
export function compileVectorized(
	expr: Expr,
	schema: Schema,
): VectorizedPredicate | null {
	const ctx = createContext(schema);
	try {
		return compileVectorizedInternal(expr, ctx);
	} catch {
		return null;
	}
}

/**
 * Internal vectorized compiler — returns null for non-vectorizable expressions.
 */
function compileVectorizedInternal(
	expr: Expr,
	ctx: CompileContext,
): VectorizedPredicate | null {
	switch (expr.type) {
		case ExprType.Eq:
		case ExprType.Neq:
		case ExprType.Lt:
		case ExprType.Lte:
		case ExprType.Gt:
		case ExprType.Gte:
			return tryVectorizeComparison(expr as ComparisonExpr, ctx);

		case ExprType.Contains:
		case ExprType.StartsWith:
		case ExprType.EndsWith:
			return tryVectorizeStringOp(expr as StringOpExpr, ctx);

		case ExprType.IsIn:
			return tryVectorizeIsIn(expr as IsInExpr, ctx);

		case ExprType.And:
			return tryVectorizeAnd(expr as LogicalExpr, ctx);

		case ExprType.Not:
			return tryVectorizeNot(expr as NotExpr, ctx);

		case ExprType.Between:
			return tryVectorizeBetween(expr as BetweenExpr, ctx);

		default:
			return null;
	}
}

/**
 * Vectorize a numeric comparison: col(x) op literal.
 * Also handles dictionary string eq/neq.
 */
function tryVectorizeComparison(
	expr: ComparisonExpr,
	ctx: CompileContext,
): VectorizedPredicate | null {
	// Pattern: Column op Literal
	let colIdx: number | undefined;
	let literalValue: number | bigint | string | boolean | null = null;
	let colOnLeft = true;

	if (expr.left.type === ExprType.Column && expr.right.type === ExprType.Literal) {
		colIdx = ctx.columnIndices.get((expr.left as ColumnExpr).name);
		literalValue = (expr.right as LiteralExpr).value;
		colOnLeft = true;
	} else if (expr.right.type === ExprType.Column && expr.left.type === ExprType.Literal) {
		colIdx = ctx.columnIndices.get((expr.right as ColumnExpr).name);
		literalValue = (expr.left as LiteralExpr).value;
		colOnLeft = false;
	}

	if (colIdx === undefined || literalValue === null) return null;

	const colDef = ctx.schema.columns[colIdx];
	if (!colDef) return null;
	const kind = colDef.dtype.kind;

	// String dictionary path: eq/neq only
	if (kind === DTypeKind.String && typeof literalValue === "string") {
		if (expr.type !== ExprType.Eq && expr.type !== ExprType.Neq) return null;
		return vectorizeDictEqNeq(colIdx, literalValue, expr.type === ExprType.Eq);
	}

	// Numeric path
	if (typeof literalValue !== "number" && typeof literalValue !== "bigint") return null;

	const threshold = typeof literalValue === "bigint" ? Number(literalValue) : literalValue;
	const nullable = colDef.dtype.nullable;
	const ci = colIdx;

	// Generate vectorized predicate based on comparison type
	// Swap comparison direction if literal is on the left: 5 < col → col > 5
	let effectiveType = expr.type;
	if (!colOnLeft) {
		switch (expr.type) {
			case ExprType.Lt: effectiveType = ExprType.Gt; break;
			case ExprType.Lte: effectiveType = ExprType.Gte; break;
			case ExprType.Gt: effectiveType = ExprType.Lt; break;
			case ExprType.Gte: effectiveType = ExprType.Lte; break;
		}
	}

	return (chunk, selOut) => {
		const col = chunk.getColumn(ci);
		if (!col) return 0;
		const data = col.data;
		const selection = chunk.getSelection();
		let count = 0;

		if (selection === null) {
			// No existing selection: scan all physical rows
			const len = chunk.getPhysicalRowCount();
			if (nullable) {
				switch (effectiveType) {
					case ExprType.Gt:
						for (let i = 0; i < len; i++) { if (!col.isNull(i) && (data[i] as number) > threshold) selOut[count++] = i; } break;
					case ExprType.Gte:
						for (let i = 0; i < len; i++) { if (!col.isNull(i) && (data[i] as number) >= threshold) selOut[count++] = i; } break;
					case ExprType.Lt:
						for (let i = 0; i < len; i++) { if (!col.isNull(i) && (data[i] as number) < threshold) selOut[count++] = i; } break;
					case ExprType.Lte:
						for (let i = 0; i < len; i++) { if (!col.isNull(i) && (data[i] as number) <= threshold) selOut[count++] = i; } break;
					case ExprType.Eq:
						for (let i = 0; i < len; i++) { if (!col.isNull(i) && data[i] === threshold) selOut[count++] = i; } break;
					case ExprType.Neq:
						for (let i = 0; i < len; i++) { if (!col.isNull(i) && data[i] !== threshold) selOut[count++] = i; } break;
				}
			} else {
				switch (effectiveType) {
					case ExprType.Gt:
						for (let i = 0; i < len; i++) { if ((data[i] as number) > threshold) selOut[count++] = i; } break;
					case ExprType.Gte:
						for (let i = 0; i < len; i++) { if ((data[i] as number) >= threshold) selOut[count++] = i; } break;
					case ExprType.Lt:
						for (let i = 0; i < len; i++) { if ((data[i] as number) < threshold) selOut[count++] = i; } break;
					case ExprType.Lte:
						for (let i = 0; i < len; i++) { if ((data[i] as number) <= threshold) selOut[count++] = i; } break;
					case ExprType.Eq:
						for (let i = 0; i < len; i++) { if (data[i] === threshold) selOut[count++] = i; } break;
					case ExprType.Neq:
						for (let i = 0; i < len; i++) { if (data[i] !== threshold) selOut[count++] = i; } break;
				}
			}
		} else {
			// Has existing selection: scan only selected rows
			const len = selection.length;
			if (nullable) {
				switch (effectiveType) {
					case ExprType.Gt:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if (!col.isNull(p) && (data[p] as number) > threshold) selOut[count++] = p; } break;
					case ExprType.Gte:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if (!col.isNull(p) && (data[p] as number) >= threshold) selOut[count++] = p; } break;
					case ExprType.Lt:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if (!col.isNull(p) && (data[p] as number) < threshold) selOut[count++] = p; } break;
					case ExprType.Lte:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if (!col.isNull(p) && (data[p] as number) <= threshold) selOut[count++] = p; } break;
					case ExprType.Eq:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if (!col.isNull(p) && data[p] === threshold) selOut[count++] = p; } break;
					case ExprType.Neq:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if (!col.isNull(p) && data[p] !== threshold) selOut[count++] = p; } break;
				}
			} else {
				switch (effectiveType) {
					case ExprType.Gt:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if ((data[p] as number) > threshold) selOut[count++] = p; } break;
					case ExprType.Gte:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if ((data[p] as number) >= threshold) selOut[count++] = p; } break;
					case ExprType.Lt:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if ((data[p] as number) < threshold) selOut[count++] = p; } break;
					case ExprType.Lte:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if ((data[p] as number) <= threshold) selOut[count++] = p; } break;
					case ExprType.Eq:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if (data[p] === threshold) selOut[count++] = p; } break;
					case ExprType.Neq:
						for (let i = 0; i < len; i++) { const p = selection[i]!; if (data[p] !== threshold) selOut[count++] = p; } break;
				}
			}
		}
		return count;
	};
}

/**
 * Vectorize dictionary string eq/neq.
 */
function vectorizeDictEqNeq(
	colIdx: number,
	targetStr: string,
	isEq: boolean,
): VectorizedPredicate {
	// Cache the interned dictionary index per dictionary instance
	let cachedTargetIdx: number | undefined;
	let cachedDict: unknown = null;

	return (chunk, selOut) => {
		const dict = chunk.dictionary;
		if (dict !== cachedDict) {
			cachedDict = dict;
			cachedTargetIdx = dict?.internString(targetStr);
		}

		const col = chunk.getColumn(colIdx);
		if (!col || cachedTargetIdx === undefined) return isEq ? 0 : chunk.rowCount;

		const data = col.data;
		const selection = chunk.getSelection();
		const target = cachedTargetIdx;
		let count = 0;

		if (selection === null) {
			const len = chunk.getPhysicalRowCount();
			if (isEq) {
				for (let i = 0; i < len; i++) { if (data[i] === target) selOut[count++] = i; }
			} else {
				for (let i = 0; i < len; i++) { if (data[i] !== target) selOut[count++] = i; }
			}
		} else {
			const len = selection.length;
			if (isEq) {
				for (let i = 0; i < len; i++) { const p = selection[i]!; if (data[p] === target) selOut[count++] = p; }
			} else {
				for (let i = 0; i < len; i++) { const p = selection[i]!; if (data[p] !== target) selOut[count++] = p; }
			}
		}
		return count;
	};
}

/**
 * Vectorize string ops (contains/startsWith/endsWith) with byte-level comparison.
 */
function tryVectorizeStringOp(
	expr: StringOpExpr,
	ctx: CompileContext,
): VectorizedPredicate | null {
	if (expr.expr.type !== ExprType.Column) return null;

	const colIdx = ctx.columnIndices.get(expr.expr.name);
	if (colIdx === undefined) return null;

	const patternBytes = new TextEncoder().encode(expr.pattern);
	const ci = colIdx;

	let matchFn: (strBytes: Uint8Array) => boolean;
	switch (expr.type) {
		case ExprType.Contains:
			matchFn = (b) => bytesContains(b, patternBytes);
			break;
		case ExprType.StartsWith:
			matchFn = (b) => bytesStartsWith(b, patternBytes);
			break;
		case ExprType.EndsWith:
			matchFn = (b) => bytesEndsWith(b, patternBytes);
			break;
		default:
			return null;
	}

	return (chunk, selOut) => {
		const dict = chunk.dictionary;
		if (!dict) return 0;

		const col = chunk.getColumn(ci);
		if (!col) return 0;

		const data = col.data;
		const selection = chunk.getSelection();
		let count = 0;

		if (selection === null) {
			const len = chunk.getPhysicalRowCount();
			for (let i = 0; i < len; i++) {
				if (col.isNull(i)) continue;
				const strBytes = dict.getBytes(data[i] as number);
				if (strBytes && matchFn(strBytes)) selOut[count++] = i;
			}
		} else {
			const slen = selection.length;
			for (let i = 0; i < slen; i++) {
				const p = selection[i]!;
				if (col.isNull(p)) continue;
				const strBytes = dict.getBytes(data[p] as number);
				if (strBytes && matchFn(strBytes)) selOut[count++] = p;
			}
		}
		return count;
	};
}

/**
 * Vectorize isIn for dictionary string columns.
 */
function tryVectorizeIsIn(
	expr: IsInExpr,
	ctx: CompileContext,
): VectorizedPredicate | null {
	if (expr.expr.type !== ExprType.Column) return null;

	const colIdx = ctx.columnIndices.get(expr.expr.name);
	if (colIdx === undefined) return null;

	const colDef = ctx.schema.columns[colIdx];
	if (!colDef) return null;

	const kind = colDef.dtype.kind;
	const ci = colIdx;

	if (kind === DTypeKind.String) {
		// Dictionary string isIn: cache interned indices per dictionary
		const literalStrings = expr.values.filter((v): v is string => typeof v === "string");
		if (literalStrings.length !== expr.values.length) return null;

		let cachedIndexSet: Set<number> | null = null;
		let cachedDict: unknown = null;

		return (chunk, selOut) => {
			const dict = chunk.dictionary;
			if (dict !== cachedDict) {
				cachedDict = dict;
				cachedIndexSet = new Set<number>();
				for (const s of literalStrings) {
					const idx = dict?.internString(s);
					if (idx !== undefined) cachedIndexSet.add(idx);
				}
			}
			if (!cachedIndexSet || cachedIndexSet.size === 0) return 0;

			const col = chunk.getColumn(ci);
			if (!col) return 0;
			const data = col.data;
			const selection = chunk.getSelection();
			const indexSet = cachedIndexSet;
			let count = 0;

			if (selection === null) {
				const len = chunk.getPhysicalRowCount();
				for (let i = 0; i < len; i++) { if (indexSet.has(data[i] as number)) selOut[count++] = i; }
			} else {
				const slen = selection.length;
				for (let i = 0; i < slen; i++) { const p = selection[i]!; if (indexSet.has(data[p] as number)) selOut[count++] = p; }
			}
			return count;
		};
	}

	// Numeric isIn
	if (expr.values.every((v) => typeof v === "number")) {
		const numSet = new Set(expr.values as number[]);

		return (chunk, selOut) => {
			const col = chunk.getColumn(ci);
			if (!col) return 0;
			const data = col.data;
			const selection = chunk.getSelection();
			let count = 0;

			if (selection === null) {
				const len = chunk.getPhysicalRowCount();
				for (let i = 0; i < len; i++) { if (numSet.has(data[i] as number)) selOut[count++] = i; }
			} else {
				const slen = selection.length;
				for (let i = 0; i < slen; i++) { const p = selection[i]!; if (numSet.has(data[p] as number)) selOut[count++] = p; }
			}
			return count;
		};
	}

	return null;
}

/**
 * Vectorize AND: apply left vectorized, then right vectorized on the result.
 */
function tryVectorizeAnd(
	expr: LogicalExpr,
	ctx: CompileContext,
): VectorizedPredicate | null {
	const vecPreds: VectorizedPredicate[] = [];
	for (const sub of expr.exprs) {
		const vec = compileVectorizedInternal(sub, ctx);
		if (!vec) return null; // All sub-expressions must be vectorizable
		vecPreds.push(vec);
	}

	if (vecPreds.length === 0) return null;
	if (vecPreds.length === 1) return vecPreds[0]!;

	return (chunk, selOut) => {
		// Apply first predicate to get initial selection
		let count = vecPreds[0]!(chunk, selOut);
		if (count === 0) return 0;

		// For subsequent predicates, create a temporary chunk view with the selection
		// and re-filter. We reuse the selOut buffer.
		const tempSel = new Uint32Array(count);
		for (let p = 1; p < vecPreds.length; p++) {
			// Copy current selection to temp
			tempSel.set(selOut.subarray(0, count));

			// Create a virtual view by passing selection through
			// We need to filter tempSel[0..count] and output matching to selOut
			let newCount = 0;
			const predFn = vecPreds[p]!;

			// Use a scratch buffer for the inner predicate results
			const innerOut = new Uint32Array(count);
			// Temporarily apply selection via a view chunk
			const view = chunk.withSelection(tempSel.subarray(0, count), count);
			newCount = predFn(view, innerOut);

			// Copy results back to selOut
			for (let i = 0; i < newCount; i++) {
				selOut[i] = innerOut[i]!;
			}
			count = newCount;
			if (count === 0) return 0;
		}
		return count;
	};
}

/**
 * Vectorize NOT: apply inner vectorized, then invert.
 */
function tryVectorizeNot(
	expr: NotExpr,
	ctx: CompileContext,
): VectorizedPredicate | null {
	const inner = compileVectorizedInternal(expr.expr, ctx);
	if (!inner) return null;

	return (chunk, selOut) => {
		// Get matching rows from inner predicate
		const matchCount = inner(chunk, selOut);

		// Build complement: all rows NOT in the match set
		const selection = chunk.getSelection();
		const total = chunk.rowCount;

		if (matchCount === 0) {
			// All rows match NOT
			if (selection === null) {
				const len = chunk.getPhysicalRowCount();
				for (let i = 0; i < len; i++) selOut[i] = i;
				return len;
			} else {
				for (let i = 0; i < total; i++) selOut[i] = selection[i]!;
				return total;
			}
		}

		// Build a set of matched physical indices for fast lookup
		const matched = new Set<number>();
		for (let i = 0; i < matchCount; i++) matched.add(selOut[i]!);

		let count = 0;
		if (selection === null) {
			const len = chunk.getPhysicalRowCount();
			for (let i = 0; i < len; i++) {
				if (!matched.has(i)) selOut[count++] = i;
			}
		} else {
			for (let i = 0; i < total; i++) {
				const p = selection[i]!;
				if (!matched.has(p)) selOut[count++] = p;
			}
		}
		return count;
	};
}

/**
 * Vectorize BETWEEN: col >= low AND col <= high.
 */
function tryVectorizeBetween(
	expr: BetweenExpr,
	ctx: CompileContext,
): VectorizedPredicate | null {
	if (expr.expr.type !== ExprType.Column) return null;
	if (expr.low.type !== ExprType.Literal || expr.high.type !== ExprType.Literal) return null;

	const colIdx = ctx.columnIndices.get(expr.expr.name);
	if (colIdx === undefined) return null;

	const low = (expr.low as LiteralExpr).value;
	const high = (expr.high as LiteralExpr).value;
	if (typeof low !== "number" || typeof high !== "number") return null;

	const ci = colIdx;
	const colDef = ctx.schema.columns[ci];
	const nullable = colDef?.dtype.nullable ?? false;

	return (chunk, selOut) => {
		const col = chunk.getColumn(ci);
		if (!col) return 0;
		const data = col.data;
		const selection = chunk.getSelection();
		let count = 0;

		if (selection === null) {
			const len = chunk.getPhysicalRowCount();
			if (nullable) {
				for (let i = 0; i < len; i++) {
					if (!col.isNull(i)) {
						const v = data[i] as number;
						if (v >= low && v <= high) selOut[count++] = i;
					}
				}
			} else {
				for (let i = 0; i < len; i++) {
					const v = data[i] as number;
					if (v >= low && v <= high) selOut[count++] = i;
				}
			}
		} else {
			const slen = selection.length;
			if (nullable) {
				for (let i = 0; i < slen; i++) {
					const p = selection[i]!;
					if (!col.isNull(p)) {
						const v = data[p] as number;
						if (v >= low && v <= high) selOut[count++] = p;
					}
				}
			} else {
				for (let i = 0; i < slen; i++) {
					const p = selection[i]!;
					const v = data[p] as number;
					if (v >= low && v <= high) selOut[count++] = p;
				}
			}
		}
		return count;
	};
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
	// Fast path: dictionary-level string comparison
	// Compares dictionary indices (integers) instead of decoding strings per row
	if (expr.type === ExprType.Eq || expr.type === ExprType.Neq) {
		const dictPred = tryDictStringComparison(expr, ctx);
		if (dictPred) return dictPred;
	}

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

/**
 * Try to compile a dictionary-level string comparison.
 * For col("x").eq("literal"), compares dictionary indices instead of
 * decoding strings — ~100x faster for string equality/inequality.
 */
function tryDictStringComparison(
	expr: ComparisonExpr,
	ctx: CompileContext,
): CompiledPredicate | null {
	let colIdx: number | undefined;
	let literalStr: string | null = null;

	// Detect: Column(String) == Literal(string) or Literal(string) == Column(String)
	if (expr.left.type === ExprType.Column && expr.right.type === ExprType.Literal) {
		const ci = ctx.columnIndices.get(expr.left.name);
		const colDef = ci !== undefined ? ctx.schema.columns[ci] : undefined;
		if (colDef?.dtype.kind === DTypeKind.String && typeof (expr.right as LiteralExpr).value === "string") {
			colIdx = ci;
			literalStr = (expr.right as LiteralExpr).value as string;
		}
	} else if (expr.right.type === ExprType.Column && expr.left.type === ExprType.Literal) {
		const ci = ctx.columnIndices.get(expr.right.name);
		const colDef = ci !== undefined ? ctx.schema.columns[ci] : undefined;
		if (colDef?.dtype.kind === DTypeKind.String && typeof (expr.left as LiteralExpr).value === "string") {
			colIdx = ci;
			literalStr = (expr.left as LiteralExpr).value as string;
		}
	}

	if (colIdx === undefined || literalStr === null) return null;

	const isEq = expr.type === ExprType.Eq;
	const targetStr = literalStr;
	const ci = colIdx;

	// Cache the interned dictionary index per dictionary instance
	let cachedTargetIdx: number | undefined;
	let cachedDict: unknown = null;

	return (chunk, row) => {
		const dict = chunk.dictionary;
		if (dict !== cachedDict) {
			cachedDict = dict;
			cachedTargetIdx = dict?.internString(targetStr);
		}
		if (cachedTargetIdx === undefined) return !isEq;
		if (chunk.isNull(ci, row)) return false;
		const rawIdx = chunk.getValue(ci, row);
		return isEq ? rawIdx === cachedTargetIdx : rawIdx !== cachedTargetIdx;
	};
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

/** Compile string operation (contains/startsWith/endsWith).
 * Uses byte-level comparison on UTF-8 data to avoid TextDecoder per row.
 */
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
	// Pre-encode pattern to UTF-8 bytes once at compile time
	const patternBytes = new TextEncoder().encode(pattern);

	switch (expr.type) {
		case ExprType.Contains:
			return (chunk, row) => {
				if (chunk.isNull(colIdx, row)) return false;
				const rawIdx = chunk.getValue(colIdx, row) as number;
				const strBytes = chunk.dictionary?.getBytes(rawIdx);
				if (!strBytes) return false;
				return bytesContains(strBytes, patternBytes);
			};
		case ExprType.StartsWith:
			return (chunk, row) => {
				if (chunk.isNull(colIdx, row)) return false;
				const rawIdx = chunk.getValue(colIdx, row) as number;
				const strBytes = chunk.dictionary?.getBytes(rawIdx);
				if (!strBytes) return false;
				return bytesStartsWith(strBytes, patternBytes);
			};
		case ExprType.EndsWith:
			return (chunk, row) => {
				if (chunk.isNull(colIdx, row)) return false;
				const rawIdx = chunk.getValue(colIdx, row) as number;
				const strBytes = chunk.dictionary?.getBytes(rawIdx);
				if (!strBytes) return false;
				return bytesEndsWith(strBytes, patternBytes);
			};
		default:
			throw new Error(`Unknown string op: ${expr.type}`);
	}
}

/** Check if haystack contains needle (byte-level, no string decode). */
function bytesContains(haystack: Uint8Array, needle: Uint8Array): boolean {
	if (needle.length === 0) return true;
	if (needle.length > haystack.length) return false;
	const end = haystack.length - needle.length;
	const first = needle[0]!;
	outer: for (let i = 0; i <= end; i++) {
		if (haystack[i] !== first) continue;
		for (let j = 1; j < needle.length; j++) {
			if (haystack[i + j] !== needle[j]) continue outer;
		}
		return true;
	}
	return false;
}

/** Check if haystack starts with prefix (byte-level). */
function bytesStartsWith(haystack: Uint8Array, prefix: Uint8Array): boolean {
	if (prefix.length > haystack.length) return false;
	for (let i = 0; i < prefix.length; i++) {
		if (haystack[i] !== prefix[i]) return false;
	}
	return true;
}

/** Check if haystack ends with suffix (byte-level). */
function bytesEndsWith(haystack: Uint8Array, suffix: Uint8Array): boolean {
	if (suffix.length > haystack.length) return false;
	const offset = haystack.length - suffix.length;
	for (let i = 0; i < suffix.length; i++) {
		if (haystack[offset + i] !== suffix[i]) return false;
	}
	return true;
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
		case ExprType.AddDays:
		case ExprType.SubDays:
			return compileDateAdd(expr, ctx);
		case ExprType.DiffDays:
			return compileDiffDays(expr, ctx);
		case ExprType.TruncateDate:
			return compileTruncateDate(expr, ctx);
		case ExprType.ToDate:
		case ExprType.ToTimestamp:
			return compileDateParse(expr, ctx);
		case ExprType.FormatDate:
			return compileFormatDate(expr, ctx);
		case ExprType.ParseJson:
			return compileParseJson(expr, ctx);
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

/** Compile addDays/subDays expressions. */
function compileDateAdd(expr: DateAddExpr, ctx: CompileContext): CompiledValue {
	const value = compileValueInternal(expr.expr, ctx);
	const delta = expr.type === ExprType.SubDays ? -expr.days : expr.days;

	return (chunk, row) => {
		const v = value(chunk, row);
		if (v === null) return null;
		const date = coerceToDate(v);
		if (!date) return null;
		const shifted = date.getTime() + delta * 86400000;
		return Math.floor(shifted / 86400000);
	};
}

/** Compile diffDays expression. */
function compileDiffDays(expr: DiffDaysExpr, ctx: CompileContext): CompiledValue {
	const left = compileValueInternal(expr.left, ctx);
	const right = compileValueInternal(expr.right, ctx);

	return (chunk, row) => {
		const l = left(chunk, row);
		const r = right(chunk, row);
		if (l === null || r === null) return null;
		const leftDate = coerceToDate(l);
		const rightDate = coerceToDate(r);
		if (!leftDate || !rightDate) return null;
		return Math.floor(
			(leftDate.getTime() - rightDate.getTime()) / 86400000,
		);
	};
}

/** Compile truncateDate expression. */
function compileTruncateDate(
	expr: TruncateDateExpr,
	ctx: CompileContext,
): CompiledValue {
	const value = compileValueInternal(expr.expr, ctx);
	const period = expr.period;

	return (chunk, row) => {
		const v = value(chunk, row);
		if (v === null) return null;
		const date = coerceToDate(v);
		if (!date) return null;

		let y = date.getUTCFullYear();
		let m = date.getUTCMonth();
		let d = date.getUTCDate();

		switch (period) {
			case "year":
				m = 0;
				d = 1;
				break;
			case "quarter":
				m = Math.floor(m / 3) * 3;
				d = 1;
				break;
			case "month":
				d = 1;
				break;
			case "week": {
				const dayOfWeek = date.getUTCDay();
				// ISO week starts on Monday (1). Adjust Sunday (0) to 6.
				const offset = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
				const monday = new Date(
					Date.UTC(y, m, d - offset, 0, 0, 0, 0),
				);
				return Math.floor(monday.getTime() / 86400000);
			}
			case "day":
			default:
				break;
		}

		const truncated = Date.UTC(y, m, d, 0, 0, 0, 0);
		return Math.floor(truncated / 86400000);
	};
}

/** Compile toDate/toTimestamp expressions. */
function compileDateParse(
	expr: DateParseExpr,
	ctx: CompileContext,
): CompiledValue {
	const value = compileValueInternal(expr.expr, ctx);
	const parser = expr.format ? buildDateParser(expr.format) : null;
	const isTimestamp = expr.type === ExprType.ToTimestamp;

	return (chunk, row) => {
		const v = value(chunk, row);
		if (v === null) return null;
		const date = coerceToDate(v, parser);
		if (!date) return null;
		if (isTimestamp) return BigInt(date.getTime());
		return Math.floor(date.getTime() / 86400000);
	};
}

/** Compile formatDate expression. */
function compileFormatDate(
	expr: FormatDateExpr,
	ctx: CompileContext,
): CompiledValue {
	const value = compileValueInternal(expr.expr, ctx);
	const formatter = buildDateFormatter(expr.format);

	return (chunk, row) => {
		const v = value(chunk, row);
		if (v === null) return null;
		const date = coerceToDate(v);
		if (!date) return null;
		return formatter(date);
	};
}

/** Compile parseJson expression. */
function compileParseJson(
	expr: ParseJsonExpr,
	ctx: CompileContext,
): CompiledValue {
	const value = compileValueInternal(expr.expr, ctx);
	return (chunk, row) => {
		const v = value(chunk, row);
		if (v === null) return null;
		if (typeof v !== "string") return null;
		try {
			const parsed = JSON.parse(v);
			if (parsed === null || parsed === undefined) return null;
			if (typeof parsed === "string") return parsed;
			if (typeof parsed === "number" || typeof parsed === "boolean") {
				return String(parsed);
			}
			return JSON.stringify(parsed);
		} catch {
			return null;
		}
	};
}

type DateParser = (value: string) => Date | null;
type DateFormatter = (value: Date) => string;

function coerceToDate(value: unknown, parser?: DateParser | null): Date | null {
	if (value instanceof Date) return value;
	if (typeof value === "bigint") {
		const asNumber = Number(value);
		if (Number.isFinite(asNumber)) return new Date(asNumber);
		return null;
	}
	if (typeof value === "number") {
		if (Number.isInteger(value) && Math.abs(value) < 10_000_000) {
			return new Date(value * 86400000);
		}
		return new Date(value);
	}
	if (typeof value === "string") {
		if (parser) return parser(value);
		const parsed = Date.parse(value);
		if (Number.isNaN(parsed)) return null;
		return new Date(parsed);
	}
	return null;
}

function buildDateParser(format: string): DateParser {
	const tokens = ["YYYY", "MMM", "MM", "DD", "HH", "mm", "ss", "SSS"] as const;
	const tokenRegex: Record<(typeof tokens)[number], string> = {
		YYYY: "(\\d{4})",
		MMM: "([A-Za-z]{3})",
		MM: "(\\d{2})",
		DD: "(\\d{2})",
		HH: "(\\d{2})",
		mm: "(\\d{2})",
		ss: "(\\d{2})",
		SSS: "(\\d{3})",
	};

	const groups: string[] = [];
	let pattern = "";
	for (let i = 0; i < format.length; ) {
		let matched = false;
		for (const token of tokens) {
			if (format.startsWith(token, i)) {
				pattern += tokenRegex[token];
				groups.push(token);
				i += token.length;
				matched = true;
				break;
			}
		}
		if (!matched) {
			const ch = format[i] ?? "";
			pattern += ch.replace(/[.*+?^${}()|[\\]\\]/g, "\\$&");
			i++;
		}
	}

	const regex = new RegExp(`^${pattern}$`);
	const monthNames = [
		"JAN",
		"FEB",
		"MAR",
		"APR",
		"MAY",
		"JUN",
		"JUL",
		"AUG",
		"SEP",
		"OCT",
		"NOV",
		"DEC",
	];

	return (value) => {
		const match = regex.exec(value);
		if (!match) return null;
		let year = 1970;
		let month = 1;
		let day = 1;
		let hour = 0;
		let minute = 0;
		let second = 0;
		let millis = 0;

		for (let i = 0; i < groups.length; i++) {
			const token = groups[i];
			const raw = match[i + 1];
			if (!raw || !token) continue;
			switch (token) {
				case "YYYY":
					year = Number(raw);
					break;
				case "MMM": {
					const idx = monthNames.indexOf(raw.toUpperCase());
					if (idx >= 0) month = idx + 1;
					break;
				}
				case "MM":
					month = Number(raw);
					break;
				case "DD":
					day = Number(raw);
					break;
				case "HH":
					hour = Number(raw);
					break;
				case "mm":
					minute = Number(raw);
					break;
				case "ss":
					second = Number(raw);
					break;
				case "SSS":
					millis = Number(raw);
					break;
			}
		}

		const time = Date.UTC(year, month - 1, day, hour, minute, second, millis);
		return new Date(time);
	};
}

function buildDateFormatter(format: string): DateFormatter {
	const monthNames = [
		"Jan",
		"Feb",
		"Mar",
		"Apr",
		"May",
		"Jun",
		"Jul",
		"Aug",
		"Sep",
		"Oct",
		"Nov",
		"Dec",
	];

	return (date) => {
		const yyyy = date.getUTCFullYear().toString().padStart(4, "0");
		const mm = (date.getUTCMonth() + 1).toString().padStart(2, "0");
		const mmm = monthNames[date.getUTCMonth()] ?? "";
		const dd = date.getUTCDate().toString().padStart(2, "0");
		const hh = date.getUTCHours().toString().padStart(2, "0");
		const min = date.getUTCMinutes().toString().padStart(2, "0");
		const ss = date.getUTCSeconds().toString().padStart(2, "0");
		const ms = date.getUTCMilliseconds().toString().padStart(3, "0");

		return format.replace(/YYYY|MMM|MM|DD|HH|mm|ss|SSS/g, (token) => {
			switch (token) {
				case "YYYY":
					return yyyy;
				case "MMM":
					return mmm;
				case "MM":
					return mm;
				case "DD":
					return dd;
				case "HH":
					return hh;
				case "mm":
					return min;
				case "ss":
					return ss;
				case "SSS":
					return ms;
				default:
					return token;
			}
		});
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
	// Fast path: dictionary-level isIn for string columns
	// Compares dictionary indices instead of decoding strings per row
	if (expr.expr.type === ExprType.Column) {
		const colIdx = ctx.columnIndices.get(expr.expr.name);
		const colDef = colIdx !== undefined ? ctx.schema.columns[colIdx] : undefined;
		if (
			colDef?.dtype.kind === DTypeKind.String &&
			expr.values.every((v) => typeof v === "string")
		) {
			const stringValues = expr.values as string[];
			const ci = colIdx!;
			let cachedIdxSet: Set<number> | null = null;
			let cachedDict: unknown = null;

			return (chunk, row) => {
				const dict = chunk.dictionary;
				if (dict !== cachedDict) {
					cachedDict = dict;
					cachedIdxSet = new Set<number>();
					if (dict) {
						for (const s of stringValues) {
							cachedIdxSet.add(dict.internString(s));
						}
					}
				}
				if (!cachedIdxSet || cachedIdxSet.size === 0) return false;
				if (chunk.isNull(ci, row)) return false;
				const rawIdx = chunk.getValue(ci, row) as number;
				return cachedIdxSet.has(rawIdx);
			};
		}
	}

	// General path: compare materialized values
	const inner = compileValueInternal(expr.expr, ctx);
	const valueSet = new Set(expr.values);

	return (chunk, row) => {
		const v = inner(chunk, row);
		return valueSet.has(v as number | bigint | string | boolean | null);
	};
}
