/* FILTERING METHODS
/*-----------------------------------------------------
/* Filter and where methods for DataFrame
/* ==================================================== */

import type { Expr } from "../expr/ast.ts";
import { ExprType } from "../expr/ast.ts";
import { ColumnRef } from "../expr/builders.ts";
import { FilterOperator, filter } from "../ops/index.ts";
import { ErrorCode } from "../types/error.ts";
import type { DataFrame } from "./core.ts";

export function addFilteringMethods(df: typeof DataFrame.prototype) {
	df.filter = function (expr: Expr | ColumnRef): DataFrame {
		const condition = expr instanceof ColumnRef ? expr.toExpr() : expr;

		// Filter fusion: if the last operator is also a Filter with a known Expr,
		// fuse them into a single AND filter for better vectorization.
		if (this.operators.length > 0) {
			const lastOp = this.operators[this.operators.length - 1];
			if (lastOp instanceof FilterOperator && lastOp.expr !== null) {
				const fusedExpr: Expr = {
					type: ExprType.And,
					exprs: [lastOp.expr, condition],
				};
				const fusedResult = filter(lastOp.outputSchema, fusedExpr);
				if (fusedResult.error === ErrorCode.None) {
					// Replace last filter with fused filter
					const newOps = this.operators.slice(0, -1);
					return this.withOperators(newOps, fusedResult.value);
				}
			}
		}

		const result = filter(this.currentSchema(), condition);
		if (result.error !== ErrorCode.None) {
			throw new Error(`Filter error: ${result.error}`);
		}
		return this.withOperator(result.value);
	};

	df.where = function (expr: Expr): DataFrame {
		return this.filter(expr);
	};
}
