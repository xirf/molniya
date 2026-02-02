/* LIMITING METHODS
/*-----------------------------------------------------
/* Limit and slice row counts
/* ==================================================== */

import { limit } from "../ops/index.ts";
import type { DataFrame } from "./core.ts";

export function addLimitingMethods(df: typeof DataFrame.prototype) {
	df.limit = function (count: number): DataFrame {
		return this.withOperator(limit(this.currentSchema(), count));
	};

	df.head = function (count: number = 5): DataFrame {
		return this.limit(count);
	};

	df.slice = function (start: number, count: number): DataFrame {
		return this.withOperator(limit(this.currentSchema(), count, start));
	};

	df.tail = async function (count: number = 5): Promise<DataFrame> {
		// Get total count first
		const total = await this.count();
		const start = Math.max(0, total - count);
		return this.slice(start, count);
	};
}
