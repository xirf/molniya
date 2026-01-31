/* INSPECTION METHODS
/*-----------------------------------------------------
/* Tools for inspecting DataFrame schema and plan
/* ==================================================== */

import { formatSchema } from "../types/index.ts";
import type { DataFrame } from "./core.ts";

export function addInspectionMethods(df: typeof DataFrame.prototype) {
	df.printSchema = function (): void {
		console.log(formatSchema(this.currentSchema()));
	};

	df.explain = function (): string {
		if (this.operators.length === 0) {
			return "DataFrame (Materialized)";
		}

		let plan = `DataFrame [${this.columnNames.join(", ")}]\n`;
		plan += "Execution Plan:\n";

		for (let i = 0; i < this.operators.length; i++) {
			const op = this.operators[i];
			const prefix = i === this.operators.length - 1 ? "└─" : "├─";
			plan += `${prefix} ${op?.name}\n`;
		}

		return plan;
	};
}
