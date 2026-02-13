/* PLOTTING METHODS
/*-----------------------------------------------------
/* DataFrame mixin for chart generation
/* ==================================================== */

import { PlotBuilder } from "../plot/plot.ts";
import type { DataFrame } from "./core.ts";

export function addPlottingMethods(df: typeof DataFrame.prototype) {
	df.plot = async function (): Promise<PlotBuilder> {
		const columns = await this.toColumns();
		return new PlotBuilder(columns);
	};
}
