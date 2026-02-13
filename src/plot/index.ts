/* PLOT MODULE
/*-----------------------------------------------------
/* Public API surface for the plot subsystem.
/* ==================================================== */

export { PlotBuilder, PlotResult } from "./plot.ts";
export { renderSvg } from "./svg.ts";
export {
	linearScale,
	ordinalScale,
	computeNiceTicks,
	computeDomain,
} from "./scales.ts";
export type {
	BarOptions,
	ChartOptions,
	ChartType,
	DataPoint,
	HistogramOptions,
	LineOptions,
	Padding,
	PlotSpec,
	ScatterOptions,
	SeriesData,
	SeriesStyle,
	AxisSpec,
} from "./types.ts";
export {
	DEFAULT_COLORS,
	DEFAULT_DIMENSIONS,
	DEFAULT_PADDING,
} from "./types.ts";
export {
	toVegaLiteSpec,
	renderVegaLite,
	type VegaLiteSpec,
} from "./adapters/vega-lite.ts";
