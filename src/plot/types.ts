/* PLOT TYPES
/*-----------------------------------------------------
/* Neutral intermediate representation for chart specifications.
/* All chart builders produce PlotSpec, all renderers consume it.
/* ==================================================== */

export type ChartType = "bar" | "line" | "scatter" | "histogram";

export interface DataPoint {
	x: number | string;
	y: number;
	label?: string;
}

export interface SeriesStyle {
	strokeWidth?: number;
	opacity?: number;
	dashArray?: string;
	pointRadius?: number;
}

export interface SeriesData {
	name: string;
	values: DataPoint[];
	color?: string;
	style?: SeriesStyle;
}

export interface AxisSpec {
	label?: string;
	ticks?: number;
	domain?: [number, number];
}

export interface Padding {
	top: number;
	right: number;
	bottom: number;
	left: number;
}

export interface PlotSpec {
	type: ChartType;
	data: SeriesData[];
	axes: { x: AxisSpec; y: AxisSpec };
	dimensions: { width: number; height: number };
	title?: string;
	padding: Padding;
}

export interface ChartOptions {
	title?: string;
	width?: number;
	height?: number;
	padding?: Partial<Padding>;
	color?: string;
}

export interface BarOptions extends ChartOptions {
	horizontal?: boolean;
}

export interface LineOptions extends ChartOptions {
	strokeWidth?: number;
	showPoints?: boolean;
}

export interface ScatterOptions extends ChartOptions {
	pointRadius?: number;
}

export interface HistogramOptions extends ChartOptions {
	bins?: number;
}

export const DEFAULT_DIMENSIONS = { width: 800, height: 500 } as const;
export const DEFAULT_PADDING: Padding = {
	top: 40,
	right: 30,
	bottom: 60,
	left: 70,
} as const;
export const DEFAULT_COLORS = [
	"#4e79a7",
	"#f28e2b",
	"#e15759",
	"#76b7b2",
	"#59a14f",
	"#edc948",
	"#b07aa1",
	"#ff9da7",
	"#9c755f",
	"#bab0ac",
] as const;
