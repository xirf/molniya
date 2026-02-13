/* SCATTER CHART BUILDER
/*-----------------------------------------------------
/* Produces a PlotSpec for scatter plots from columnar data.
/* ==================================================== */

import {
	DEFAULT_COLORS,
	DEFAULT_DIMENSIONS,
	DEFAULT_PADDING,
	type DataPoint,
	type PlotSpec,
	type ScatterOptions,
} from "../types.ts";

export function buildScatterSpec(
	xValues: number[],
	yValues: number[],
	options: ScatterOptions = {},
): PlotSpec {
	const values: DataPoint[] = [];
	const count = Math.min(xValues.length, yValues.length);
	for (let i = 0; i < count; i++) {
		values.push({ x: xValues[i]!, y: yValues[i]! });
	}

	return {
		type: "scatter",
		data: [
			{
				name: "default",
				values,
				color: options.color ?? DEFAULT_COLORS[0],
				style: {
					pointRadius: options.pointRadius ?? 4,
					opacity: 0.7,
				},
			},
		],
		axes: {
			x: { label: undefined },
			y: { label: undefined },
		},
		dimensions: {
			width: options.width ?? DEFAULT_DIMENSIONS.width,
			height: options.height ?? DEFAULT_DIMENSIONS.height,
		},
		title: options.title,
		padding: { ...DEFAULT_PADDING, ...options.padding },
	};
}
