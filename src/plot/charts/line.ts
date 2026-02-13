/* LINE CHART BUILDER
/*-----------------------------------------------------
/* Produces a PlotSpec for line charts from columnar data.
/* ==================================================== */

import {
	DEFAULT_COLORS,
	DEFAULT_DIMENSIONS,
	DEFAULT_PADDING,
	type DataPoint,
	type LineOptions,
	type PlotSpec,
} from "../types.ts";

export function buildLineSpec(
	xValues: (string | number)[],
	yValues: number[],
	options: LineOptions = {},
): PlotSpec {
	const values: DataPoint[] = [];
	const count = Math.min(xValues.length, yValues.length);
	for (let i = 0; i < count; i++) {
		values.push({ x: xValues[i]!, y: yValues[i]! });
	}

	return {
		type: "line",
		data: [
			{
				name: "default",
				values,
				color: options.color ?? DEFAULT_COLORS[0],
				style: {
					strokeWidth: options.strokeWidth ?? 2,
					pointRadius: options.showPoints ? 3 : 0,
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
