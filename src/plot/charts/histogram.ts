/* HISTOGRAM BUILDER
/*-----------------------------------------------------
/* Produces a PlotSpec for histograms from a single numeric column.
/* Bins the raw values into frequency counts.
/* ==================================================== */

import {
	DEFAULT_COLORS,
	DEFAULT_DIMENSIONS,
	DEFAULT_PADDING,
	type DataPoint,
	type HistogramOptions,
	type PlotSpec,
} from "../types.ts";

const DEFAULT_BINS = 10;

export function buildHistogramSpec(
	values: number[],
	options: HistogramOptions = {},
): PlotSpec {
	const filtered = values.filter(
		(v) => v !== null && v !== undefined && !Number.isNaN(v),
	);
	if (filtered.length === 0) {
		return emptyHistogram(options);
	}

	const binCount = options.bins ?? DEFAULT_BINS;
	const { binEdges, counts } = computeBins(filtered, binCount);

	// Each DataPoint represents a bin edge with its count.
	// Consecutive pairs define [leftEdge, rightEdge) with leftEdge.y = frequency.
	const dataPoints: DataPoint[] = [];
	for (let i = 0; i < counts.length; i++) {
		dataPoints.push({ x: binEdges[i]!, y: counts[i]! });
	}
	// Final edge (right boundary of last bin, y=0 for rendering)
	dataPoints.push({ x: binEdges[binEdges.length - 1]!, y: 0 });

	return {
		type: "histogram",
		data: [
			{
				name: "default",
				values: dataPoints,
				color: options.color ?? DEFAULT_COLORS[0],
			},
		],
		axes: {
			x: { label: undefined },
			y: { label: "Frequency" },
		},
		dimensions: {
			width: options.width ?? DEFAULT_DIMENSIONS.width,
			height: options.height ?? DEFAULT_DIMENSIONS.height,
		},
		title: options.title,
		padding: { ...DEFAULT_PADDING, ...options.padding },
	};
}

function computeBins(
	values: number[],
	binCount: number,
): { binEdges: number[]; counts: number[] } {
	let min = values[0]!;
	let max = values[0]!;
	for (let i = 1; i < values.length; i++) {
		if (values[i]! < min) min = values[i]!;
		if (values[i]! > max) max = values[i]!;
	}

	// Avoid zero-width bins
	if (min === max) {
		min = min - 1;
		max = max + 1;
	}

	const binWidth = (max - min) / binCount;
	const binEdges: number[] = [];
	for (let i = 0; i <= binCount; i++) {
		binEdges.push(min + i * binWidth);
	}

	const counts = new Array<number>(binCount).fill(0);
	for (const v of values) {
		let idx = Math.floor((v - min) / binWidth);
		if (idx >= binCount) idx = binCount - 1;
		if (idx < 0) idx = 0;
		counts[idx]!++;
	}

	return { binEdges, counts };
}

function emptyHistogram(options: HistogramOptions): PlotSpec {
	return {
		type: "histogram",
		data: [
			{
				name: "default",
				values: [],
				color: options.color ?? DEFAULT_COLORS[0],
			},
		],
		axes: { x: { label: undefined }, y: { label: "Frequency" } },
		dimensions: {
			width: options.width ?? DEFAULT_DIMENSIONS.width,
			height: options.height ?? DEFAULT_DIMENSIONS.height,
		},
		title: options.title,
		padding: { ...DEFAULT_PADDING, ...options.padding },
	};
}
