/* VEGA-LITE ADAPTER
/*-----------------------------------------------------
/* Converts PlotSpec to Vega-Lite specification JSON.
/* The conversion itself is zero-dependency.
/* Rendering requires optional peer deps: vega-lite + vega.
/* ==================================================== */

import type { PlotSpec, SeriesData, DataPoint } from "../types.ts";

type VegaLiteMark = "bar" | "line" | "point" | "rect";

interface VegaLiteEncoding {
	x?: Record<string, unknown>;
	y?: Record<string, unknown>;
	x2?: Record<string, unknown>;
	y2?: Record<string, unknown>;
	color?: Record<string, unknown>;
	opacity?: Record<string, unknown>;
	size?: Record<string, unknown>;
	strokeWidth?: Record<string, unknown>;
}

interface VegaLiteLayer {
	mark: Record<string, unknown>;
	encoding: VegaLiteEncoding;
}

export interface VegaLiteSpec {
	$schema: string;
	width: number;
	height: number;
	title?: string;
	padding: { top: number; right: number; bottom: number; left: number };
	data: { values: Record<string, unknown>[] };
	layer?: VegaLiteLayer[];
	mark?: Record<string, unknown>;
	encoding?: VegaLiteEncoding;
}

const VEGA_LITE_SCHEMA = "https://vega.github.io/schema/vega-lite/v5.json";

const MARK_MAP: Record<string, VegaLiteMark> = {
	bar: "bar",
	line: "line",
	scatter: "point",
	histogram: "rect",
};

function flattenSeriesData(series: SeriesData[]): Record<string, unknown>[] {
	const rows: Record<string, unknown>[] = [];
	for (const s of series) {
		for (const point of s.values) {
			rows.push({
				x: point.x,
				y: point.y,
				...(point.label ? { label: point.label } : {}),
				...(series.length > 1 ? { series: s.name } : {}),
			});
		}
	}
	return rows;
}

function xFieldType(spec: PlotSpec): string {
	if (spec.data.length === 0 || spec.data[0]!.values.length === 0) {
		return "nominal";
	}
	const firstX = spec.data[0]!.values[0]!.x;
	if (spec.type === "bar")
		return typeof firstX === "string" ? "nominal" : "quantitative";
	if (spec.type === "histogram") return "quantitative";
	return typeof firstX === "string" ? "ordinal" : "quantitative";
}

function buildBarEncoding(
	spec: PlotSpec,
	multiSeries: boolean,
): VegaLiteEncoding {
	const encoding: VegaLiteEncoding = {
		x: {
			field: "x",
			type: xFieldType(spec),
			axis: { title: spec.axes.x.label ?? "x" },
		},
		y: {
			field: "y",
			type: "quantitative",
			axis: { title: spec.axes.y.label ?? "y" },
		},
	};
	if (multiSeries) {
		encoding.color = { field: "series", type: "nominal" };
	}
	return encoding;
}

function buildLineEncoding(
	spec: PlotSpec,
	multiSeries: boolean,
): VegaLiteEncoding {
	const encoding: VegaLiteEncoding = {
		x: {
			field: "x",
			type: xFieldType(spec),
			axis: { title: spec.axes.x.label ?? "x" },
		},
		y: {
			field: "y",
			type: "quantitative",
			axis: { title: spec.axes.y.label ?? "y" },
		},
	};
	if (multiSeries) {
		encoding.color = { field: "series", type: "nominal" };
	}
	return encoding;
}

function buildScatterEncoding(
	spec: PlotSpec,
	multiSeries: boolean,
): VegaLiteEncoding {
	const encoding: VegaLiteEncoding = {
		x: {
			field: "x",
			type: "quantitative",
			axis: { title: spec.axes.x.label ?? "x" },
		},
		y: {
			field: "y",
			type: "quantitative",
			axis: { title: spec.axes.y.label ?? "y" },
		},
	};
	if (multiSeries) {
		encoding.color = { field: "series", type: "nominal" };
	}
	const style = spec.data[0]?.style;
	if (style?.pointRadius) {
		encoding.size = { value: style.pointRadius * style.pointRadius * Math.PI };
	}
	if (style?.opacity) {
		encoding.opacity = { value: style.opacity };
	}
	return encoding;
}

function buildHistogramEncoding(spec: PlotSpec): VegaLiteEncoding {
	return {
		x: {
			field: "x",
			type: "quantitative",
			axis: { title: spec.axes.x.label ?? "x" },
		},
		x2: { field: "x2" },
		y: {
			field: "y",
			type: "quantitative",
			axis: { title: spec.axes.y.label ?? "Frequency" },
		},
	};
}

function flattenHistogramData(series: SeriesData[]): Record<string, unknown>[] {
	const rows: Record<string, unknown>[] = [];
	for (const s of series) {
		const values = s.values;
		for (let i = 0; i < values.length - 1; i++) {
			const current = values[i]!;
			const next = values[i + 1]!;
			if (current.y > 0) {
				rows.push({ x: current.x, x2: next.x, y: current.y });
			}
		}
	}
	return rows;
}

function buildMark(spec: PlotSpec): Record<string, unknown> {
	const markType = MARK_MAP[spec.type] ?? "point";
	const mark: Record<string, unknown> = { type: markType };
	const style = spec.data[0]?.style;
	const color = spec.data[0]?.color;

	if (color) mark.color = color;

	switch (spec.type) {
		case "line":
			if (style?.strokeWidth) mark.strokeWidth = style.strokeWidth;
			if (style?.pointRadius && style.pointRadius > 0) mark.point = true;
			break;
		case "scatter":
			mark.filled = true;
			break;
		case "bar":
			break;
		case "histogram":
			break;
	}

	return mark;
}

function buildLineLayers(
	spec: PlotSpec,
	multiSeries: boolean,
): VegaLiteLayer[] | undefined {
	const style = spec.data[0]?.style;
	if (spec.type !== "line" || !style?.pointRadius || style.pointRadius <= 0) {
		return undefined;
	}

	const encoding = buildLineEncoding(spec, multiSeries);
	return [
		{
			mark: {
				type: "line",
				...(style.strokeWidth ? { strokeWidth: style.strokeWidth } : {}),
			},
			encoding,
		},
		{
			mark: {
				type: "point",
				filled: true,
				size: style.pointRadius * style.pointRadius * Math.PI,
			},
			encoding,
		},
	];
}

export function toVegaLiteSpec(spec: PlotSpec): VegaLiteSpec {
	const multiSeries = spec.data.length > 1;
	const isHistogram = spec.type === "histogram";
	const data = isHistogram
		? flattenHistogramData(spec.data)
		: flattenSeriesData(spec.data);

	const result: VegaLiteSpec = {
		$schema: VEGA_LITE_SCHEMA,
		width: spec.dimensions.width - spec.padding.left - spec.padding.right,
		height: spec.dimensions.height - spec.padding.top - spec.padding.bottom,
		padding: { ...spec.padding },
		data: { values: data },
	};

	if (spec.title) result.title = spec.title;

	if (spec.type === "line") {
		const layers = buildLineLayers(spec, multiSeries);
		if (layers) {
			result.layer = layers;
			return result;
		}
	}

	result.mark = buildMark(spec);

	switch (spec.type) {
		case "bar":
			result.encoding = buildBarEncoding(spec, multiSeries);
			break;
		case "line":
			result.encoding = buildLineEncoding(spec, multiSeries);
			break;
		case "scatter":
			result.encoding = buildScatterEncoding(spec, multiSeries);
			break;
		case "histogram":
			result.encoding = buildHistogramEncoding(spec);
			break;
	}

	return result;
}

export async function renderVegaLite(spec: PlotSpec): Promise<string> {
	let vegaLite: typeof import("vega-lite");
	let vega: typeof import("vega");

	try {
		vegaLite = await import("vega-lite");
	} catch {
		throw new Error(
			"vega-lite is required for renderVegaLite(). Install it: bun add vega-lite vega",
		);
	}

	try {
		vega = await import("vega");
	} catch {
		throw new Error(
			"vega is required for renderVegaLite(). Install it: bun add vega-lite vega",
		);
	}

	const vlSpec = toVegaLiteSpec(spec);
	const compiled = vegaLite.compile(vlSpec as any);
	const view = new vega.View(vega.parse(compiled.spec), { renderer: "none" });
	return await view.toSVG();
}
