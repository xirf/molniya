/* SVG RENDERER
/*-----------------------------------------------------
/* Zero-dep SVG string renderer.
/* Takes a PlotSpec and returns a complete SVG string.
/* ==================================================== */

import {
	computeNiceTicks,
	linearScale,
	ordinalScale,
	type LinearScale,
	type OrdinalScale,
} from "./scales.ts";
import type { DataPoint, PlotSpec, SeriesData } from "./types.ts";

const FONT_FAMILY = "system-ui, -apple-system, sans-serif";
const FONT_SIZE_LABEL = 12;
const FONT_SIZE_TITLE = 16;
const FONT_SIZE_TICK = 10;
const GRIDLINE_COLOR = "#e0e0e0";
const AXIS_COLOR = "#333";
const TEXT_COLOR = "#333";

export function renderSvg(spec: PlotSpec): string {
	const { dimensions, padding } = spec;
	const plotWidth = dimensions.width - padding.left - padding.right;
	const plotHeight = dimensions.height - padding.top - padding.bottom;

	const parts: string[] = [];
	parts.push(svgOpen(dimensions.width, dimensions.height));

	if (spec.title) {
		parts.push(renderTitle(spec.title, dimensions.width, padding.top));
	}

	// Compute scales from data
	const { xScale, yScale, xIsOrdinal } = buildScales(
		spec,
		plotWidth,
		plotHeight,
	);

	// Clip group for the plot area
	parts.push(`<g transform="translate(${padding.left},${padding.top})">`);

	// Gridlines
	parts.push(renderYGridlines(yScale as LinearScale, plotWidth, plotHeight));

	// Data
	switch (spec.type) {
		case "bar":
			parts.push(renderBars(spec.data, xScale, yScale, plotHeight));
			break;
		case "line":
			parts.push(renderLines(spec.data, xScale, yScale));
			break;
		case "scatter":
			parts.push(renderScatter(spec.data, xScale, yScale));
			break;
		case "histogram":
			parts.push(
				renderHistogramBars(
					spec.data,
					xScale as LinearScale,
					yScale,
					plotHeight,
				),
			);
			break;
	}

	// Axes
	parts.push(renderXAxis(xScale, xIsOrdinal, plotHeight, spec.axes.x.label));
	parts.push(renderYAxis(yScale as LinearScale, plotHeight, spec.axes.y.label));

	parts.push("</g>");
	parts.push("</svg>");
	return parts.join("\n");
}

function svgOpen(width: number, height: number): string {
	return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" font-family="${FONT_FAMILY}">`;
}

function renderTitle(
	title: string,
	totalWidth: number,
	paddingTop: number,
): string {
	const x = totalWidth / 2;
	const y = paddingTop / 2;
	return `<text x="${x}" y="${y}" text-anchor="middle" font-size="${FONT_SIZE_TITLE}" font-weight="600" fill="${TEXT_COLOR}">${escapeXml(title)}</text>`;
}

// --- Scale construction ---

interface ScaleSet {
	xScale: LinearScale | OrdinalScale;
	yScale: LinearScale;
	xIsOrdinal: boolean;
}

function buildScales(
	spec: PlotSpec,
	plotWidth: number,
	plotHeight: number,
): ScaleSet {
	const allPoints = spec.data.flatMap((s) => s.values);

	const xIsOrdinal =
		allPoints.length > 0 && typeof allPoints[0]!.x === "string";
	const yValues = allPoints.map((p) => p.y);
	const yDomain = spec.axes.y.domain ?? computeYDomain(yValues);
	const yScale = linearScale(yDomain, [plotHeight, 0]);

	let xScale: LinearScale | OrdinalScale;
	if (xIsOrdinal) {
		const categories = uniqueStrings(allPoints.map((p) => String(p.x)));
		xScale = ordinalScale(categories, [0, plotWidth]);
	} else {
		const xValues = allPoints.map((p) => p.x as number);
		const xDomain =
			spec.axes.x.domain ?? computeXDomain(xValues, spec.type === "histogram");
		xScale = linearScale(xDomain, [0, plotWidth]);
	}

	return { xScale, yScale, xIsOrdinal };
}

function computeYDomain(values: number[]): [number, number] {
	if (values.length === 0) return [0, 1];
	let min = values[0]!;
	let max = values[0]!;
	for (let i = 1; i < values.length; i++) {
		if (values[i]! < min) min = values[i]!;
		if (values[i]! > max) max = values[i]!;
	}
	// Always include 0 for bar/histogram charts
	if (min > 0) min = 0;
	const ticks = computeNiceTicks(min, max, 5);
	return [ticks[0]!, ticks[ticks.length - 1]!];
}

function computeXDomain(
	values: number[],
	includeZero: boolean,
): [number, number] {
	if (values.length === 0) return [0, 1];
	let min = values[0]!;
	let max = values[0]!;
	for (let i = 1; i < values.length; i++) {
		if (values[i]! < min) min = values[i]!;
		if (values[i]! > max) max = values[i]!;
	}
	if (includeZero && min > 0) min = 0;
	const ticks = computeNiceTicks(min, max, 5);
	return [ticks[0]!, ticks[ticks.length - 1]!];
}

function uniqueStrings(arr: string[]): string[] {
	const seen = new Set<string>();
	const result: string[] = [];
	for (const s of arr) {
		if (!seen.has(s)) {
			seen.add(s);
			result.push(s);
		}
	}
	return result;
}

// --- Gridlines ---

function renderYGridlines(
	yScale: LinearScale,
	plotWidth: number,
	plotHeight: number,
): string {
	const ticks = computeNiceTicks(yScale.domain[0], yScale.domain[1], 5);
	const lines: string[] = [];
	for (const tick of ticks) {
		const y = yScale(tick);
		if (y >= 0 && y <= plotHeight) {
			lines.push(
				`<line x1="0" y1="${y}" x2="${plotWidth}" y2="${y}" stroke="${GRIDLINE_COLOR}" stroke-width="1"/>`,
			);
		}
	}
	return lines.join("\n");
}

// --- Data renderers ---

function renderBars(
	series: SeriesData[],
	xScale: LinearScale | OrdinalScale,
	yScale: LinearScale,
	plotHeight: number,
): string {
	const parts: string[] = [];
	const ordinal = "bandwidth" in xScale;

	for (const s of series) {
		const color = s.color ?? "#4e79a7";
		for (const point of s.values) {
			const y = yScale(point.y);
			const barHeight = plotHeight - y;
			if (ordinal) {
				const cx = (xScale as OrdinalScale)(String(point.x));
				const bw = (xScale as OrdinalScale).bandwidth * 0.7;
				const x = cx - bw / 2;
				parts.push(
					`<rect x="${x}" y="${y}" width="${bw}" height="${barHeight}" fill="${color}" rx="2"/>`,
				);
			} else {
				const x = (xScale as LinearScale)(point.x as number);
				parts.push(
					`<rect x="${x - 4}" y="${y}" width="8" height="${barHeight}" fill="${color}" rx="2"/>`,
				);
			}
		}
	}
	return parts.join("\n");
}

function renderLines(
	series: SeriesData[],
	xScale: LinearScale | OrdinalScale,
	yScale: LinearScale,
): string {
	const parts: string[] = [];
	for (const s of series) {
		const color = s.color ?? "#4e79a7";
		const strokeWidth = s.style?.strokeWidth ?? 2;
		const showPoints =
			s.style?.pointRadius !== undefined && s.style.pointRadius > 0;

		if (s.values.length === 0) continue;

		const pathPoints = s.values.map((p) => {
			const x =
				typeof p.x === "string"
					? (xScale as OrdinalScale)(p.x)
					: (xScale as LinearScale)(p.x);
			return `${x},${yScale(p.y)}`;
		});

		parts.push(
			`<polyline points="${pathPoints.join(" ")}" fill="none" stroke="${color}" stroke-width="${strokeWidth}" stroke-linejoin="round" stroke-linecap="round"/>`,
		);

		if (showPoints) {
			const radius = s.style!.pointRadius!;
			for (const p of s.values) {
				const x =
					typeof p.x === "string"
						? (xScale as OrdinalScale)(p.x)
						: (xScale as LinearScale)(p.x);
				parts.push(
					`<circle cx="${x}" cy="${yScale(p.y)}" r="${radius}" fill="${color}"/>`,
				);
			}
		}
	}
	return parts.join("\n");
}

function renderScatter(
	series: SeriesData[],
	xScale: LinearScale | OrdinalScale,
	yScale: LinearScale,
): string {
	const parts: string[] = [];
	for (const s of series) {
		const color = s.color ?? "#4e79a7";
		const radius = s.style?.pointRadius ?? 4;
		const opacity = s.style?.opacity ?? 0.7;
		for (const p of s.values) {
			const x =
				typeof p.x === "string"
					? (xScale as OrdinalScale)(p.x)
					: (xScale as LinearScale)(p.x);
			parts.push(
				`<circle cx="${x}" cy="${yScale(p.y)}" r="${radius}" fill="${color}" opacity="${opacity}"/>`,
			);
		}
	}
	return parts.join("\n");
}

function renderHistogramBars(
	series: SeriesData[],
	xScale: LinearScale,
	yScale: LinearScale,
	plotHeight: number,
): string {
	const parts: string[] = [];
	for (const s of series) {
		const color = s.color ?? "#4e79a7";
		const values = s.values;
		if (values.length < 2) continue;

		for (let i = 0; i < values.length - 1; i++) {
			const leftEdge = values[i]! as DataPoint;
			const rightEdge = values[i + 1]! as DataPoint;
			const x1 = xScale(leftEdge.x as number);
			const x2 = xScale(rightEdge.x as number);
			const y = yScale(leftEdge.y);
			const barHeight = plotHeight - y;
			parts.push(
				`<rect x="${x1}" y="${y}" width="${x2 - x1}" height="${barHeight}" fill="${color}" stroke="white" stroke-width="1"/>`,
			);
		}
	}
	return parts.join("\n");
}

// --- Axes ---

function renderXAxis(
	xScale: LinearScale | OrdinalScale,
	isOrdinal: boolean,
	plotHeight: number,
	label?: string,
): string {
	const parts: string[] = [];
	// Axis line
	const xEnd = xScale.range[1];
	parts.push(
		`<line x1="0" y1="${plotHeight}" x2="${xEnd}" y2="${plotHeight}" stroke="${AXIS_COLOR}" stroke-width="1"/>`,
	);

	if (isOrdinal) {
		const scale = xScale as OrdinalScale;
		for (const cat of scale.domain) {
			const x = scale(cat);
			parts.push(
				`<text x="${x}" y="${plotHeight + 20}" text-anchor="middle" font-size="${FONT_SIZE_TICK}" fill="${TEXT_COLOR}">${escapeXml(cat)}</text>`,
			);
		}
	} else {
		const scale = xScale as LinearScale;
		const ticks = computeNiceTicks(scale.domain[0], scale.domain[1], 5);
		for (const tick of ticks) {
			const x = scale(tick);
			parts.push(
				`<line x1="${x}" y1="${plotHeight}" x2="${x}" y2="${plotHeight + 5}" stroke="${AXIS_COLOR}"/>`,
			);
			parts.push(
				`<text x="${x}" y="${plotHeight + 18}" text-anchor="middle" font-size="${FONT_SIZE_TICK}" fill="${TEXT_COLOR}">${formatTickValue(tick)}</text>`,
			);
		}
	}

	if (label) {
		const midX = (xScale.range[0] + xScale.range[1]) / 2;
		parts.push(
			`<text x="${midX}" y="${plotHeight + 45}" text-anchor="middle" font-size="${FONT_SIZE_LABEL}" fill="${TEXT_COLOR}">${escapeXml(label)}</text>`,
		);
	}

	return parts.join("\n");
}

function renderYAxis(
	yScale: LinearScale,
	plotHeight: number,
	label?: string,
): string {
	const parts: string[] = [];
	// Axis line
	parts.push(
		`<line x1="0" y1="0" x2="0" y2="${plotHeight}" stroke="${AXIS_COLOR}" stroke-width="1"/>`,
	);

	const ticks = computeNiceTicks(yScale.domain[0], yScale.domain[1], 5);
	for (const tick of ticks) {
		const y = yScale(tick);
		if (y >= 0 && y <= plotHeight) {
			parts.push(
				`<line x1="-5" y1="${y}" x2="0" y2="${y}" stroke="${AXIS_COLOR}"/>`,
			);
			parts.push(
				`<text x="-10" y="${y + 4}" text-anchor="end" font-size="${FONT_SIZE_TICK}" fill="${TEXT_COLOR}">${formatTickValue(tick)}</text>`,
			);
		}
	}

	if (label) {
		const midY = plotHeight / 2;
		parts.push(
			`<text x="${-50}" y="${midY}" text-anchor="middle" font-size="${FONT_SIZE_LABEL}" fill="${TEXT_COLOR}" transform="rotate(-90, -50, ${midY})">${escapeXml(label)}</text>`,
		);
	}

	return parts.join("\n");
}

// --- Helpers ---

function formatTickValue(value: number): string {
	if (Math.abs(value) >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
	if (Math.abs(value) >= 1_000) return `${(value / 1_000).toFixed(1)}k`;
	if (Number.isInteger(value)) return String(value);
	return value.toFixed(2);
}

function escapeXml(text: string): string {
	return text
		.replace(/&/g, "&amp;")
		.replace(/</g, "&lt;")
		.replace(/>/g, "&gt;")
		.replace(/"/g, "&quot;");
}
