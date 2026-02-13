/* PLOT BUILDER
/*-----------------------------------------------------
/* Fluent API for constructing charts from columnar data.
/* Returns PlotResult which can render to SVG or raw JSON.
/* ==================================================== */

import {
	toVegaLiteSpec,
	renderVegaLite,
	type VegaLiteSpec,
} from "./adapters/vega-lite.ts";
import { buildBarSpec } from "./charts/bar.ts";
import { buildHistogramSpec } from "./charts/histogram.ts";
import { buildLineSpec } from "./charts/line.ts";
import { buildScatterSpec } from "./charts/scatter.ts";
import { renderSvg } from "./svg.ts";
import type {
	BarOptions,
	HistogramOptions,
	LineOptions,
	PlotSpec,
	ScatterOptions,
} from "./types.ts";

export class PlotResult {
	constructor(private readonly spec: PlotSpec) {}

	toJSON(): PlotSpec {
		return this.spec;
	}

	toSVG(): string {
		return renderSvg(this.spec);
	}

	toVegaLite(): VegaLiteSpec {
		return toVegaLiteSpec(this.spec);
	}

	async toVegaLiteSVG(): Promise<string> {
		return renderVegaLite(this.spec);
	}

	async toFile(path: string): Promise<void> {
		const svg = this.toSVG();
		await Bun.write(path, svg);
	}
}

export class PlotBuilder {
	constructor(private readonly columns: Record<string, unknown>) {}

	bar(x: string, y: string, options?: BarOptions): PlotResult {
		const xValues = this.getColumn(x);
		const yValues = this.getNumericColumn(y);
		const spec = buildBarSpec(xValues as (string | number)[], yValues, options);
		spec.axes.x.label = x;
		spec.axes.y.label = y;
		return new PlotResult(spec);
	}

	line(x: string, y: string, options?: LineOptions): PlotResult {
		const xValues = this.getColumn(x);
		const yValues = this.getNumericColumn(y);
		const spec = buildLineSpec(
			xValues as (string | number)[],
			yValues,
			options,
		);
		spec.axes.x.label = x;
		spec.axes.y.label = y;
		return new PlotResult(spec);
	}

	scatter(x: string, y: string, options?: ScatterOptions): PlotResult {
		const xValues = this.getNumericColumn(x);
		const yValues = this.getNumericColumn(y);
		const spec = buildScatterSpec(xValues, yValues, options);
		spec.axes.x.label = x;
		spec.axes.y.label = y;
		return new PlotResult(spec);
	}

	histogram(column: string, options?: HistogramOptions): PlotResult {
		const values = this.getNumericColumn(column);
		const spec = buildHistogramSpec(values, options);
		spec.axes.x.label = column;
		return new PlotResult(spec);
	}

	private getColumn(name: string): unknown[] {
		const col = this.columns[name];
		if (!col) throw new Error(`Column "${name}" not found`);
		return Array.isArray(col) ? col : Array.from(col as ArrayLike<unknown>);
	}

	private getNumericColumn(name: string): number[] {
		const raw = this.getColumn(name);
		const result = new Array<number>(raw.length);
		for (let i = 0; i < raw.length; i++) {
			const v = raw[i];
			result[i] = typeof v === "number" ? v : Number(v);
		}
		return result;
	}
}
