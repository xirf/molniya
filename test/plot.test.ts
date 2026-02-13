import { describe, expect, it } from "bun:test";
import { fromRecords, DType, col } from "../src/index.ts";
import {
	linearScale,
	ordinalScale,
	computeNiceTicks,
	computeDomain,
} from "../src/plot/scales.ts";
import { buildBarSpec } from "../src/plot/charts/bar.ts";
import { buildLineSpec } from "../src/plot/charts/line.ts";
import { buildScatterSpec } from "../src/plot/charts/scatter.ts";
import { buildHistogramSpec } from "../src/plot/charts/histogram.ts";
import { renderSvg } from "../src/plot/svg.ts";
import { PlotBuilder, PlotResult } from "../src/plot/plot.ts";
import { toVegaLiteSpec } from "../src/plot/adapters/vega-lite.ts";
import { existsSync, unlinkSync } from "node:fs";
import type { PlotSpec } from "../src/plot/types.ts";

/* SCALES
/*----------------------------------------------------- */

describe("linearScale", () => {
	it("should map domain values to range values", () => {
		const scale = linearScale([0, 100], [0, 500]);
		expect(scale(0)).toBe(0);
		expect(scale(50)).toBe(250);
		expect(scale(100)).toBe(500);
	});

	it("should handle inverted ranges (top=height, bottom=0)", () => {
		const scale = linearScale([0, 100], [400, 0]);
		expect(scale(0)).toBe(400);
		expect(scale(100)).toBe(0);
		expect(scale(50)).toBe(200);
	});

	it("should return midpoint when domain span is zero", () => {
		const scale = linearScale([5, 5], [0, 100]);
		expect(scale(5)).toBe(50);
	});

	it("should preserve domain and range on the function object", () => {
		const scale = linearScale([10, 20], [100, 200]);
		expect(scale.domain).toEqual([10, 20]);
		expect(scale.range).toEqual([100, 200]);
	});
});

describe("ordinalScale", () => {
	it("should map categories to band centers", () => {
		const scale = ordinalScale(["A", "B", "C"], [0, 300]);
		expect(scale("A")).toBe(50); // 0 + 100/2
		expect(scale("B")).toBe(150); // 100 + 100/2
		expect(scale("C")).toBe(250); // 200 + 100/2
	});

	it("should compute correct bandwidth", () => {
		const scale = ordinalScale(["X", "Y"], [0, 200]);
		expect(scale.bandwidth).toBe(100);
	});

	it("should return range start for unknown categories", () => {
		const scale = ordinalScale(["A"], [50, 150]);
		expect(scale("unknown")).toBe(50);
	});

	it("should handle empty domain", () => {
		const scale = ordinalScale([], [0, 100]);
		expect(scale.bandwidth).toBe(0);
	});
});

describe("computeNiceTicks", () => {
	it("should produce human-readable tick values", () => {
		const ticks = computeNiceTicks(0, 100, 5);
		expect(ticks[0]).toBe(0);
		expect(ticks[ticks.length - 1]).toBe(100);
		// All ticks should be round numbers
		for (const t of ticks) {
			expect(Number.isInteger(t)).toBe(true);
		}
	});

	it("should return single-element array when min equals max", () => {
		const ticks = computeNiceTicks(42, 42, 5);
		expect(ticks).toEqual([42]);
	});

	it("should handle fractional ranges", () => {
		const ticks = computeNiceTicks(0, 1, 5);
		expect(ticks.length).toBeGreaterThan(1);
		expect(ticks[0]).toBeLessThanOrEqual(0);
		expect(ticks[ticks.length - 1]).toBeGreaterThanOrEqual(1);
	});
});

describe("computeDomain", () => {
	it("should return [0, 1] for empty array", () => {
		expect(computeDomain([])).toEqual([0, 1]);
	});

	it("should expand single-value domains", () => {
		const [min, max] = computeDomain([5]);
		expect(min).toBeLessThan(5);
		expect(max).toBeGreaterThan(5);
	});

	it("should return nice-extended domain by default", () => {
		const [min, max] = computeDomain([3, 7, 12, 45]);
		expect(min).toBeLessThanOrEqual(3);
		expect(max).toBeGreaterThanOrEqual(45);
	});
});

/* CHART BUILDERS
/*----------------------------------------------------- */

describe("buildBarSpec", () => {
	it("should produce valid PlotSpec with correct type and data", () => {
		const spec = buildBarSpec(["Jan", "Feb", "Mar"], [100, 150, 120]);
		expect(spec.type).toBe("bar");
		expect(spec.data).toHaveLength(1);
		expect(spec.data[0]!.values).toHaveLength(3);
		expect(spec.data[0]!.values[0]).toEqual({ x: "Jan", y: 100 });
		expect(spec.data[0]!.values[2]).toEqual({ x: "Mar", y: 120 });
	});

	it("should apply title and custom dimensions from options", () => {
		const spec = buildBarSpec(["a"], [1], {
			title: "Test",
			width: 400,
			height: 300,
		});
		expect(spec.title).toBe("Test");
		expect(spec.dimensions.width).toBe(400);
		expect(spec.dimensions.height).toBe(300);
	});

	it("should handle mismatched array lengths (uses minimum)", () => {
		const spec = buildBarSpec(["a", "b", "c"], [10, 20]);
		expect(spec.data[0]!.values).toHaveLength(2);
	});
});

describe("buildLineSpec", () => {
	it("should produce line spec with correct data", () => {
		const spec = buildLineSpec([1, 2, 3], [10, 20, 30]);
		expect(spec.type).toBe("line");
		expect(spec.data[0]!.values).toHaveLength(3);
	});

	it("should set strokeWidth and pointRadius from options", () => {
		const spec = buildLineSpec([1], [1], { strokeWidth: 4, showPoints: true });
		expect(spec.data[0]!.style!.strokeWidth).toBe(4);
		expect(spec.data[0]!.style!.pointRadius).toBe(3);
	});

	it("should default pointRadius to 0 when showPoints is false", () => {
		const spec = buildLineSpec([1], [1], { showPoints: false });
		expect(spec.data[0]!.style!.pointRadius).toBe(0);
	});
});

describe("buildScatterSpec", () => {
	it("should produce scatter spec with correct data", () => {
		const spec = buildScatterSpec([1, 2, 3], [4, 5, 6]);
		expect(spec.type).toBe("scatter");
		expect(spec.data[0]!.values).toHaveLength(3);
		expect(spec.data[0]!.style!.pointRadius).toBe(4);
		expect(spec.data[0]!.style!.opacity).toBe(0.7);
	});

	it("should apply custom pointRadius", () => {
		const spec = buildScatterSpec([1], [1], { pointRadius: 8 });
		expect(spec.data[0]!.style!.pointRadius).toBe(8);
	});
});

describe("buildHistogramSpec", () => {
	it("should bin values into frequency counts", () => {
		const values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
		const spec = buildHistogramSpec(values, { bins: 5 });
		expect(spec.type).toBe("histogram");
		expect(spec.data[0]!.values.length).toBeGreaterThan(1);
		// Sum of frequencies should equal input count
		const totalFreq = spec.data[0]!.values.slice(0, -1) // last point is the trailing edge
			.reduce((sum, p) => sum + p.y, 0);
		expect(totalFreq).toBe(10);
	});

	it("should handle empty values", () => {
		const spec = buildHistogramSpec([]);
		expect(spec.type).toBe("histogram");
		expect(spec.data[0]!.values).toHaveLength(0);
	});

	it("should handle single value", () => {
		const spec = buildHistogramSpec([42], { bins: 5 });
		expect(spec.data[0]!.values.length).toBeGreaterThan(0);
	});

	it("should filter NaN values", () => {
		const spec = buildHistogramSpec([1, NaN, 2, NaN, 3], { bins: 3 });
		const totalFreq = spec.data[0]!.values.slice(0, -1).reduce(
			(sum, p) => sum + p.y,
			0,
		);
		expect(totalFreq).toBe(3);
	});
});

/* SVG RENDERER
/*----------------------------------------------------- */

describe("renderSvg", () => {
	function makeBarSpec(): PlotSpec {
		return buildBarSpec(["A", "B", "C"], [10, 20, 30], { title: "Test Chart" });
	}

	it("should return a valid SVG string", () => {
		const svg = renderSvg(makeBarSpec());
		expect(svg).toStartWith("<svg");
		expect(svg).toEndWith("</svg>");
		expect(svg).toContain('xmlns="http://www.w3.org/2000/svg"');
	});

	it("should include the title text element", () => {
		const svg = renderSvg(makeBarSpec());
		expect(svg).toContain("Test Chart");
		expect(svg).toContain("<text");
	});

	it("should include rect elements for bar charts", () => {
		const svg = renderSvg(makeBarSpec());
		expect(svg).toContain("<rect");
	});

	it("should include polyline for line charts", () => {
		const spec = buildLineSpec([1, 2, 3], [10, 20, 30]);
		const svg = renderSvg(spec);
		expect(svg).toContain("<polyline");
	});

	it("should include circle elements for scatter charts", () => {
		const spec = buildScatterSpec([1, 2, 3], [4, 5, 6]);
		const svg = renderSvg(spec);
		expect(svg).toContain("<circle");
	});

	it("should include rect elements for histogram charts", () => {
		const spec = buildHistogramSpec([1, 2, 3, 4, 5], { bins: 3 });
		const svg = renderSvg(spec);
		expect(svg).toContain("<rect");
	});

	it("should escape XML special characters in titles", () => {
		const spec = buildBarSpec(["a"], [1], { title: "Sales <2024> & Revenue" });
		const svg = renderSvg(spec);
		expect(svg).toContain("Sales &lt;2024&gt; &amp; Revenue");
	});

	it("should include axis lines", () => {
		const svg = renderSvg(makeBarSpec());
		expect(svg).toContain("<line");
	});

	it("should include gridlines", () => {
		const svg = renderSvg(makeBarSpec());
		// Gridlines use the GRIDLINE_COLOR #e0e0e0
		expect(svg).toContain("#e0e0e0");
	});
});

/* PLOT BUILDER & PLOT RESULT
/*----------------------------------------------------- */

describe("PlotResult", () => {
	it("toJSON should return the raw PlotSpec", () => {
		const spec = buildBarSpec(["x"], [1]);
		const result = new PlotResult(spec);
		expect(result.toJSON()).toBe(spec);
	});

	it("toSVG should return a valid SVG string", () => {
		const spec = buildBarSpec(["x"], [1]);
		const result = new PlotResult(spec);
		const svg = result.toSVG();
		expect(svg).toStartWith("<svg");
		expect(svg).toEndWith("</svg>");
	});

	it("toFile should write SVG to disk", async () => {
		const spec = buildBarSpec(["a"], [10], { title: "FileTest" });
		const result = new PlotResult(spec);
		const path = "/tmp/molniya-test-chart.svg";
		try {
			await result.toFile(path);
			expect(existsSync(path)).toBe(true);
			const content = await Bun.file(path).text();
			expect(content).toStartWith("<svg");
			expect(content).toContain("FileTest");
		} finally {
			if (existsSync(path)) unlinkSync(path);
		}
	});
});

describe("PlotBuilder", () => {
	const columns = {
		month: ["Jan", "Feb", "Mar"],
		sales: [100, 150, 120],
		clicks: [500, 600, 550],
	};
	const builder = new PlotBuilder(columns);

	it("bar() should produce a bar chart PlotResult", () => {
		const result = builder.bar("month", "sales");
		const spec = result.toJSON();
		expect(spec.type).toBe("bar");
		expect(spec.axes.x.label).toBe("month");
		expect(spec.axes.y.label).toBe("sales");
		expect(spec.data[0]!.values).toHaveLength(3);
	});

	it("line() should produce a line chart PlotResult", () => {
		const result = builder.line("month", "sales");
		const spec = result.toJSON();
		expect(spec.type).toBe("line");
		expect(spec.data[0]!.values[0]).toEqual({ x: "Jan", y: 100 });
	});

	it("scatter() should produce a scatter chart PlotResult", () => {
		const result = builder.scatter("sales", "clicks");
		const spec = result.toJSON();
		expect(spec.type).toBe("scatter");
		expect(spec.data[0]!.values).toHaveLength(3);
	});

	it("histogram() should produce a histogram PlotResult", () => {
		const result = builder.histogram("sales", { bins: 3 });
		const spec = result.toJSON();
		expect(spec.type).toBe("histogram");
		expect(spec.axes.x.label).toBe("sales");
	});

	it("should throw on nonexistent column", () => {
		expect(() => builder.bar("nonexistent", "sales")).toThrow(
			'Column "nonexistent" not found',
		);
	});

	it("should pass options through to the spec", () => {
		const result = builder.bar("month", "sales", {
			title: "Monthly",
			width: 600,
		});
		const spec = result.toJSON();
		expect(spec.title).toBe("Monthly");
		expect(spec.dimensions.width).toBe(600);
	});
});

/* DATAFRAME INTEGRATION
/*----------------------------------------------------- */

describe("DataFrame.plot()", () => {
	it("should return a PlotBuilder from a DataFrame", async () => {
		const df = fromRecords(
			[
				{ month: "Jan", sales: 100 },
				{ month: "Feb", sales: 150 },
				{ month: "Mar", sales: 120 },
			],
			{ month: DType.string, sales: DType.int32 },
		);

		const plot = await df.plot();
		expect(plot).toBeInstanceOf(PlotBuilder);

		const result = plot.bar("month", "sales", { title: "Monthly Sales" });
		const svg = result.toSVG();
		expect(svg).toStartWith("<svg");
		expect(svg).toContain("Monthly Sales");
		expect(svg).toContain("<rect");
	});

	it("should work with numeric x-axis", async () => {
		const df = fromRecords(
			[
				{ x: 1, y: 10 },
				{ x: 2, y: 20 },
				{ x: 3, y: 15 },
			],
			{ x: DType.int32, y: DType.int32 },
		);

		const plot = await df.plot();
		const result = plot.line("x", "y");
		const svg = result.toSVG();
		expect(svg).toContain("<polyline");
	});

	it("should work with filtered DataFrame", async () => {
		const df = fromRecords(
			[
				{ category: "A", value: 10 },
				{ category: "B", value: 20 },
				{ category: "A", value: 30 },
				{ category: "B", value: 40 },
			],
			{ category: DType.string, value: DType.int32 },
		);

		const filtered = df.filter(col("category").eq("A"));
		const plot = await filtered.plot();
		const spec = plot.bar("category", "value").toJSON();
		expect(spec.data[0]!.values).toHaveLength(2);
	});

	it("should handle empty DataFrame", async () => {
		const df = fromRecords([], { x: DType.int32, y: DType.int32 });

		const plot = await df.plot();
		// Empty DataFrame has no column keys in toColumns() result
		// PlotBuilder should handle missing columns gracefully
		expect(plot).toBeInstanceOf(PlotBuilder);
	});
});

/* VEGA-LITE ADAPTER
/*----------------------------------------------------- */

describe("toVegaLiteSpec", () => {
	it("should produce valid Vega-Lite schema for bar charts", () => {
		const spec = buildBarSpec(["A", "B", "C"], [10, 20, 30], {
			title: "Bar Test",
		});
		const vl = toVegaLiteSpec(spec);
		expect(vl.$schema).toContain("vega-lite");
		expect(vl.title).toBe("Bar Test");
		expect(vl.mark!.type).toBe("bar");
		expect(vl.encoding!.x!.field).toBe("x");
		expect(vl.encoding!.x!.type).toBe("nominal");
		expect(vl.encoding!.y!.field).toBe("y");
		expect(vl.encoding!.y!.type).toBe("quantitative");
		expect(vl.data.values).toHaveLength(3);
		expect(vl.data.values[0]).toEqual({ x: "A", y: 10 });
	});

	it("should produce valid Vega-Lite schema for line charts", () => {
		const spec = buildLineSpec([1, 2, 3], [10, 20, 30]);
		const vl = toVegaLiteSpec(spec);
		expect(vl.mark!.type).toBe("line");
		expect(vl.encoding!.x!.type).toBe("quantitative");
		expect(vl.data.values).toHaveLength(3);
	});

	it("should use layer for line charts with showPoints", () => {
		const spec = buildLineSpec([1, 2], [10, 20], { showPoints: true });
		const vl = toVegaLiteSpec(spec);
		expect(vl.layer).toBeDefined();
		expect(vl.layer).toHaveLength(2);
		expect(vl.layer![0]!.mark.type).toBe("line");
		expect(vl.layer![1]!.mark.type).toBe("point");
		// Single-mark fields should not be set when using layers
		expect(vl.mark).toBeUndefined();
		expect(vl.encoding).toBeUndefined();
	});

	it("should produce valid Vega-Lite schema for scatter charts", () => {
		const spec = buildScatterSpec([1, 2, 3], [4, 5, 6], { pointRadius: 6 });
		const vl = toVegaLiteSpec(spec);
		expect(vl.mark!.type).toBe("point");
		expect(vl.mark!.filled).toBe(true);
		expect(vl.encoding!.x!.type).toBe("quantitative");
		expect(vl.encoding!.y!.type).toBe("quantitative");
		// pointRadius mapped to area-based size
		expect(vl.encoding!.size!.value).toBeCloseTo(6 * 6 * Math.PI);
	});

	it("should produce valid Vega-Lite schema for histograms", () => {
		const spec = buildHistogramSpec([1, 2, 3, 4, 5], { bins: 3 });
		const vl = toVegaLiteSpec(spec);
		expect(vl.mark!.type).toBe("rect");
		expect(vl.encoding!.x!.field).toBe("x");
		expect(vl.encoding!.x2).toBeDefined();
		expect(vl.encoding!.x2!.field).toBe("x2");
		// Each row should have x, x2, y
		for (const row of vl.data.values) {
			expect(row).toHaveProperty("x");
			expect(row).toHaveProperty("x2");
			expect(row).toHaveProperty("y");
		}
	});

	it("should compute inner width/height by subtracting padding", () => {
		const spec = buildBarSpec(["a"], [1], { width: 800, height: 500 });
		const vl = toVegaLiteSpec(spec);
		expect(vl.width).toBe(800 - spec.padding.left - spec.padding.right);
		expect(vl.height).toBe(500 - spec.padding.top - spec.padding.bottom);
	});

	it("should omit title when not set", () => {
		const spec = buildBarSpec(["a"], [1]);
		const vl = toVegaLiteSpec(spec);
		expect(vl.title).toBeUndefined();
	});

	it("should preserve color in mark", () => {
		const spec = buildBarSpec(["a"], [1], { color: "#ff0000" });
		const vl = toVegaLiteSpec(spec);
		expect(vl.mark!.color).toBe("#ff0000");
	});

	it("should handle empty data", () => {
		const spec = buildBarSpec([], []);
		const vl = toVegaLiteSpec(spec);
		expect(vl.data.values).toHaveLength(0);
	});
});

describe("PlotResult.toVegaLite()", () => {
	it("should return a valid Vega-Lite spec", () => {
		const spec = buildBarSpec(["A", "B"], [10, 20], { title: "VL Test" });
		const result = new PlotResult(spec);
		const vl = result.toVegaLite();
		expect(vl.$schema).toContain("vega-lite");
		expect(vl.title).toBe("VL Test");
		expect(vl.data.values).toHaveLength(2);
	});

	it("should be accessible from PlotBuilder chain", () => {
		const builder = new PlotBuilder({ x: ["a", "b"], y: [1, 2] });
		const vl = builder.bar("x", "y").toVegaLite();
		expect(vl.$schema).toContain("vega-lite");
		expect(vl.data.values).toHaveLength(2);
	});
});
