/**
 * Benchmark script for Phase 6 optimizer improvements.
 * Tests filter fusion (chained vs manual AND) and limit early termination.
 */
import { fromColumns, col } from "../src/dataframe/index.ts";
import { and } from "../src/expr/builders.ts";
import { DType } from "../src/types/dtypes.ts";

const N = 500_000;
const countries = ["USA", "Brazil", "Germany", "Japan", "India", "China", "France", "UK", "Australia", "Canada"];
const countryCol = Array.from({ length: N }, (_, i) => countries[i % countries.length]!);
const valueCol = Array.from({ length: N }, (_, i) => i * 1.5);

const df = fromColumns(
	{ country: countryCol, value: valueCol },
	{ country: DType.string, value: DType.float64 },
);

async function bench(label: string, fn: () => Promise<unknown>, iterations = 10): Promise<void> {
	// Warmup
	for (let i = 0; i < 3; i++) await fn();

	const times: number[] = [];
	for (let i = 0; i < iterations; i++) {
		const start = performance.now();
		await fn();
		times.push(performance.now() - start);
	}
	const avg = times.reduce((a, b) => a + b, 0) / times.length;
	const min = Math.min(...times);
	const throughput = (N / min * 1000).toFixed(0);
	console.log(`  ${label.padEnd(50)} avg=${avg.toFixed(2)}ms  min=${min.toFixed(2)}ms  ${throughput} rows/s`);
}

console.log(`\n=== Phase 6: Optimizer Benchmarks (${(N / 1000).toFixed(0)}K rows) ===\n`);

// ---------- Filter Fusion ----------
console.log("--- Filter Fusion: chained .filter().filter() vs manual and() ---\n");

// Two string filters
await bench("chained: .filter(eq).filter(neq)", () =>
	df.filter(col("country").eq("Brazil")).filter(col("country").neq("USA")).count(),
);
await bench("manual:  .filter(and(eq, neq))", () =>
	df.filter(and(col("country").eq("Brazil"), col("country").neq("USA"))).count(),
);

console.log();

// String + numeric filters
await bench("chained: .filter(country eq).filter(value gt)", () =>
	df.filter(col("country").eq("Brazil")).filter(col("value").gt(100000)).count(),
);
await bench("manual:  .filter(and(country eq, value gt))", () =>
	df.filter(and(col("country").eq("Brazil"), col("value").gt(100000))).count(),
);

console.log();

// Three chained filters
await bench("chained: .filter(A).filter(B).filter(C)", () =>
	df.filter(col("country").eq("Brazil"))
		.filter(col("value").gt(100000))
		.filter(col("value").lt(500000))
		.count(),
);
await bench("manual:  .filter(and(A, and(B, C)))", () =>
	df.filter(and(col("country").eq("Brazil"), and(col("value").gt(100000), col("value").lt(500000)))).count(),
);

console.log();

// ---------- Limit Early Termination ----------
console.log("--- Limit Pushdown: early termination ---\n");

await bench("filter(eq).count() [full scan]", () =>
	df.filter(col("country").eq("Brazil")).count(),
);
await bench("filter(eq).limit(10).count()", () =>
	df.filter(col("country").eq("Brazil")).limit(10).count(),
);
await bench("filter(eq).limit(100).count()", () =>
	df.filter(col("country").eq("Brazil")).limit(100).count(),
);
await bench("filter(eq).limit(1000).count()", () =>
	df.filter(col("country").eq("Brazil")).limit(1000).count(),
);

console.log();

// ---------- Select + Filter vs Filter + Select ----------
console.log("--- Select + Filter ordering ---\n");

await bench("filter then select", () =>
	df.filter(col("country").eq("Brazil")).select("value").count(),
);
await bench("select then filter (includes filter col)", () =>
	df.select("country", "value").filter(col("country").eq("Brazil")).count(),
);

console.log("\nDone.");
