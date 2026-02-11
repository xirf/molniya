/**
 * Benchmark script for Phase 2 string optimizations.
 * Tests dictionary-level comparisons, byte-level string ops, and getString caching.
 */
import { fromColumns, col } from "../src/dataframe/index.ts";
import { DType } from "../src/types/dtypes.ts";

const N = 500_000;
const countries = ["USA", "Brazil", "Germany", "Japan", "India", "China", "France", "UK", "Australia", "Canada"];
const countryCol = Array.from({ length: N }, (_, i) => countries[i % countries.length]!);
const valueCol = Array.from({ length: N }, (_, i) => i * 1.5);

const df = fromColumns(
	{ country: countryCol, value: valueCol },
	{ country: DType.string, value: DType.float64 },
);

async function bench(label: string, fn: () => Promise<unknown>, iterations = 5): Promise<void> {
	// Warmup
	await fn();

	const times: number[] = [];
	for (let i = 0; i < iterations; i++) {
		const start = performance.now();
		await fn();
		times.push(performance.now() - start);
	}
	const avg = times.reduce((a, b) => a + b, 0) / times.length;
	const min = Math.min(...times);
	console.log(`${label}: avg=${avg.toFixed(2)}ms  min=${min.toFixed(2)}ms  (${(N / avg * 1000).toFixed(0)} rows/s)`);
}

console.log(`\nPhase 2 String Optimization Benchmarks (${(N / 1000).toFixed(0)}K rows)\n`);

// 2.1: Dictionary-aware string equality
await bench("filter eq('Brazil')", () => df.filter(col("country").eq("Brazil")).count());

// 2.1: Dictionary-aware string inequality
await bench("filter neq('USA')", () => df.filter(col("country").neq("USA")).count());

// 2.1b: Dictionary-aware isIn
await bench("filter isIn 3 vals", () =>
	df.filter(col("country").isIn(["Brazil", "Germany", "Japan"])).count(),
);

// 2.2: Byte-level contains
await bench("filter contains('an')", () =>
	df.filter(col("country").contains("an")).count(),
);

// 2.2: Byte-level startsWith
await bench("filter startsWith('Br')", () =>
	df.filter(col("country").startsWith("Br")).count(),
);

// 2.2: Byte-level endsWith
await bench("filter endsWith('a')", () =>
	df.filter(col("country").endsWith("a")).count(),
);

// Baseline: numeric filter for comparison
await bench("filter value > 250k (numeric)", () =>
	df.filter(col("value").gt(250000)).count(),
);

console.log("\nDone.");
