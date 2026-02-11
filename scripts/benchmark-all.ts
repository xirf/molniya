/**
 * Multi-Scale Sustained Throughput Benchmark
 * Tests realistic query patterns at multiple dataset sizes to identify performance cliffs.
 * Run: bun run scripts/benchmark-all.ts
 */
import { readCsv, col, sum, count } from "../src/dataframe/index.ts";
import { and } from "../src/expr/builders.ts";
import { DType } from "../src/types/dtypes.ts";

// Load full dataset once
console.log("Loading complete student dataset...");
const startLoad = performance.now();

const fullDf = await readCsv("students_worldwide_100k.csv", {
	student_rollno: DType.int32,
	student_name: DType.string,
	degree: DType.string,
	specialization: DType.string,
	course_names: DType.string,
	subjects_marks: DType.string,
	country: DType.string,
	city: DType.string,
	state: DType.string,
	university_name: DType.string,
});

console.log(`CSV loaded in ${(performance.now() - startLoad).toFixed(0)}ms`);
const maxRows = await fullDf.count();
console.log(`Full dataset: ${(maxRows / 1e6).toFixed(1)}M rows available\n`);

// Test scales: from 1K to full dataset
const scales = [
	{ name: "1K", rows: 1_000 },
	{ name: "10K", rows: 10_000 },
	{ name: "100K", rows: 100_000 },
	{ name: "1M", rows: 1_000_000 },
	{ name: "Full", rows: maxRows },
];

// Realistic query patterns
const queries = [
	{
		name: "Simple Filter",
		fn: (df: any) => df.filter(col("student_rollno").lt(500000)).count(),
	},
	{
		name: "String Search", 
		fn: (df: any) => df.filter(col("country").contains("ian")).count(),
	},
	{
		name: "Complex Filter",
		fn: (df: any) => df.filter(and(
			col("student_rollno").gte(10000),
			col("country").startsWith("A")
		)).count(),
	},
	{
		name: "Count All",
		fn: (df: any) => df.count(),
	},
];

async function sustainedBench(name: string, datasetSize: number, fn: () => Promise<unknown>): Promise<{ name: string; size: number; time: number; throughput: number }> {
	// Single run for sustained throughput (no micro-iterations)
	const start = performance.now();
	await fn();
	const elapsed = performance.now() - start;
	const throughput = datasetSize / elapsed * 1000; // rows/second
	return { name, size: datasetSize, time: elapsed, throughput };
}

function formatThroughput(throughput: number): string {
	if (throughput >= 1e9) return `${(throughput / 1e9).toFixed(2)}B/s`;
	if (throughput >= 1e6) return `${(throughput / 1e6).toFixed(1)}M/s`;
	if (throughput >= 1e3) return `${(throughput / 1e3).toFixed(0)}K/s`;
	return `${throughput.toFixed(0)}/s`;
}

function formatTime(time: number): string {
	if (time >= 1000) return `${(time / 1000).toFixed(1)}s`;
	return `${time.toFixed(0)}ms`;
}

console.log(`\n╔══════════════════════════════════════════════════════════════════════════╗`);
console.log(`║              Molniya — Multi-Scale Sustained Throughput Benchmark       ║`);
console.log(`║                     Real CSV Data Performance Analysis                  ║`);
console.log(`╚══════════════════════════════════════════════════════════════════════════╝\n`);

// Run benchmark across all scales
for (const scale of scales) {
	const testDf = scale.rows === maxRows ? fullDf : fullDf.limit(scale.rows);
	
	console.log(`━━━ ${scale.name} Rows (${(scale.rows / 1000).toFixed(0)}K) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
	
	for (const query of queries) {
		const result = await sustainedBench(query.name, scale.rows, () => query.fn(testDf));
		const throughputStr = formatThroughput(result.throughput);
		const timeStr = formatTime(result.time);
		
		console.log(`  ${query.name.padEnd(20)} │ ${timeStr.padStart(8)} │ ${throughputStr.padStart(10)} rows/s`);
		
		// Early termination if query takes too long (>30s)
		if (result.time > 30000) {
			console.log(`  ⚠️  Query timeout - skipping remaining scales for realistic benchmarking`);
			break;
		}
	}
	console.log();
	
	// Skip larger scales if current scale is already slow
	if (scale.rows >= 1000000) {
		const testResult = await sustainedBench("Quick Test", scale.rows, () => testDf.limit(1000).count());
		if (testResult.time > 5000) {
			console.log(`⚠️  Dataset size ${scale.name} shows performance degradation, skipping larger scales\n`);
			break;
		}
	}
}

console.log(`━━━ Performance Analysis ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
console.log(`📊 This benchmark tests SUSTAINED throughput on realistic query patterns`);
console.log(`🎯 Results show where performance cliffs occur as dataset size increases`);
console.log(`📈 Look for sudden drops in throughput to identify scaling limitations`);
console.log(`💾 Memory pressure and I/O become factors at larger scales\n`);
console.log(`Dataset: students_worldwide_100k.csv (${(maxRows / 1e6).toFixed(1)}M total rows)`);
console.log(`Engine: Molniya DataFrame with real-world CSV data\n`);
