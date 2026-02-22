/**
 * Multi-Scale Sustained Throughput Benchmark
 * Tests realistic query patterns at multiple dataset sizes to identify performance cliffs.
 * Run: bun run scripts/benchmark-all.ts
 */
import { readCsv, col } from "../src/dataframe/index.ts";
import type { CsvReadOptions, DataFrame } from "../src/dataframe/dataframe.ts";
import { and } from "../src/expr/builders.ts";
import { DType } from "../src/types/dtypes.ts";

const schema = {
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
};

const readOptions: CsvReadOptions = {
	// offload: true,
};
const useOffload = Boolean(readOptions.offload || readOptions.offloadFile);

let fullDf: DataFrame | null = null;
let maxRows = 0;

console.log("Loading complete student dataset...");
const startLoad = performance.now();

if (useOffload) {
	fullDf = await readCsv("students_worldwide_100k.csv", schema, readOptions);
	console.log(`CSV loaded in ${(performance.now() - startLoad).toFixed(0)}ms`);
	maxRows = await fullDf.count();
	console.log(`Full dataset: ${(maxRows / 1e6).toFixed(1)}M rows available\n`);
} else {
	console.log(`CSV reader initialized in ${(performance.now() - startLoad).toFixed(0)}ms (streaming)`);
	console.log("Streaming mode: will re-read CSV per scale with maxRows to avoid full materialization.\n");
}

const baseScales = [
	{ name: "1K", rows: 1_000 },
	{ name: "10K", rows: 10_000 },
	{ name: "100K", rows: 100_000 },
	{ name: "1M", rows: 1_000_000 },
	{ name: "Full", rows: 0 },
];

if (useOffload) {
	baseScales[baseScales.length - 1]!.rows = maxRows;
} else {
	maxRows = baseScales[baseScales.length - 2]!.rows;
}

const scales = useOffload
	? baseScales
	: baseScales.filter((s) => s.name !== "Full");

const queries = [
	{
		name: "Simple Filter",
		prepare: (df: DataFrame) => df.filter(col("student_rollno").lt(500000)),
		run: (df: DataFrame) => df.count(),
	},
	{
		name: "String Search",
		prepare: (df: DataFrame) => df.filter(col("country").contains("or")),
		run: (df: DataFrame) => df.count(),
	},
	{
		name: "Complex Filter",
		prepare: (df: DataFrame) => df.filter(and(
			col("student_rollno").gte(10000),
			col("country").startsWith("A"),
		)),
		run: (df: DataFrame) => df.count(),
	},
	{
		name: "Count All",
		prepare: (df: DataFrame) => df,
		run: (df: DataFrame) => df.count(),
	},
];

async function sustainedBench(
	name: string,
	datasetSize: number,
	fn: () => Promise<unknown>,
): Promise<{
	name: string;
	size: number;
	iterations: number;
	time: number;
	throughput: number;
	result: unknown;
}> {
	// Run 3 times and report median. For tiny datasets, repeat to reduce timer noise.
	const targetRows = 1_000_000;
	const iterations = Math.max(1, Math.ceil(targetRows / datasetSize));
	const times: number[] = [];
	let result: unknown;
	for (let i = 0; i < 3; i++) {
		const start = performance.now();
		for (let j = 0; j < iterations; j++) {
			result = await fn();
		}
		times.push(performance.now() - start);
	}
	times.sort((a, b) => a - b);
	const elapsed = times[1]!; // median
	const totalRows = datasetSize * iterations;
	const throughput = totalRows / elapsed * 1000; // rows/second
	return { name, size: datasetSize, iterations, time: elapsed, throughput, result };
}

function formatThroughput(throughput: number): string {
	if (throughput >= 1e9) return `${(throughput / 1e9).toFixed(2)}B/s`;
	if (throughput >= 1e6) return `${(throughput / 1e6).toFixed(1)}M/s`;
	if (throughput >= 1e3) return `${(throughput / 1e3).toFixed(0)}K/s`;
	return `${throughput.toFixed(0)}/s`;
}

function formatTime(time: number): string {
	if (time >= 1000) return `${(time / 1000).toFixed(1)}s`;
	if (time >= 10) return `${time.toFixed(0)}ms`;
	return `${time.toFixed(2)}ms`;
}

console.log(`\n╔══════════════════════════════════════════════════════════════════════════╗`);
console.log(`║              Molniya — Multi-Scale Sustained Throughput Benchmark       ║`);
console.log(`║                     Real CSV Data Performance Analysis                  ║`);
console.log(`╚══════════════════════════════════════════════════════════════════════════╝\n`);

for (const scale of scales) {
	const testDf = useOffload
		? (scale.rows === maxRows ? fullDf : fullDf?.limit(scale.rows))
		: await readCsv("students_worldwide_100k.csv", schema, {
			...readOptions,
			maxRows: scale.rows,
		});

	if (!testDf) {
		console.log("  ⚠️  Missing dataset for scale, skipping.");
		continue;
	}

	console.log(`━━━ ${scale.name} Rows (${(scale.rows / 1000).toFixed(0)}K) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

	for (const query of queries) {
		const prepared = query.prepare(testDf);
		const result = await sustainedBench(query.name, scale.rows, () => query.run(prepared));
		const throughputStr = formatThroughput(result.throughput);
		const timeStr = formatTime(result.time);
		const resultStr = typeof result.result === "number"
			? result.result.toLocaleString()
			: String(result.result);
		const iterStr = result.iterations > 1 ? ` x${result.iterations}` : "";

		console.log(`  ${query.name.padEnd(20)} │ ${timeStr.padStart(8)} │ ${throughputStr.padStart(10)} rows/s │ ${resultStr.padStart(10)}${iterStr}`);

		if (result.time > 30000) {
			console.log(`  ⚠️  Query timeout - skipping remaining scales for realistic benchmarking`);
			break;
		}
	}
	console.log();

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
if (useOffload) {
	console.log(`Dataset: students_worldwide_100k.csv (${(maxRows / 1e6).toFixed(1)}M total rows)`);
} else {
	console.log("Dataset: students_worldwide_100k.csv (streaming, maxRows per scale)");
}
console.log(`Engine: Molniya DataFrame with real-world CSV data\n`);