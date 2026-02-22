/**
 * Side-by-side benchmark: Molniya counterpart to benchmark-compare.py
 * Tests the same operations on the same data for honest comparison against Polars.
 *
 * Two phases:
 *   Phase 1: End-to-end CSV read + query (cold)
 *   Phase 2: In-memory query only (hot, pre-loaded data)
 *
 * Run: bun run scripts/benchmark-compare.ts
 */
import { readCsv, col, sum, count } from "../src/dataframe/index.ts";
import { and } from "../src/expr/builders.ts";
import { DType } from "../src/types/dtypes.ts";

const CSV_PATH = "students_worldwide_100k.csv";
const N_RUNS = 5;

const file = Bun.file(CSV_PATH);
const fileSizeMb = file.size / (1024 * 1024);
console.log(`CSV file: ${CSV_PATH} (${fileSizeMb.toFixed(1)} MB)`);
console.log(`Runtime: Bun ${Bun.version}`);
console.log();

// ─── Phase 1: End-to-end CSV read ───────────────────────────────────────

console.log("=".repeat(70));
console.log("Phase 1: CSV Read (cold, from disk)");
console.log("=".repeat(70));

const schemaSpec = {
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

// Cold read (no offload - honest CSV parse timing)
const t0 = performance.now();
const df = await readCsv(CSV_PATH, schemaSpec);
// Must materialize to time honestly - collect() forces full parse
const collected = await df.collect();
const t1 = performance.now();
const csvTime = t1 - t0;
const rowCount = await collected.count();
console.log(`  Rows loaded: ${rowCount.toLocaleString()}`);
console.log(`  Read time:   ${csvTime.toFixed(0)} ms`);
console.log(`  Throughput:  ${(rowCount / csvTime * 1000 / 1e6).toFixed(1)} M rows/s`);
console.log();

// Verify source is now in-memory
console.log(`  Source type:  ${Array.isArray(collected.source) ? "Chunk[] (in-memory)" : "AsyncIterable (streaming)"}`);
if (Array.isArray(collected.source)) {
	const chunks = collected.source;
	console.log(`  Chunks:       ${chunks.length}`);
	console.log(`  Avg rows/chunk: ${Math.round(rowCount / chunks.length)}`);
}
console.log();

// ─── Phase 2: In-memory queries ────────────────────────────────────────

console.log("=".repeat(70));
console.log("Phase 2: In-memory query benchmarks");
console.log("=".repeat(70));

type BenchResult = { label: string; medianMs: number; result: number; throughput: string };

async function bench(label: string, fn: () => Promise<number>, expectedCount?: number): Promise<BenchResult> {
	const times: number[] = [];
	let result = 0;
	for (let i = 0; i < N_RUNS; i++) {
		const s = performance.now();
		result = await fn();
		const e = performance.now();
		times.push(e - s);
	}
	times.sort((a, b) => a - b);
	const medianMs = times[Math.floor(N_RUNS / 2)]!;
	const throughput = rowCount / medianMs * 1000;

	let check = "";
	if (expectedCount !== undefined) {
		check = result === expectedCount ? " ✓" : ` ✗ got ${result}`;
	}

	let tpStr: string;
	if (throughput >= 1e9) tpStr = `${(throughput / 1e9).toFixed(2)} B/s`;
	else if (throughput >= 1e6) tpStr = `${(throughput / 1e6).toFixed(1)} M/s`;
	else tpStr = `${(throughput / 1e3).toFixed(0)} K/s`;

	console.log(`  ${label.padEnd(30)} │ ${medianMs.toFixed(1).padStart(8)} ms │ ${tpStr.padStart(12)} │ result=${result.toLocaleString().padStart(10)}${check}`);
	return { label, medianMs, result, throughput: tpStr };
}

console.log();
console.log(`  ${"Query".padEnd(30)} │ ${"Time".padStart(8)}    │ ${"Throughput".padStart(12)} │ ${"Result".padStart(10)}`);
console.log("  " + "─".repeat(30) + "─┼─" + "─".repeat(11) + "─┼─" + "─".repeat(12) + "─┼─" + "─".repeat(20));

// 1. Count all
await bench("Count All", async () => await collected.count());

// 2. Simple numeric filter — verify result
const r1 = await collected.filter(col("student_rollno").lt(500000)).count();
await bench("Filter: rollno < 500K",
	async () => await collected.filter(col("student_rollno").lt(500000)).count(),
	r1);

// 3. String contains
const r2 = await collected.filter(col("country").contains("ian")).count();
await bench("String: country contains 'ian'",
	async () => await collected.filter(col("country").contains("ian")).count(),
	r2);

// 4. Complex filter (AND)
const r3 = await collected.filter(and(
	col("student_rollno").gte(10000),
	col("country").startsWith("A")
)).count();
await bench("Complex: rollno>=10K AND country^A",
	async () => await collected.filter(and(
		col("student_rollno").gte(10000),
		col("country").startsWith("A")
	)).count(),
	r3);

// 5. Count All (no-op baseline for comparison)
await bench("Count All (baseline)",
	async () => await collected.count());

console.log();

// ─── Phase 3: Scaled tests ─────────────────────────────────────────────

console.log("=".repeat(70));
console.log("Phase 3: Scaling behavior (1K → Full)");
console.log("=".repeat(70));

const scales = [
	{ name: "1K", rows: 1_000 },
	{ name: "10K", rows: 10_000 },
	{ name: "100K", rows: 100_000 },
	{ name: "1M", rows: 1_000_000 },
	{ name: "Full", rows: rowCount },
];

console.log();
console.log(`  Simple Filter (rollno < 500K) at different scales:`);
console.log(`  ${"Scale".padEnd(8)} │ ${"Rows".padStart(10)} │ ${"Time".padStart(8)}    │ ${"Throughput".padStart(12)} │ ${"Matched".padStart(10)}`);
console.log("  " + "─".repeat(8) + "─┼─" + "─".repeat(10) + "─┼─" + "─".repeat(11) + "─┼─" + "─".repeat(12) + "─┼─" + "─".repeat(10));

for (const scale of scales) {
	// For sub-scales, collect first so we time only the filter, not limit
	const subset = scale.rows < rowCount
		? await collected.limit(scale.rows).collect()
		: collected;

	const times: number[] = [];
	let result = 0;
	for (let i = 0; i < N_RUNS; i++) {
		const s = performance.now();
		result = await subset.filter(col("student_rollno").lt(500000)).count();
		const e = performance.now();
		times.push(e - s);
	}
	times.sort((a, b) => a - b);
	const medianMs = times[Math.floor(N_RUNS / 2)]!;
	const throughput = scale.rows / medianMs * 1000;

	let tpStr: string;
	if (throughput >= 1e9) tpStr = `${(throughput / 1e9).toFixed(2)} B/s`;
	else if (throughput >= 1e6) tpStr = `${(throughput / 1e6).toFixed(1)} M/s`;
	else tpStr = `${(throughput / 1e3).toFixed(0)} K/s`;

	console.log(`  ${scale.name.padEnd(8)} │ ${scale.rows.toLocaleString().padStart(10)} │ ${medianMs.toFixed(1).padStart(8)} ms │ ${tpStr.padStart(12)} │ ${result.toLocaleString().padStart(10)}`);
}

console.log();
console.log("─".repeat(70));
console.log("Compare with Polars: python scripts/benchmark-compare.py");
console.log();
