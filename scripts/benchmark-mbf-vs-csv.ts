/**
 * MBF vs CSV — End-to-End Benchmark
 *
 * Converts the 100K student CSV to MBF, then compares query performance
 * across both formats at multiple dataset scales.
 *
 * Run:  bun run scripts/benchmark-all.ts
 */
import { readCsv, col, count } from "../src/dataframe/index.ts";
import { and } from "../src/expr/builders.ts";
import { DType } from "../src/types/dtypes.ts";
import { writeToMbf } from "../src/io/offload-utils.ts";
import { readMbf } from "../src/io/mbf-source.ts";
import { unwrap } from "../src/types/error.ts";
import * as fs from "node:fs/promises";

const MBF_FILE = "cache/students.mbf";

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

// ── Helpers ──────────────────────────────────────────────────────────

function fmt(throughput: number): string {
	if (throughput >= 1e9) return `${(throughput / 1e9).toFixed(2)}B/s`;
	if (throughput >= 1e6) return `${(throughput / 1e6).toFixed(1)}M/s`;
	if (throughput >= 1e3) return `${(throughput / 1e3).toFixed(0)}K/s`;
	return `${throughput.toFixed(0)}/s`;
}
function fmtTime(ms: number): string {
	return ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${ms.toFixed(1)}ms`;
}

async function bench(
	label: string,
	rows: number,
	fn: () => Promise<number>,
): Promise<{ label: string; ms: number; throughput: number; result: number }> {
	// No warmup for lazy sources (they're one-shot async iterables)
	const t0 = performance.now();
	const result = await fn();
	const ms = performance.now() - t0;
	const throughput = rows / ms * 1000;
	return { label, ms, throughput, result };
}

function printRow(r: { label: string; ms: number; throughput: number; result: number }) {
	console.log(
		`  ${r.label.padEnd(28)} │ ${fmtTime(r.ms).padStart(9)} │ ${fmt(r.throughput).padStart(11)} │ count=${r.result}`,
	);
}

// ── Step 1: Prepare MBF cache ────────────────────────────────────────

console.log("╔══════════════════════════════════════════════════════════════╗");
console.log("║           Molniya — MBF vs CSV Benchmark                    ║");
console.log("╚══════════════════════════════════════════════════════════════╝\n");

console.log("Step 1: Converting CSV → MBF ...");
const csvDf = await readCsv("students_worldwide_100k.csv", schema);
const convStart = performance.now();
await writeToMbf(csvDf as any, MBF_FILE);
const convTime = performance.now() - convStart;

const mbfStat = await fs.stat(MBF_FILE);
const csvStat = await fs.stat("students_worldwide_100k.csv");
console.log(`  CSV  size: ${(csvStat.size / 1024 / 1024).toFixed(1)} MB`);
console.log(`  MBF  size: ${(mbfStat.size / 1024 / 1024).toFixed(1)} MB`);
console.log(`  Conversion time: ${fmtTime(convTime)}\n`);

// ── Step 2: Verify row count match ──────────────────────────────────

const csvCount = await csvDf.count();
const mbfSource = unwrap(await readMbf(MBF_FILE));
const mbfDf = (await import("../src/dataframe/core.ts")).DataFrame.fromStream(
	mbfSource,
	mbfSource.getSchema(),
	null, // MBF chunks carry their own dictionaries
);
const mbfCount = await mbfDf.count();
console.log(`  CSV rows: ${csvCount.toLocaleString()}`);
console.log(`  MBF rows: ${mbfCount.toLocaleString()}`);
if (csvCount !== mbfCount) {
	console.error("  ❌ ROW COUNT MISMATCH!");
	process.exit(1);
}
console.log("  ✅ Row counts match\n");

// ── Step 3: Benchmark at various scales ─────────────────────────────

const scales = [
	{ name: "10K", rows: 10_000 },
	{ name: "100K", rows: 100_000 },
	{ name: "1M", rows: 1_000_000 },
	{ name: "Full (~10M)", rows: csvCount },
];

const queries = [
	{ name: "Count All", fn: (df: any) => df.count() },
	{
		name: "Numeric Filter (rollno<50K)",
		fn: (df: any) => df.filter(col("student_rollno").lt(50000)).count(),
	},
	{
		name: "String Search (contains)",
		fn: (df: any) => df.filter(col("country").contains("an")).count(),
	},
	{
		name: "Complex AND filter",
		fn: (df: any) =>
			df.filter(and(col("student_rollno").gte(10000), col("country").startsWith("A"))).count(),
	},
];

for (const scale of scales) {
	// Skip full-dataset CSV benchmark (takes minutes); still bench MBF at full scale
	const skipCsvAtFull = scale.rows === csvCount;

	console.log(`━━━ ${scale.name} (${scale.rows.toLocaleString()} rows) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

	// ── CSV source (re-parses from disk each query) ──
	if (skipCsvAtFull) {
		console.log("  [CSV — skipped at full scale (too slow for interactive benchmark)]");
	} else {
		console.log("  [CSV — lazy streaming from disk]");
		for (const q of queries) {
			const csvSrc = await readCsv("students_worldwide_100k.csv", schema);
			const csvL = scale.rows === csvCount ? csvSrc : csvSrc.limit(scale.rows);
			const r = await bench(q.name, scale.rows, () => q.fn(csvL));
			printRow(r);
		}
	}

	// ── MBF source (binary columnar from disk) ──
	// MBF source is one-shot (async generator), so create fresh source per query
	const { DataFrame } = await import("../src/dataframe/core.ts");
	async function freshMbfDf() {
		const src = unwrap(await readMbf(MBF_FILE));
		return DataFrame.fromStream(src, src.getSchema(), null);
	}

	console.log("  [MBF — binary columnar from disk]");
	for (const q of queries) {
		const mbf = await freshMbfDf();
		const mbfL = scale.rows === csvCount ? mbf : mbf.limit(scale.rows);
		const r = await bench(q.name, scale.rows, () => q.fn(mbfL));
		printRow(r);
	}

	// ── Speedup summary ──
	console.log();
}

// ── Step 4: CSV offload path (auto MBF caching) ─────────────────────

console.log("━━━ Offload Path: readCsv({ offload: true }) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
console.log("  First call (cold): CSV parse + MBF write + MBF read");
// Delete any existing cache to force cold start
try { await fs.rm("./cache", { recursive: true }); } catch {}

const coldStart = performance.now();
const offloadDf1 = await readCsv("students_worldwide_100k.csv", schema, { offload: true });
const coldCount = await offloadDf1.limit(100000).count();
const coldTime = performance.now() - coldStart;
console.log(`  Cold: ${coldCount.toLocaleString()} rows in ${fmtTime(coldTime)}`);

console.log("  Second call (warm): MBF cache hit");
const warmStart = performance.now();
const offloadDf2 = await readCsv("students_worldwide_100k.csv", schema, { offload: true });
const warmCount = await offloadDf2.limit(100000).count();
const warmTime = performance.now() - warmStart;
console.log(`  Warm: ${warmCount.toLocaleString()} rows in ${fmtTime(warmTime)}`);
console.log(`  Speedup from cache: ${(coldTime / warmTime).toFixed(1)}×\n`);

// Cleanup
try { await fs.rm("./cache", { recursive: true }); } catch {}

console.log("═══════════════════════════════════════════════════════════════");
console.log("  💡 CSV re-parses text from disk every query (I/O bound)");
console.log("  💡 MBF reads pre-encoded columnar binary (much faster I/O)");
console.log("  💡 Use { offload: true } for automatic MBF caching");
console.log("═══════════════════════════════════════════════════════════════\n");
