import { readCsv } from "../src/dataframe/index.ts";
import { DType } from "../src/types/dtypes.ts";
import { asc, desc } from "../src/ops/sort.ts";

async function main() {
	process.stdout.write("Loading CSV...\n");
	const fullDf = await readCsv("students_worldwide_100k.csv", {
		student_rollno: DType.int32,
		student_name: DType.string,
		student_course: DType.string,
		student_section: DType.string,
		student_gender: DType.string,
		student_age: DType.int32,
		student_country: DType.string,
	});

	console.log(`Loaded ${await fullDf.count()} rows.`);

	// Warmup / Small scale
	// 100k
	const subDf = fullDf.limit(100_000);

	process.stdout.write("Sorting 100k rows by Age (Int32)...\n");
	const start1 = performance.now();
	await subDf.sort(asc("student_age")).collect();
	const mid1 = performance.now();
	console.log(`Sorted 100k Int32 in ${(mid1 - start1).toFixed(2)}ms`);

	process.stdout.write("Sorting 100k rows by Name (String)...\n");
	const start2 = performance.now();
	await subDf.sort(asc("student_name")).collect();
	const mid2 = performance.now();
	console.log(`Sorted 100k String in ${(mid2 - start2).toFixed(2)}ms`);

	// 1M (actually full dataset is 100k rows? Filename says 100k. If so, 1M limit does nothing.)
	// Let's assume filename is accurate.
	// I'll just sort fullDf.

	process.stdout.write("Sorting Full rows by Age (Int32)...\n");
	const start3 = performance.now();
	await fullDf.sort(asc("student_age")).collect();
	const mid3 = performance.now();
	console.log(`Sorted Full Int32 in ${(mid3 - start3).toFixed(2)}ms`);

	process.stdout.write("Sorting Full rows by Name (String)...\n");
	const start4 = performance.now();
	await fullDf.sort(asc("student_name")).collect();
	const mid4 = performance.now();
	console.log(`Sorted Full String in ${(mid4 - start4).toFixed(2)}ms`);
}

main();
