/**
 * Preview script to understand the real data structure
 */
import { readCsv, col } from "../src/dataframe/index.ts";
import { DType } from "../src/types/dtypes.ts";

const df = await readCsv("students_worldwide_100k.csv", {
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

console.log("Dataset overview:");
console.log(`Total rows: ${await df.count()}`);

console.log("\nFirst 10 rows:");
const sample = await df.limit(10).toArray();
console.table(sample);

console.log("\nUnique countries (sample from first 1000 rows):");
const firstThousand = await df.limit(1000).toArray();
const uniqueCountries = [...new Set(firstThousand.map(r => r.country))].slice(0, 10);
console.log(uniqueCountries);

console.log("\nDegree types (sample):");
const uniqueDegrees = [...new Set(firstThousand.map(r => r.degree))].slice(0, 5);
console.log(uniqueDegrees);

console.log("\nSample student roll numbers:");
console.log(firstThousand.slice(0, 5).map(r => r.student_rollno));