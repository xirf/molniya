import { readCsv, col } from "../src/dataframe/index.ts";
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

const rssMB = () => Math.round(process.memoryUsage().rss / 1024 / 1024);

try {
	const df = await readCsv("students_worldwide_100k.csv", schema, { maxRows: 1000 });
	console.log("start rss", rssMB(), "MB");

	for (let i = 1; i <= 120; i++) {
		const result = await df.filter(col("student_rollno").lt(500000)).count();
		if (i % 20 === 0) {
			console.log("iter", i, "count", result, "rss", rssMB(), "MB");
		}
	}

	console.log("end rss", rssMB(), "MB");
} catch (error) {
	console.error("probe error", error);
	process.exit(1);
}
