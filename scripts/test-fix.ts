import { CsvSource } from "../src/io/csv-source.js";
import { DType } from "../src/types/dtypes.js";
import { unwrap } from "../src/types/error.js";

function measureMemoryAndTime<T>(
	fn: () => Promise<T>,
	label: string,
): Promise<T> {
	return new Promise(async (resolve, reject) => {
		const initialMemory = process.memoryUsage().heapUsed;
		const startTime = performance.now();

		try {
			const result = await fn();
			const endTime = performance.now();
			const finalMemory = process.memoryUsage().heapUsed;

			const memoryDelta = (finalMemory - initialMemory) / 1024 / 1024;
			const duration = endTime - startTime;

			console.log(
				`${label}: ${duration.toFixed(2)}ms, memory: ${memoryDelta.toFixed(2)}MB`,
			);
			resolve(result);
		} catch (error) {
			reject(error);
		}
	});
}

async function testStreamingFix() {
	console.log("Testing CSV streaming fix...");

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

	const csvSource = unwrap(
		CsvSource.fromFile("students_worldwide_100k.csv", schema),
	);

	await measureMemoryAndTime(async () => {
		let rows = 0;
		let chunks = 0;

		for await (const chunk of csvSource) {
			chunks++;
			chunks++;
			// @ts-ignore
			rows += chunk.rowCount;

			// Log progress every 100 chunks
			if (chunks % 100 === 0) {
				const memory = process.memoryUsage().heapUsed / 1024 / 1024;
				console.log(
					`  Processed ${chunks} chunks, ${rows} rows, memory: ${memory.toFixed(2)}MB`,
				);
			}
		}

		console.log(`  Total: ${chunks} chunks, ${rows} rows`);
	}, "CSV streaming test");

	console.log("✅ Test complete");
}

testStreamingFix().catch(console.error);
