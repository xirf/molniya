import { CsvSource } from '../src/io/csv-source.js';
import { DType } from '../src/types/dtypes.js';
import { unwrap } from '../src/types/error.js';

async function testActualIssue() {
	console.log('🔍 Analyzing memory accumulation pattern...');
	
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
	
	const csvSource = unwrap(CsvSource.fromFile('students_worldwide_100k.csv', schema));

	// Test with smaller limits to understand scaling
	const limits = [100, 1000, 10000, 50000];
	
	for (const limit of limits) {
		console.log(`\n--- Testing ${limit} rows ---`);
		const startMem = process.memoryUsage().heapUsed / 1024 / 1024;
		const start = performance.now();
		
		let processedRows = 0;
		let chunks = 0;
		
		for await (const chunk of csvSource) {
			chunks++;
			// Determine number of rows in this chunk in a type-safe way.
			let rowsInChunk = 0;
			if (Array.isArray(chunk)) {
				rowsInChunk = chunk.length;
			} else if ('length' in (chunk as any) && typeof (chunk as any).length === 'number') {
				rowsInChunk = (chunk as any).length;
			} else if ('size' in (chunk as any) && typeof (chunk as any).size === 'number') {
				rowsInChunk = (chunk as any).size;
			} else if ('count' in (chunk as any) && typeof (chunk as any).count === 'number') {
				rowsInChunk = (chunk as any).count;
			} else {
				// Fallback when shape is unknown
				rowsInChunk = 1;
			}
			processedRows += rowsInChunk;
			
			if (processedRows >= limit) {
				break;
			}
		}
		
		const duration = performance.now() - start;
		const endMem = process.memoryUsage().heapUsed / 1024 / 1024;
		const memUsed = endMem - startMem;
		
		console.log(`  Processed: ${processedRows} rows, ${chunks} chunks`);
		console.log(`  Time: ${duration.toFixed(2)}ms (${(processedRows/duration*1000).toFixed(0)} rows/sec)`);
		console.log(`  Memory: ${memUsed.toFixed(2)}MB (${(memUsed/processedRows*1000000).toFixed(1)}B per row)`);
		
		// Memory should be roughly linear with rows if there's accumulation
		if (limit > 100) {
			const bytesPerRow = memUsed / processedRows * 1000000;
			if (bytesPerRow > 50) {
				console.log(`  ⚠️  HIGH MEMORY: ${bytesPerRow.toFixed(1)} bytes/row suggests accumulation!`);
			}
		}
	}
}

testActualIssue().catch(console.error);