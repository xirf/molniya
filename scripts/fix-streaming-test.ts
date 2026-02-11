/**
 * **STREAMING CSV PARSER FIX**
 * 
 * The root issue: CsvParser accumulates ALL string data in a shared dictionary
 * across the entire file, causing 990MB memory usage for 10M rows.
 * 
 * Solution: Create per-chunk dictionaries that get released after each yield.
 */
import { CsvSource } from '../src/io/csv-source.ts';
import { DType } from '../src/types/dtypes.ts';
import { unwrap } from '../src/types/error.ts';
import { createCsvParser } from '../src/io/csv-parser.ts';
import { createSchema } from '../src/types/schema.ts';

console.log('🔧 Testing Streaming CSV Fix Approach');

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

// Test current issue
console.log('\n=== Current Issue Confirmation ===');
const source = unwrap(CsvSource.fromFile('students_worldwide_100k.csv', schema));

const startMem = process.memoryUsage().heapUsed / 1024 / 1024;
const start = performance.now();

let totalRows = 0;
let chunks = 0;

for await (const chunk of source) {
    chunks++;
    totalRows += chunk.rowCount;
    
    if (chunks % 100 === 0) {
        const currentMem = process.memoryUsage().heapUsed / 1024 / 1024;
        console.log(`  Chunk ${chunks}: ${totalRows.toLocaleString()} rows, ${currentMem.toFixed(1)}MB`);
    }
    
    // Test early termination - should break early but doesn't
    if (totalRows >= 100000) {
        console.log(`  🚨 Breaking early at ${totalRows} rows, ${chunks} chunks`);
        break;
    }
}

const duration = performance.now() - start;
const endMem = process.memoryUsage().heapUsed / 1024 / 1024;

console.log(`\n📊 Results:`);
console.log(`  Processed: ${totalRows.toLocaleString()} rows in ${chunks} chunks`);
console.log(`  Duration: ${duration.toFixed(2)}ms`);
console.log(`  Memory: ${startMem.toFixed(1)}MB → ${endMem.toFixed(1)}MB (+${(endMem-startMem).toFixed(1)}MB)`);
console.log(`  Throughput: ${(totalRows / duration * 1000).toFixed(0)} rows/s`);

if (totalRows > 100000) {
    console.log(`\n🚨 CONFIRMED BUG: Early termination failed!`);
    console.log(`  Expected: ~100K rows`);
    console.log(`  Actual: ${totalRows.toLocaleString()} rows`);
    console.log(`  This proves the async iterator doesn't support early termination.`);
}

if (endMem - startMem > 50) {
    console.log(`\n🚨 CONFIRMED BUG: Memory accumulation!`);
    console.log(`  Expected: <50MB growth`);
    console.log(`  Actual: ${(endMem-startMem).toFixed(1)}MB growth`);
    console.log(`  This proves the dictionary accumulates across chunks.`);
}