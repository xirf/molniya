/**
 * CsvSource Streaming Bug Diagnostic
 * Tests CsvSource async iterator directly to isolate the bug
 */
import { CsvSource } from "../src/io/csv-source.ts";
import { DType } from "../src/types/dtypes.ts";
import { unwrap } from "../src/types/error.ts";

console.log("🔍 CsvSource Stream Diagnostic - Testing async iterator directly\n");

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

// Test CsvSource async iterator directly  
console.log("=== Testing CsvSource[Symbol.asyncIterator] directly ===");
const source = unwrap(CsvSource.fromFile("students_worldwide_100k.csv", schema));

const startMem = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
const start = performance.now();

let totalRows = 0;
let chunkCount = 0;

try {
    // This calls CsvSource's Symbol.asyncIterator which calls stream()
    for await (const chunk of source) {
        totalRows += chunk.rowCount;
        chunkCount++;
        
        // Log progress every 100 chunks to see if it's really streaming
        if (chunkCount % 100 === 0) {
            const currentMem = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
            const elapsed = performance.now() - start;
            console.log(`  Chunk ${chunkCount}: ${totalRows.toLocaleString()} rows, ${currentMem}MB (${elapsed.toFixed(0)}ms)`);
        }
    }

    const elapsed = performance.now() - start;
    const endMem = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);

    console.log(`\n✅ CsvSource async iterator completed:`);
    console.log(`   Total rows: ${totalRows.toLocaleString()}`);
    console.log(`   Total chunks: ${chunkCount}`);
    console.log(`   Time: ${elapsed.toFixed(0)}ms`);
    console.log(`   Memory: ${startMem}MB → ${endMem}MB`);
    console.log(`   Throughput: ${(totalRows / elapsed * 1000).toLocaleString()} rows/s`);

    if (elapsed > 5000) {
        console.log("\n🚨 BUG: CsvSource streaming is too slow!");
        console.log("   Expected: ~1-2 seconds for streaming");
        console.log("   Actual: " + (elapsed / 1000).toFixed(1) + " seconds");
    }

    if (parseFloat(endMem) - parseFloat(startMem) > 200) {
        console.log("\n🚨 BUG: CsvSource using too much memory!");
        console.log("   Expected: <50MB memory growth");
        console.log("   Actual: " + (parseFloat(endMem) - parseFloat(startMem)).toFixed(1) + "MB growth");
    }

} catch (error) {
    console.log(`❌ ERROR: ${error.message}`);
}