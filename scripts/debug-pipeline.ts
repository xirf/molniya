/**
 * Targeted Pipeline Diagnostic
 * Tests the exact pipeline execution path for count() operations
 */
import { readCsv, col } from "../src/dataframe/index.ts";
import { DType } from "../src/types/dtypes.ts";

console.log("🔍 Pipeline Diagnostic - Testing count() Implementation\n");

// Load full dataset
const fullDf = await readCsv("students_worldwide_100k.csv", {
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

// Test 1: Direct source count (no operators)
console.log("=== Test 1: Direct source count() (no operators) ===");
const start1 = performance.now();
const mem1 = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
const directCount = await fullDf.count();
const elapsed1 = performance.now() - start1;
const mem1End = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
console.log(`Direct count: ${directCount} rows in ${elapsed1.toFixed(0)}ms (${mem1}MB → ${mem1End}MB)`);

// Test 2: Small limit count (should be fast)
console.log("\n=== Test 2: Small limit count() (100 rows) ===");
const start2 = performance.now();
const mem2 = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
const limitedDf = fullDf.limit(100);
console.log(`Operators in limited DF: ${(limitedDf as any).operators.length}`);
const limitCount = await limitedDf.count();
const elapsed2 = performance.now() - start2;
const mem2End = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
console.log(`Limited count: ${limitCount} rows in ${elapsed2.toFixed(0)}ms (${mem2}MB → ${mem2End}MB)`);

// Test 3: Large limit count (this is what hangs)
console.log("\n=== Test 3: Large limit count() (1M rows) - THIS IS THE BUG ===");
const start3 = performance.now();
const mem3 = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
const largeLimitDf = fullDf.limit(1_000_000);
console.log(`Operators in 1M limit DF: ${(largeLimitDf as any).operators.length}`);
console.log(`Source type: ${(largeLimitDf as any).source.constructor.name}`);
console.log(`Has asyncIterator: ${Symbol.asyncIterator in (largeLimitDf as any).source}`);

// This should stream but doesn't
const largeCount = await largeLimitDf.count();
const elapsed3 = performance.now() - start3;
const mem3End = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
console.log(`Large count: ${largeCount} rows in ${elapsed3.toFixed(0)}ms (${mem3}MB → ${mem3End}MB)`);

// Test 4: What about filter? 
console.log("\n=== Test 4: Filter count() (for comparison) ===");
const start4 = performance.now();
const mem4 = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
const filteredDf = fullDf.filter(col("student_rollno").lt(1000000));
console.log(`Operators in filtered DF: ${(filteredDf as any).operators.length}`);
const filterCount = await filteredDf.count();
const elapsed4 = performance.now() - start4;
const mem4End = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
console.log(`Filter count: ${filterCount} rows in ${elapsed4.toFixed(0)}ms (${mem4}MB → ${mem4End}MB)`);

console.log("\n🎯 ANALYSIS:");
console.log("If Test 3 is much slower than Test 4, the bug is in the LimitOperator");
console.log("If Test 3 has similar performance to Test 4, the bug is in Pipeline execution");
console.log("Memory growth indicates whether data is being buffered instead of streamed");