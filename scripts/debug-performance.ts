/**
 * Performance Bug Diagnostic Tool
 * Tests specific hypotheses about the 60x performance degradation
 */
import { readCsv, col } from "../src/dataframe/index.ts";
import { DType } from "../src/types/dtypes.ts";

console.log("🔍 Diagnosing Performance Bug\n");

// Test memory allocation patterns
console.log("=== Memory Allocation Analysis ===");

async function measureMemoryAndTime(label: string, fn: () => Promise<any>) {
    const startMem = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
    const start = performance.now();
    
    await fn();
    
    const endMem = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
    const elapsed = performance.now() - start;
    
    console.log(`${label}: ${elapsed.toFixed(0)}ms, Memory: ${startMem}MB → ${endMem}MB`);
    return { time: elapsed, memoryDelta: parseFloat(endMem) - parseFloat(startMem) };
}

// Test 1: CSV Loading Memory Pattern
console.log("\n--- Test 1: CSV Loading Memory Allocation ---");
let fullDf: any;
await measureMemoryAndTime("Loading full CSV (10M rows)", async () => {
    fullDf = await readCsv("students_worldwide_100k.csv", {
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
});

// Test 2: count() vs collect() Performance 
console.log("\n--- Test 2: count() Implementation Analysis ---");

const smallDf = fullDf.limit(100000); // 100K rows
const largeDf = fullDf.limit(1000000); // 1M rows

await measureMemoryAndTime("Small count() (100K)", async () => {
    await smallDf.count();
});

await measureMemoryAndTime("Large count() (1M)", async () => {
    await largeDf.count();
});

// Test 3: Filter Memory Allocation
console.log("\n--- Test 3: Filter Operation Memory Pattern ---");

await measureMemoryAndTime("Filter on 100K rows", async () => {
    await smallDf.filter(col("student_rollno").lt(50000)).count();
});

await measureMemoryAndTime("Filter on 1M rows", async () => {
    await largeDf.filter(col("student_rollno").lt(500000)).count();
});

// Test 4: Compare Streaming vs Collection
console.log("\n--- Test 4: Streaming vs Collection Patterns ---");

try {
    // This should use streaming if implemented correctly
    await measureMemoryAndTime("Full dataset count() [SHOULD STREAM]", async () => {
        await fullDf.count();
    });
} catch (error) {
    console.log("Full dataset count() FAILED:", error.message);
}

try {
    // This forces collection
    await measureMemoryAndTime("Full dataset collect() [FORCES MEMORY]", async () => {
        await fullDf.limit(10).toArray();
    });
} catch (error) {
    console.log("Full dataset collection FAILED:", error.message);
}

// Test 5: Memory Growth Pattern
console.log("\n--- Test 5: Memory Growth Pattern ---");
for (const size of [10000, 100000, 500000]) {
    const testDf = fullDf.limit(size);
    await measureMemoryAndTime(`Dataset ${size} rows`, async () => {
        await testDf.filter(col("student_rollno").gt(1000)).count();
    });
}

console.log("\n🎯 DIAGNOSIS COMPLETE");
console.log("Check memory allocations and timing patterns above.");
console.log("Expected findings:");
console.log("- Massive memory allocation during CSV load");
console.log("- count() operations taking excessive memory");
console.log("- Non-linear scaling in memory usage");