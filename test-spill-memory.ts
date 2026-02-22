import { readCsv, col, min } from "./src/dataframe/index.ts";
import { DType, DTypeKind } from "./src/types/dtypes.ts";
import * as fs from "node:fs";

const sv: DType = { kind: DTypeKind.StringView, nullable: false };

const schema = {
    student_rollno: DType.int32,
    student_name: sv,
    degree: sv,
    specialization: sv,
    course_names: sv,
    subjects_marks: sv,
    country: sv,
    city: sv,
    state: sv,
    university_name: sv,
};

async function checkMemory(name: string, fn: () => Promise<void>) {
    if (global.gc) global.gc();

    const monitorInterval = setInterval(() => {
        const usage = process.memoryUsage();
        const mb = Math.round(usage.rss / 1024 / 1024);
        const heap = Math.round(usage.heapUsed / 1024 / 1024);
        const ext = Math.round(usage.external / 1024 / 1024);
        const ab = Math.round(usage.arrayBuffers / 1024 / 1024);
        console.log(`[Monitor ${name}] RSS: ${mb}MB | Heap: ${heap}MB | Ext: ${ext}MB | Buffers: ${ab}MB`);
    }, 2000);

    const start = performance.now();
    try {
        console.log(`Starting ${name}...`);
        await fn();
    } finally {
        clearInterval(monitorInterval);
        const end = performance.now();
        console.log(`${name} completed in ${((end - start) / 1000).toFixed(2)}s\n`);
    }
}

async function main() {
    console.log("Starting Memory Spill Profiler");
    const file = "students_worldwide_100k.csv";

    // Test 1: Sort Operator with limit. We sort the entire 10M rows to force spilling.
    await checkMemory("Sort 10M dataset", async () => {
        const df = await readCsv(file, schema, { offload: true }); // Offload to prevent re-reading penalty if multiple sweeps? 
        // wait, offload:true forces MBF creation which might take time/space. Let's just stream.
        const dfStream = await readCsv(file, schema);
        // Order by student_rollno and count (which evaluates the sort pipeline)
        console.log("Executing sort pipeline...");
        const result = await dfStream.orderBy("student_name").limit(10).toArray();
        console.log("Sort result length:", result.length);
    });

    // Test 2: GroupBy. Group by multiple columns to force many groups and spilling.
    await checkMemory("GroupBy many groups", async () => {
        const dfStream = await readCsv(file, schema);
        console.log("Executing groupby pipeline...");
        // Group by course_names which should have many unique combinations
        const result = await dfStream.groupBy("course_names").min("student_rollno").limit(10).toArray();
        console.log("GroupBy result length:", (result as any[]).length);
    });
}

main().catch(console.error);
