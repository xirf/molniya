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

const df = await readCsv("students_worldwide_100k.csv", schema, { maxRows: 200 });

// Count total rows
const total = await df.count();
console.log("Total rows:", total);

// Inspect first chunk
for await (const chunk of df.stream()) {
    const col6 = chunk.getColumn(6);
    const dict = chunk.dictionary;
    console.log("col kind:", col6?.kind, "dict?:", !!dict, "chunk rows:", chunk.rowCount);
    // Print first few country values from dictionary
    if (dict) {
        // Check a few raw index values
        for (let i = 0; i < Math.min(5, chunk.rowCount); i++) {
            const idx = col6?.get(i) as number;
            const bytes = dict.getBytes(idx);
            const str = dict.getString(idx);
            console.log(`  row ${i}: idx=${idx}, str="${str}", bytesLen=${bytes?.length}`);
        }
    }
    break;
}

// Now try the filter
const n = await df.filter(col("country").contains("ian")).count();
console.log("String Search (contains 'ian'):", n);

const n2 = await df.filter(col("country").eq("Indian")).count();
console.log("String Eq 'Indian':", n2);
