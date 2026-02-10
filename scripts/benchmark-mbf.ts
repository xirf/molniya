import * as fs from "node:fs/promises";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import { BinaryWriter } from "../src/io/mbf/writer.ts";
import { BinaryReader } from "../src/io/mbf/reader.ts";
import { unwrap } from "../src/types/error.ts";
import { CsvSource } from "../src/io/csv-source.ts";
import { DType } from "../src/types/dtypes.ts";

const DATASET = "fedesoriano/stroke-prediction-dataset";
const ZIP_FILE = "stroke-prediction-dataset.zip";
const CSV_FILE = "healthcare-dataset-stroke-data.csv"; 
const MBF_FILE = "stroke_data.mbf";

const execAsync = promisify(exec);

// Schema for Stroke Prediction Dataset
const STROKE_SCHEMA = {
    id: DType.int32,
    gender: DType.string,
    age: DType.float64,
    hypertension: DType.int32,
    heart_disease: DType.int32,
    ever_married: DType.string,
    work_type: DType.string,
    Residence_type: DType.string,
    avg_glucose_level: DType.float64,
    bmi: DType.nullable.float64, // Might have "N/A"
    smoking_status: DType.string,
    stroke: DType.int32
};

async function fileExists(path: string) {
    try {
        await fs.access(path);
        return true;
    } catch {
        return false;
    }
}

async function downloadDataset() {
    if (await fileExists(CSV_FILE)) {
        console.log("Dataset already exists.");
        return;
    }
    if (await fileExists(ZIP_FILE)) {
         console.log("Zip exists, assuming already unzipped or unzip it now...");
         await execAsync(`unzip -o ${ZIP_FILE}`);
    } else {
        console.log(`Downloading ${DATASET}...`);
        await execAsync(`kaggle datasets download -d ${DATASET} --unzip`);
    }
}

async function main() {
    try {
        await downloadDataset();
        
        console.log("Reading CSV and writing to MBF...");
        if (!await fileExists(CSV_FILE)) {
            console.error(`CSV file ${CSV_FILE} not found.`);
            return;
        }

        // Parse CSV
        const sourceRes = CsvSource.fromFile(CSV_FILE, STROKE_SCHEMA, { hasHeader: true });
        if (sourceRes.error) {
             console.error("Failed to create CSV Source:", sourceRes.error);
             return;
        }
        const source = sourceRes.value;

        // Create Writer
        const schema = source.getSchema();
        const writer = new BinaryWriter(MBF_FILE, schema);
        await writer.open();

        let chunkCount = 0;
        let rowCount = 0;
        const startWrite = performance.now();

        for await (const chunk of source) {
            await writer.writeChunk(chunk);
            chunkCount++;
            rowCount += chunk.rowCount;
            if (chunkCount % 10 === 0) process.stdout.write(".");
        }
        
        await writer.close();
        const endWrite = performance.now();
        console.log(`\nWritten ${rowCount} rows in ${chunkCount} chunks.`);
        console.log(`Write time: ${(endWrite - startWrite).toFixed(2)}ms`);

        // Read back
        console.log("Reading back from MBF...");
        const reader = new BinaryReader(MBF_FILE, schema);
        await reader.open();
        
        let readRows = 0;
        const startRead = performance.now();
        
        for await (const chunk of reader.scan()) {
            readRows += chunk.rowCount;
            // Verify minimal data?
            // Just count for now to verify integrity of file structure
        }
        await reader.close();
        const endRead = performance.now();
        
        console.log(`Read ${readRows} rows.`);
        console.log(`Read time: ${(endRead - startRead).toFixed(2)}ms`);

        if (readRows !== rowCount) {
             console.error(`Mismatch! Written ${rowCount}, Read ${readRows}`);
             process.exit(1);
        }
        console.log("Verification SUCCESS");
        
        // Cleanup
        await fs.unlink(MBF_FILE);
        
    } catch (e: any) {
        console.error("Error:", e.message);
        console.error(e.stack);
        process.exit(1);
    }
}

main().catch(console.error);
