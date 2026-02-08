
import { BinaryWriter } from "../src/io/mbf/writer.ts";
import { readMbf } from "../src/io/index.ts";
import { DataFrame } from "../src/dataframe/index.ts";
import { createSchema } from "../src/types/schema.ts";
import { Chunk } from "../src/buffer/chunk.ts";
import { ColumnBuffer } from "../src/buffer/column-buffer.ts";
import { DTypeKind } from "../src/types/dtypes.ts";
import { unwrap } from "../src/types/error.ts";


const PROBE_SIZE = 50000;
const BUILD_SIZE = 1000;
const PROBE_FILE = "probe.mbf";
const BUILD_FILE = "build.mbf";
const RESULT_FILE = "join_result.mbf";



import { createDictionary, Dictionary } from "../src/buffer/index.ts";

async function generateData() {
    console.log("Generating data...");
    
    // Write Probe (Large)
    const probeSchema = unwrap(createSchema({
        id: { kind: DTypeKind.Int32, nullable: false },
        val_probe: { kind: DTypeKind.String, nullable: true }
    }));
    const probeWriter = new BinaryWriter(PROBE_FILE, probeSchema);
    await probeWriter.open();
    
    const chunkSize = 5000;
    for(let i=0; i<PROBE_SIZE; i+=chunkSize) {
        const count = Math.min(chunkSize, PROBE_SIZE - i);
        const idCol = new ColumnBuffer(DTypeKind.Int32, count, false);
        const valCol = new ColumnBuffer(DTypeKind.String, count, true);
        const dict = createDictionary();
        
        for(let j=0; j<count; j++) {
            idCol.set(j, i + j);
            const val = `probe_${i+j}`;
            const idx = dict.internString(val);
            valCol.set(j, idx);
        }
        const chunk = new Chunk(probeSchema, [idCol, valCol], dict);
        await probeWriter.writeChunk(chunk);
    }
    await probeWriter.close();
    console.log(`Probe written: ${PROBE_SIZE} rows to ${PROBE_FILE}`);

    // Write Build (Small) - subset of IDs
    const buildSchema = unwrap(createSchema({
        id: { kind: DTypeKind.Int32, nullable: false },
        val_build: { kind: DTypeKind.String, nullable: true }
    }));
    const buildWriter = new BinaryWriter(BUILD_FILE, buildSchema);
    await buildWriter.open();
    
    const buildCount = BUILD_SIZE;
    const idCol = new ColumnBuffer(DTypeKind.Int32, buildCount, false);
    const valCol = new ColumnBuffer(DTypeKind.String, buildCount, true);
    const buildDict = createDictionary();

    for(let i=0; i<buildCount; i++) {
        idCol.set(i, i); 
        const val = `build_${i}`;
        const idx = buildDict.internString(val);
        valCol.set(i, idx);
    }
    const chunk = new Chunk(buildSchema, [idCol, valCol], buildDict);
    await buildWriter.writeChunk(chunk);
    await buildWriter.close();
    console.log(`Build written: ${BUILD_SIZE} rows to ${BUILD_FILE}`);
}

async function benchmarkJoin() {
    console.log("Starting Benchmark...");
    const start = performance.now();

    // 1. Read Build side (fully collected)
    console.log("Reading Build MBF...");
    const buildSourceResult = await readMbf(BUILD_FILE);
    if(buildSourceResult.error) throw new Error("Failed to read build");
    console.log("Build MBF read. Creating DataFrame...");
    // Pass buildSourceResult.value directly as it is AsyncIterable
    const buildDf = DataFrame.fromStream(buildSourceResult.value, buildSourceResult.value.getSchema(), null);
    console.log("Build DataFrame created.");

    // 2. Read Probe side (streaming)
    console.log("Reading Probe MBF...");
    const probeSourceResult = await readMbf(PROBE_FILE);
    if(probeSourceResult.error) throw new Error("Failed to read probe");
    console.log("Probe MBF read. Creating DataFrame...");
    const probeDf = DataFrame.fromStream(probeSourceResult.value, probeSourceResult.value.getSchema(), null);
    console.log("Probe DataFrame created.");

    // 3. Perform Join
    console.log("Performing Join...");
    // Inner join on 'id'
    const joinedDfPromise = probeDf.innerJoin(buildDf, "id", "id");

    // 4. Stream result to disk
    const resultDf = await joinedDfPromise; 
    
    const resultWriter = new BinaryWriter(RESULT_FILE, resultDf.schema);
    await resultWriter.open();
    
    let rowCount = 0;
    for await (const chunk of resultDf.stream()) {
        await resultWriter.writeChunk(chunk);
        rowCount += chunk.rowCount;
    }
    await resultWriter.close();
    
    const end = performance.now();
    console.log(`Join completed in ${(end - start).toFixed(2)}ms`);
    console.log(`Result rows: ${rowCount}`);
    
    if (rowCount !== BUILD_SIZE) {
        throw new Error(`Expected ${BUILD_SIZE} rows, got ${rowCount}`);
    }
    console.log("Verification Passed!");
}

async function run() {
    try {
        await generateData();
        await benchmarkJoin();
    } catch(e) {
        console.error(e);
        process.exit(1);
    }
}

run();
