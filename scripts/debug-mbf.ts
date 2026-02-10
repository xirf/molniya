
import * as fs from "node:fs/promises";
import { BinaryWriter } from "../src/io/mbf/writer.ts";
import { BinaryReader } from "../src/io/mbf/reader.ts";
import { DType } from "../src/types/dtypes.ts";
import { createSchema } from "../src/types/schema.ts";
import { createChunkFromArrays } from "../src/buffer/chunk.ts";
import { Dictionary } from "../src/buffer/dictionary.ts";
import { unwrap } from "../src/types/error.ts";

const TEST_FILE = "debug_data.mbf";

const LOG_FILE = "debug_log.txt";

async function log(msg: string) {
    console.log(msg);
    await fs.appendFile(LOG_FILE, msg + "\n");
}

async function main() {
    await fs.writeFile(LOG_FILE, ""); // Clear log
    await log("Starting debug script...");
    
    const schema = unwrap(createSchema({
        id: DType.int32,
        name: DType.string,
        score: DType.nullable.int32,
    }));

    const dict = new Dictionary();
    const names = ["Alice", "Bob", "Charlie", "David", "Eve"];
    const nameIndices = new Uint32Array(names.map(n => dict.internString(n)));
    
    // Chunk 1
    const ids = new Int32Array([1, 2, 3, 4, 5]);
    const scores = new Int32Array([10, 20, 0, 40, 50]); // 0 is placeholder for null

    const chunk = unwrap(createChunkFromArrays(schema, [ids, nameIndices, scores], dict));
    // Set nulls for score (index 2)
    chunk.getColumn(2)?.setNull(2, true); // 3rd row is null

    await log("Writing file...");
    const writer = new BinaryWriter(TEST_FILE, schema);
    await writer.open();
    try {
        await writer.writeChunk(chunk);
    } catch (e: any) {
        await log(`Error: ${e.message}\nStack: ${e.stack}`);
        throw e;
    }
    await writer.close();
    await log("File written.");

    const stat = await fs.stat(TEST_FILE);
    await log(`File size: ${stat.size}`);

    await log("Reading file...");
    const reader = new BinaryReader(TEST_FILE, schema);
    await reader.open();
    
    let count = 0;
    for await (const chunk of reader.scan()) {
        await log(`Read chunk with ${chunk.rowCount} rows`);
        count++;
    }
    await log(`Total chunks read: ${count}`);
    await reader.close();
    
    await fs.unlink(TEST_FILE);
}

main().catch(console.error);
