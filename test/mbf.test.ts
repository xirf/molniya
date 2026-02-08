import { describe, expect, it, beforeAll, afterAll } from "bun:test";
import * as fs from "node:fs/promises";
import { BinaryWriter } from "../src/io/mbf/writer.ts";
import { BinaryReader } from "../src/io/mbf/reader.ts";
import { DType } from "../src/types/dtypes.ts";
import { createSchema } from "../src/types/schema.ts";
import { createChunkFromArrays } from "../src/buffer/chunk.ts";
import { Dictionary } from "../src/buffer/dictionary.ts";
import { unwrap } from "../src/types/error.ts";

const TEST_FILE = "test_data.mbf";
const LOG_FILE = "test_debug.log";

async function log(msg: string) {
    await fs.appendFile(LOG_FILE, msg + "\n");
}

describe("MBF Format", () => {
    let schema: any;

    beforeAll(async () => {
        await fs.writeFile(LOG_FILE, "Starting tests\n");
        schema = unwrap(createSchema({
            id: DType.int32,
            name: DType.string,
            score: DType.nullable.int32,
        }));

        const dict = new Dictionary();
        const names = ["Alice", "Bob", "Charlie", "David", "Eve"];
        const nameIndices = new Uint32Array(names.map(n => dict.internString(n)));
        
        const ids = new Int32Array([1, 2, 3, 4, 5]);
        const scores = new Int32Array([10, 20, 0, 40, 50]); 

        const chunk = unwrap(createChunkFromArrays(schema, [ids, nameIndices, scores], dict));
        chunk.getColumn(2)?.setNull(2, true); 

        await log("Writing file in beforeAll...");
        const writer = new BinaryWriter(TEST_FILE, schema);
        await writer.open();
        try {
            await writer.writeChunk(chunk);
        } catch (e: any) {
             await log(`Write failed: ${e.message}\n${e.stack}`);
             throw e;
        }
        await writer.close();
        await log("File written.");
        
        const stat = await fs.stat(TEST_FILE);
        await log(`File size: ${stat.size}`);
    });

    afterAll(async () => {
        try {
             await fs.unlink(TEST_FILE);
        } catch {}
    });

    it("should read mbf file", async () => {
        await log("Test 1: Reading file");
        const reader = new BinaryReader(TEST_FILE, schema);
        await reader.open();
        
        const chunks = [];
        for await (const chunk of reader.scan()) {
            chunks.push(chunk);
        }
        await log(`Test 1: Read ${chunks.length} chunks`);
        
        expect(chunks.length).toBe(1);
        const readChunk = chunks[0];
        if (!readChunk) throw new Error("No chunk");
        
        expect(readChunk.rowCount).toBe(5);
        
        const readIds = readChunk.getColumn(0)?.data;
        if (!readIds) throw new Error("Missing ID");
        expect(readIds[0]).toBe(1);
        expect(readIds[4]).toBe(5);
        
        const readDict = readChunk.dictionary;
        if (!readDict) throw new Error("Missing dict");
        const readNameIndices = readChunk.getColumn(1)?.data;
        expect(readDict.getString(readNameIndices![0])).toBe("Alice");
        
        const readScores = readChunk.getColumn(2);
        expect(readScores!.get(0)).toBe(10);
        expect(readScores!.isNull(2)).toBe(true);
        
        await reader.close();
    });

    it("should support projection pushdown", async () => {
        await log("Test 2: Projection");
        const reader = new BinaryReader(TEST_FILE, schema);
        await reader.open();
        
        const chunks = [];
        for await (const chunk of reader.scan(["name"])) {
            chunks.push(chunk);
        }
        await log(`Test 2: Read ${chunks.length} chunks`);
        
        expect(chunks.length).toBe(1);
        const readChunk = chunks[0];
        expect(readChunk!.columnCount).toBe(1);
        
        const namesCol = readChunk!.getColumn(0);
        const readDict = readChunk!.dictionary;
        expect(readDict!.getString(namesCol!.get(0) as number)).toBe("Alice");
        
        await reader.close();
    });
});
