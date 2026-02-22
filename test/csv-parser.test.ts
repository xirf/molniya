import { expect, test, describe } from "bun:test";
import { createCsvParser } from "../src/io/csv-parser.ts";
import { createSchema } from "../src/types/schema.ts";
import { DTypeKind } from "../src/types/dtypes.ts";
import { StringViewColumnBuffer } from "../src/buffer/string-view-column.ts";

describe("CsvParser StringView zero-copy parsing", () => {
    test("parses strings into StringView without encoding", () => {
        const schemaResult = createSchema({
            id: { kind: DTypeKind.Int32, nullable: false },
            name: { kind: DTypeKind.StringView, nullable: false },
        });
        const schema = schemaResult.value!;

        const parser = createCsvParser(schema, { hasHeader: true });

        const csvData = new TextEncoder().encode(`id,name\n1,Alpha\n2,Beta\n3,Gamma`);
        const chunks = parser.parse(csvData);
        const finalChunk = parser.finish();
        if (finalChunk) chunks.push(finalChunk);

        expect(chunks.length).toBe(1);
        const chunk = chunks[0]!;

        expect(chunk.rowCount).toBe(3);

        // ID column
        expect(chunk.getValue(0, 0)).toBe(1);
        expect(chunk.getValue(0, 1)).toBe(2);
        expect(chunk.getValue(0, 2)).toBe(3);

        // Name column (StringView)
        const nameCol = chunk.getColumn(1) as StringViewColumnBuffer;
        expect(nameCol.kind).toBe(DTypeKind.StringView);

        // Using zero-copy byte retrieval
        const bytes = nameCol.getBytes(0);
        expect(bytes).toBeDefined();
        expect(new TextDecoder().decode(bytes)).toBe("Alpha");

        const str2 = chunk.getStringValue(1, 1);
        expect(str2).toBe("Beta");

        console.log("Chunk rows:", chunk.rowCount);
        console.log("Name col length:", nameCol.length);
        console.log("Name col count:", (nameCol.stringView as any).count);

        const str3 = chunk.getStringValue(1, 2);
        console.log("Str3:", str3);
        expect(str3).toBe("Gamma");
    });
});
