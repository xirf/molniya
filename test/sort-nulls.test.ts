
import { describe, expect, it } from "bun:test";
import { DataFrame } from "../src/dataframe/core.ts"; 
import { fromArrays } from "../src/dataframe/dataframe.ts";
import { asc, desc } from "../src/ops/sort.ts";
import { DType } from "../src/types/dtypes.ts";
import { unwrap } from "../src/types/error.ts";
import { createSchema } from "../src/types/schema.ts";

describe("Sort Null Handling", () => {
    // Schema: id (int), val (int nullable)
    const schema = { id: DType.int32, val: DType.nullable.int32 };
    
    // Data: [1, 10], [2, null], [3, 5], [4, null]
    const df = fromArrays({
        id: [1, 2, 3, 4],
        val: [10, null, 5, null]
    }, schema);

    it("should sort nulls last by default (asc)", async () => {
        // Default: nulls last
        const sorted = await df.sort([asc("val")]);
        const rows = await sorted.toArray();
        
        expect(rows[0].val).toBe(5);
        expect(rows[1].val).toBe(10);
        expect(rows[2].val).toBe(null); 
        expect(rows[3].val).toBe(null);
    });

    it("should sort nulls last by default (desc)", async () => {
        // Default: nulls last
        const sorted = await df.sort([desc("val")]);
        const rows = await sorted.toArray();

        expect(rows[0].val).toBe(10);
        expect(rows[1].val).toBe(5);
        expect(rows[2].val).toBe(null); 
    });

    it("should support asc().nullsFirst()", async () => {
        const key = asc("val").nullsFirst();
        
        const sorted = await df.sort([key]);
        const rows = await sorted.toArray();
        
        expect(rows[0].val).toBe(null);
        expect(rows[1].val).toBe(null);
        expect(rows[2].val).toBe(5);
        expect(rows[3].val).toBe(10);
    });

    it("should support desc().nullsFirst()", async () => {
        const key = desc("val").nullsFirst();

        const sorted = await df.sort([key]);
        const rows = await sorted.toArray();

        expect(rows[0].val).toBe(null);
        expect(rows[1].val).toBe(null);
        expect(rows[2].val).toBe(10); // 10 > 5
        expect(rows[3].val).toBe(5);
    });

    it("should support asc().nullsLast() explicitly", async () => {
        const key = asc("val").nullsLast();

        const sorted = await df.sort([key]);
        const rows = await sorted.toArray();

        expect(rows[0].val).toBe(5);
        expect(rows[1].val).toBe(10);
        expect(rows[2].val).toBe(null);
    });
});
