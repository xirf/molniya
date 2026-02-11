import * as fs from "node:fs/promises";
import { type Chunk } from "../../buffer/chunk.ts";
import { ColumnBuffer } from "../../buffer/column-buffer.ts";
import { DTypeKind, DTYPE_SIZES } from "../../types/dtypes.ts";
import { ErrorCode } from "../../types/error.ts";
import { type Schema } from "../../types/schema.ts";
import { MBF_MAGIC, MBF_VERSION } from "./types.ts";

/**
 * Writer for Molniya Binary Format (.mbf).
 * Writes data in Row Groups (Chunks) to support streaming.
 */
export class BinaryWriter {
    private handle: fs.FileHandle | null = null;
    private totalRows = 0;

    constructor(
        private path: string,
        private schema: Schema
    ) {}

    async open(): Promise<void> {
        this.handle = await fs.open(this.path, "w");
        
        // Write File Header
        const header = new Uint8Array(16);
        const view = new DataView(header.buffer);
        view.setUint32(0, MBF_MAGIC, true); // Magic
        view.setUint32(4, MBF_VERSION, true); // Version
        // Placeholder for Total Rows (will update on close)
        view.setUint32(8, 0, true); 
        // Col Count
        view.setUint32(12, this.schema.columns.length, true);
        
        await this.handle.write(header);

        // Write Column Directory/Schema
        const textEncoder = new TextEncoder();
        for (const col of this.schema.columns) {
            const nameBytes = textEncoder.encode(col.name);
            const entrySize = 2 + nameBytes.length + 1 + 1; // len(2) + name + kind(1) + nullable(1)
            const entryBuf = new Uint8Array(entrySize);
            const entryView = new DataView(entryBuf.buffer);
            
            entryView.setUint16(0, nameBytes.length, true);
            entryBuf.set(nameBytes, 2);
            entryView.setUint8(2 + nameBytes.length, col.dtype.kind);
            entryView.setUint8(2 + nameBytes.length + 1, col.dtype.nullable ? 1 : 0);
            
            await this.handle.write(entryBuf);
        }
    }

    async writeChunk(chunk: Chunk): Promise<void> {
        if (!this.handle) throw new Error("Writer not open");

        // Materialize chunk if it has a selection vector to ensure
        // physical data matches logical row count. Without this,
        // we'd serialize physical columns but write logical rowCount
        // in the header, producing a corrupt file.
        let writeChunk = chunk;
        if (chunk.hasSelection()) {
            const materialized = chunk.materialize();
            if (materialized.error !== ErrorCode.None) {
                throw new Error(`Failed to materialize chunk: ${materialized.error}`);
            }
            writeChunk = materialized.value;
        }

        const rowCount = writeChunk.rowCount;
        const colCount = this.schema.columns.length;
        const dictionary = writeChunk.dictionary;

        // Serialize all columns first to calculate sizes
        const columnBuffers: Uint8Array[] = [];
        const columnLengths: number[] = []; // Byte length of each column block

        for (let i = 0; i < colCount; i++) {
            const colBuffer = writeChunk.getColumn(i);
            if (!colBuffer) throw new Error(`Missing column ${i}`);
            const serialized = this.serializeColumn(colBuffer, dictionary);
            
            // For simplicity, concat parts
            const totalLen = serialized.reduce((sum, buf) => sum + buf.byteLength, 0);
            const merged = new Uint8Array(totalLen);
            let offset = 0;
            for (const buf of serialized) {
                merged.set(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), offset);
                offset += buf.byteLength;
            }
            
            columnBuffers.push(merged);
            columnLengths.push(totalLen);
        }

        // Calculate Row Group Header Size
        // Rows (4) + Total Bytes (8) + Col Offsets (8 * ColCount)
        const headerSize = 4 + 8 + (8 * colCount);
        const header = new Uint8Array(headerSize);
        const view = new DataView(header.buffer);

        view.setUint32(0, rowCount, true);
        
        let currentOffset = BigInt(0);
        let dataTotalSize = 0;
        
        // Write offsets relative to Data Start (after header)
        for (let i = 0; i < colCount; i++) {
            view.setBigUint64(12 + (i * 8), currentOffset, true);
            const len = columnLengths[i] ?? 0;
            currentOffset += BigInt(len);
            dataTotalSize += len;
        }
        
        view.setBigUint64(4, BigInt(dataTotalSize), true);

        // Write Group Header + Data in a single write to reduce syscalls
        const totalWriteSize = headerSize + dataTotalSize;
        const writeBuf = new Uint8Array(totalWriteSize);
        writeBuf.set(header, 0);
        let writeOffset = headerSize;
        for (const buf of columnBuffers) {
            writeBuf.set(buf, writeOffset);
            writeOffset += buf.byteLength;
        }
        await this.handle.write(writeBuf);

        this.totalRows += rowCount;
    }

    async close(): Promise<void> {
        if (!this.handle) return;

        // Update Total Rows in Header
        const buffer = new Uint8Array(4);
        const view = new DataView(buffer.buffer);
        view.setUint32(0, this.totalRows, true);
        
        // Offset 8 is rowCount
        await this.handle.write(buffer, 0, 4, 8);

        await this.handle.close();
        this.handle = null;
    }

    private serializeColumn(col: ColumnBuffer, dictionary: import("../../buffer/dictionary.ts").Dictionary | null): Uint8Array[] {
        const parts: Uint8Array[] = [];

        // 1. Null Bitmap (if nullable)
        if (col.isNullable) {
             const byteLen = Math.ceil(col.length / 8);
             // Assume we can access internal buffer or copy
             // Since this is private/internal, for now create new if not accessible easily
             // Real implementation should expose nullBitmap
             const bitmap = new Uint8Array(byteLen);
             for(let i=0; i<col.length; i++) {
                 if (col.isNull(i)) {
                     bitmap[i >>> 3] |= (1 << (i & 7));
                 }
             }
             parts.push(bitmap);
        }

        // 2. Data
        if (col.kind === DTypeKind.String) {
            if (!dictionary) throw new Error("String column requires dictionary");

            const count = col.length;
            const offsets = new Uint32Array(count + 1);
            const textEncoder = new TextEncoder();
            const stringBytes: Uint8Array[] = [];
            let currentOffset = 0;
            
            for (let i = 0; i < count; i++) {
                offsets[i] = currentOffset;
                if (col.isNull(i)) {
                    continue;
                }
                
                const dictIdx = col.get(i) as number;
                const str = dictionary.getString(dictIdx);
                const bytes = textEncoder.encode(str);
                stringBytes.push(bytes);
                currentOffset += bytes.length;
            }
            offsets[count] = currentOffset; // Final offset = total length

            // Push Offsets
            parts.push(new Uint8Array(offsets.buffer));
            // Push Strings
            parts.push(...stringBytes);
        } else {
            // Fixed width
            const byteSize = DTYPE_SIZES[col.kind];
            const dataBytes = col.length * byteSize;
            // Need a view over correct length, not capacity
            // col.data is typed array of capacity
            const dataArr = col.data.subarray(0, col.length);
            
            parts.push(new Uint8Array(dataArr.buffer, dataArr.byteOffset, dataBytes));
        }

        return parts;
    }
}
