import * as fs from "node:fs/promises";
import { Chunk } from "../../buffer/chunk.ts";

import { ColumnBuffer } from "../../buffer/column-buffer.ts";
import { Dictionary } from "../../buffer/dictionary.ts";
import { DTypeKind } from "../../types/dtypes.ts";
import { createSchema, type Schema } from "../../types/schema.ts";
import { MBF_MAGIC, MBF_VERSION } from "./types.ts";
import { DType } from "../../types/dtypes.ts";
import { unwrap } from "../../types/error.ts";

export class BinaryReader {
    private handle: fs.FileHandle | null = null;
    private globalSchema: Schema | null = null;
    private filePos = 0;

    constructor(
        private path: string,
        schema?: Schema 
    ) {
        this.globalSchema = schema ?? null;
    }

    async open(): Promise<void> {
        this.handle = await fs.open(this.path, "r");
        
        // Read first 16KB (header + schema usually fits)
        const bufferSize = 16384; 
        const buffer = new Uint8Array(bufferSize);
        const { bytesRead } = await this.handle.read(buffer, 0, bufferSize, 0);
        
        if (bytesRead < 16) throw new Error("File too short");
        
        const view = new DataView(buffer.buffer, 0, bytesRead);
        
        const magic = view.getUint32(0, true);
        if (magic !== MBF_MAGIC) throw new Error("Invalid MBF file");
        
        const version = view.getUint32(4, true);
        if (version !== MBF_VERSION) throw new Error(`Unsupported MBF version: ${version}`);
        
        const colCount = view.getUint32(12, true);
        
        // Read Schema
        let offset = 16;
        const schemaSpec: Record<string, DType> = {};
        const textDecoder = new TextDecoder();

        try {
            for(let i=0; i<colCount; i++) {
                if (offset + 2 > bytesRead) throw new Error("Schema larger than buffer (handle this?)");
                
                const nameLen = view.getUint16(offset, true);
                offset += 2;

                const restSize = nameLen + 1 + 1;
                if (offset + restSize > bytesRead) throw new Error("Schema larger than buffer");

                // Note: buffer.subarray creates a view, doesn't copy.
                // TextDecoder works on view.
                const name = textDecoder.decode(buffer.subarray(offset, offset + nameLen));
                const kind = buffer[offset + nameLen];
                const nullable = buffer[offset + nameLen + 1] === 1;
                
                offset += restSize;

                if (kind === undefined) throw new Error("Invalid column kind");
                schemaSpec[name] = { kind: kind, nullable: nullable };
            }
            this.filePos = offset;
            // console.log(`Schema Read Complete. Pos=${this.filePos}`);
        } catch(e) {
            // console.error("Error reading schema:", e);
            throw e;
        }

        const fileSchema = unwrap(createSchema(schemaSpec));

        if (this.globalSchema) {
            if (fileSchema.columns.length !== this.globalSchema.columns.length) {
                 throw new Error(`Schema mismatch: File has ${fileSchema.columns.length} columns, expected ${this.globalSchema.columns.length}`);
            }
        } else {
            this.globalSchema = fileSchema;
        }
    }

    getSchema(): Schema {
        if (!this.globalSchema) throw new Error("Schema not loaded (call open() first)");
        return this.globalSchema;
    }


    async *scan(projection?: string[]): AsyncGenerator<Chunk> {
        if (!this.handle) throw new Error("Reader not open");
        if (!this.globalSchema) throw new Error("Schema not loaded");
        
        const stat = await this.handle.stat();
        const fileSize = stat.size;
        // console.log(`Debug Reader: fileSize=${fileSize}, filePos=${this.filePos}`);


        // Map projection to indices
        const indices: number[] = [];
        if (projection) {
            for (const name of projection) {
                const idx = this.globalSchema.columnMap.get(name);
                if (idx !== undefined) indices.push(idx);
            }
        } else {
             // All columns
             for(let i=0; i<this.globalSchema.columns.length; i++) indices.push(i);
        }
        
        indices.sort((a, b) => a - b);
        
        // Construct projected schema
        const projectedCols = indices.map(i => this.globalSchema.columns[i]);
        const projectedSchema: Schema = {
             columns: projectedCols.filter((c): c is NonNullable<typeof c> => c !== undefined),
             columnMap: new Map(projectedCols.map((c, i) => [c?.name ?? "", i])),
             columnCount: projectedCols.length,
             rowSize: 0 
        };

        const colCount = this.globalSchema.columns.length;

        while (this.filePos < fileSize) {
            const headerSize = 12 + (8 * colCount);
            const headerBuf = new Uint8Array(headerSize);
            const { bytesRead } = await this.handle.read(headerBuf, 0, headerSize, this.filePos);
            
            if (bytesRead === 0) break; // EOF
            if (bytesRead < headerSize) throw new Error("Unexpected EOF in chunk header");
            
            const view = new DataView(headerBuf.buffer);
            const rowCount = view.getUint32(0, true);
            const dataTotalSize = Number(view.getBigUint64(4, true));
            
            const offsets: bigint[] = [];
            for(let i=0; i<colCount; i++) {
                offsets.push(view.getBigUint64(12 + (i * 8), true));
            }
            
            const dataStart = this.filePos + headerSize;
            const nextChunkPos = dataStart + dataTotalSize;
            
            // Read requested columns
            const columnBuffers: ColumnBuffer[] = [];
            const dictionary = new Dictionary(); 

            for (let i = 0; i < indices.length; i++) {
                const colIdx = indices[i];
                if (colIdx === undefined) continue;
                const colDef = this.globalSchema.columns[colIdx];
                if (!colDef) continue;
                
                const startOffset = offsets[colIdx];
                if (startOffset === undefined) continue;

                let length: bigint;
                if (colIdx < colCount - 1) {
                    const nextOff = offsets[colIdx + 1];
                    length = (nextOff !== undefined ? nextOff : BigInt(dataTotalSize)) - startOffset;
                } else {
                    length = BigInt(dataTotalSize) - startOffset;
                }
                
                const colBuf = new Uint8Array(Number(length));
                await this.handle.read(colBuf, 0, Number(length), dataStart + Number(startOffset));
                
                columnBuffers.push(this.deserializeColumn(colBuf, colDef.dtype.kind, rowCount, colDef.dtype.nullable, dictionary));
            }

            yield new Chunk(projectedSchema, columnBuffers, dictionary);

            this.filePos = nextChunkPos;
        }
    }

    async close(): Promise<void> {
        if (this.handle) {
            await this.handle.close();
            this.handle = null;
        }
    }

    private deserializeColumn(
        buffer: Uint8Array, 
        kind: DTypeKind, 
        rowCount: number, 
        nullable: boolean, 
        dictionary: Dictionary
    ): ColumnBuffer {
        let offset = 0;
        let nullBitmap: Uint8Array | null = null;
        
        if (nullable) {
            const bitmapLen = Math.ceil(rowCount / 8);
            nullBitmap = buffer.subarray(0, bitmapLen);
            offset += bitmapLen;
        }
        
        const colBuffer = new ColumnBuffer(kind, rowCount, nullable);
        
        if (kind === DTypeKind.String) {
            const offsetsLen = (rowCount + 1) * 4;
            
            // Create aligned copy of offsets
            const offsetsBytes = buffer.subarray(offset, offset + offsetsLen);
            const offsets = new Uint32Array(offsetsBytes.slice().buffer);
            
            offset += offsetsLen;
            const stringDataStart = offset;
            const textDecoder = new TextDecoder();
            
            for(let i=0; i<rowCount; i++) {
                if (nullable && nullBitmap) {
                     const isNull = (nullBitmap[i >>> 3]! & (1 << (i & 7))) !== 0; 
                     if (isNull) {
                         colBuffer.setNull(i, true);
                         continue;
                     }
                }
                
                const start = offsets[i]!;
                const end = offsets[i+1]!;
                
                const strBytes = buffer.subarray(stringDataStart + start, stringDataStart + end);
                const str = textDecoder.decode(strBytes);
                const dictIdx = dictionary.internString(str);
                colBuffer.set(i, dictIdx);
            }
             (colBuffer as unknown as { _length: number })._length = rowCount;
        } else {
            const dataBytes = buffer.subarray(offset);
            const alignedBuffer = dataBytes.slice().buffer;
            
            // biome-ignore lint/suspicious/noExplicitAny: Generic casting
            const target = colBuffer.data as unknown as { set: (arr: any) => void, constructor: any, BYTES_PER_ELEMENT: number };
            const Constructor = target.constructor;
            const view = new Constructor(alignedBuffer);
            
            target.set(view);
            (colBuffer as unknown as { _length: number })._length = rowCount;
        }
        
        if (nullable && nullBitmap) {
             const targetBitmap = (colBuffer as any).nullBitmap as Uint8Array;
             targetBitmap.set(nullBitmap);
        }

        return colBuffer;
    }
}
