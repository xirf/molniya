import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode } from "../types/error.ts";
import { ColumnBuffer } from "./column-buffer.ts";
import { StringView } from "./string-view.ts";

/**
 * A specialized ColumnBuffer that natively encapsulates a StringView.
 * It extends ColumnBuffer<DTypeKind.StringView> so it can be seamlessly 
 * included in a Chunk's column array, but it overrides null handling and 
 * provides zero-copy appending for pipelines like CsvParser.
 */
export class StringViewColumnBuffer extends ColumnBuffer<DTypeKind.StringView> {
    readonly stringView: StringView;

    constructor(capacity: number, nullable: boolean = false) {
        // Pass capacity=0 to base so the base Uint8Array wastes no memory.
        // All actual data lives in this.stringView (SharedStringBuffer + Uint32Array offsets/lengths).
        super(DTypeKind.StringView, 0, nullable);
        this.stringView = new StringView(capacity, nullable);
    }

    override get length(): number {
        // StringView internally tracks its own count, but let's sync them
        return super.length;
    }

    override isNull(index: number): boolean {
        return this.stringView.isNull(index);
    }

    override setNull(index: number, isNull: boolean): void {
        this.stringView.setNull(index, isNull);
        super.setNull(index, isNull);
    }

    override appendNull(): ErrorCode {
        this.stringView.appendNull();
        super.appendNull();
        return ErrorCode.None;
    }

    override clear(): void {
        super.clear();
        // Reset without reallocating — reuse the existing Uint32Array offsets/lengths
        // and SharedStringBuffer. This is critical for pool performance because
        // bufferPool.release() calls clear(), and recreating StringView would defeat pooling.
        this.stringView.resetForReuse();
    }

    /** Zero-copy append for parsing bytes directly into the StringView */
    appendBytes(data: Uint8Array, start: number, end: number): void {
        const slice = data.subarray(start, end);
        const len = this.length;
        // Only call ensureCapacity when we're about to overflow — the pool allocates
        // buffers at exactly chunkSize so this guard fires rarely during normal parsing.
        if (len >= this.stringView.capacity) {
            this.stringView.ensureCapacity(len + 1);
        }
        (this.stringView as any).appendBytes(slice);
        super.setLength(len + 1);
    }

    override append(value: any): import("../types/error.ts").ErrorCode {
        if (typeof value === "string") {
            this.stringView.append(value);
            super.setLength(super.length + 1);
        } else {
            super.append(value);
        }
        return ErrorCode.None;
    }

    /** Retrieve string value (materializes a JS string) */
    getString(index: number): string | undefined {
        return this.stringView.getString(index);
    }

    /** Retrieve raw bytes (zero-copy) */
    getBytes(index: number): Uint8Array | undefined {
        return this.stringView.getBytes(index);
    }

    override copySelected(
        src: ColumnBuffer,
        selection: Uint32Array,
        count: number,
    ): ErrorCode {
        if (!(src instanceof StringViewColumnBuffer)) {
            return ErrorCode.TypeMismatch;
        }

        // Ideally we'd share the SharedStringBuffer and copy offsets!
        // But for materialization, we can just append string values for now.
        for (let i = 0; i < count; i++) {
            const idx = selection[i]!;
            if (src.isNull(idx)) {
                this.appendNull();
            } else {
                const bytes = src.getBytes(idx);
                if (bytes) {
                    this.appendBytes(bytes, 0, bytes.length);
                } else {
                    this.appendNull();
                }
            }
        }
        return ErrorCode.None;
    }

    override ensureCapacity(minCapacity: number): void {
        this.stringView.ensureCapacity(minCapacity);
        // Note: we do NOT call super.ensureCapacity() because the base Uint8Array is size 0
        // and not used for actual data. The base _length (used for super.length) is maintained
        // separately via super.setLength().
    }
}
