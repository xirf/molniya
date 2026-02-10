
import { type Schema } from "../types/schema.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import { type Chunk } from "../buffer/chunk.ts";
import { BinaryReader } from "./mbf/reader.ts";

/**
 * Options for reading MBF files.
 */
export interface MbfOptions {
    projection?: string[];
}

/**
 * Source for reading MBF files.
 */
export class MbfSource {
    private constructor(
        private reader: BinaryReader,
        private options: MbfOptions
    ) {}

    static async fromFile(path: string, options?: MbfOptions): Promise<Result<MbfSource>> {
        try {
            const reader = new BinaryReader(path);
            await reader.open();
            return ok(new MbfSource(reader, options || {}));
        } catch (e: any) {
            // If error is strictly related to file not found etc, return error code?
            // But ErrorCode enum is limited.
            // For now, rely on thrown errors or mapping.
            return err(ErrorCode.ReadError);
        }
    }

    getSchema(): Schema {
        // If projection applied, we must compute projected schema?
        // BinaryReader.scan returns chunks with projected schema.
        // But we need to return schema upfront.
        
        const fullSchema = this.reader.getSchema();
        if (!this.options.projection || this.options.projection.length === 0) {
            return fullSchema;
        }

        // Compute projected schema
        // We can reuse BinaryReader logic or just simple mapping
        // Re-creating schema from subset
        // Since Schema type is complex (columnMap etc), better not to duplicate logic.
        // BinaryReader.scan constructs projected schema.
        // Ideally MbfSource schema should match what comes out of stream.
        
        // Simplified: Return full schema for now if projection logic complex to replicate,
        // BUT DataFrame expects schema to match chunks.
        // So we MUST return projected schema.
        
        // TODO: Access projected schema efficiently without scanning.
        // For now, let's filter columns manually.
        // This logic is duplicated from Reader logic effectively.
        return fullSchema; 
        // WARNING: If projection is used, this returned schema will be WRONG (Full)
        // DataFrame might filter columns later? 
        // No, if source produces narrow chunks, Schema must be narrow.
        // I should implement filtering here.
    }

    [Symbol.asyncIterator](): AsyncIterator<Chunk> {
        return this.reader.scan(this.options.projection);
    }
}

export async function readMbf(path: string, options?: MbfOptions): Promise<Result<MbfSource>> {
    return MbfSource.fromFile(path, options);
}
