import { DTypeKind } from "../../types/dtypes.ts";

export const MBF_MAGIC = 0x4d4c4e59; // "MLNY"
export const MBF_VERSION = 1;

/**
 * File Header Structure (16 bytes)
 */
export interface FileHeader {
    magic: number;      // 4 bytes: 0x4d4c4e59
    version: number;    // 4 bytes: 1
    rowCount: number;   // 4 bytes: uint32
    colCount: number;   // 4 bytes: uint32
}

/**
 * Column Directory Entry
 */
export interface ColumnEntry {
    nameLength: number; // 2 bytes: uint16
    name: string;       // Variable: UTF-8
    kind: DTypeKind;    // 1 byte: uint8
    dataOffset: bigint; // 8 bytes: uint64 (Absolute)
    dataLength: bigint; // 8 bytes: uint64
}
