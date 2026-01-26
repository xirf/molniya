import { DType } from '../../types/dtypes';

// Magic number "MOLN" (Molniya)
export const MAGIC_NUMBER = new Uint8Array([0x4d, 0x4f, 0x4c, 0x4e]);
export const FORMAT_VERSION = 1;
export const HEADER_SIZE = 64;

// DType to numeric ID mapping for binary format
export const DTYPE_TO_ID: Record<DType, number> = {
  [DType.Int32]: 0,
  [DType.Float64]: 1,
  [DType.String]: 2,
  [DType.Bool]: 3,
  [DType.DateTime]: 4,
  [DType.Date]: 5,
};

export const ID_TO_DTYPE: Record<number, DType> = {
  0: DType.Int32,
  1: DType.Float64,
  2: DType.String,
  3: DType.Bool,
  4: DType.DateTime,
  5: DType.Date,
};

/**
 * Column data in a block
 */
export interface BlockColumn {
  dtype: DType;
  data: Int32Array | Float64Array | Uint8Array | BigInt64Array | string[];
  hasNulls: boolean;
  nullBitmap?: Uint8Array;
}

/**
 * A single block of columnar data (~512KB target)
 */
export interface BinaryBlock {
  blockId: number;
  rowCount: number;
  columns: Record<string, BlockColumn>;
}

/**
 * Metadata for reading blocks
 */
export interface BlockIndex {
  blockId: number;
  fileOffset: number;
  rowCount: number;
}

/**
 * Footer structure
 */
export interface Footer {
  totalRows: number;
  blockIndex: BlockIndex[];
  columnMetadata: Array<{
    name: string;
    dtype: DType;
    hasNulls: boolean;
  }>;
}

/**
 * Result of reading binary file
 */
export interface BinaryReadResult {
  blocks: BinaryBlock[];
  totalRows: number;
  schema: Record<string, DType>;
}

/**
 * Get bytes per element for a dtype
 */
export function getBytesPerElement(dtype: DType): number {
  switch (dtype) {
    case DType.Int32:
      return 4;
    case DType.Float64:
    case DType.DateTime:
    case DType.Date:
      return 8;
    case DType.Bool:
      return 1;
    default:
      throw new Error(`Unknown dtype: ${dtype}`);
  }
}
