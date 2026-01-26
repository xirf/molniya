import * as fs from 'node:fs';
import { DType } from '../../types/dtypes';
import { type Result, err, ok } from '../../types/result';
import {
  type BinaryBlock,
  type BinaryReadResult,
  FORMAT_VERSION,
  type Footer,
  HEADER_SIZE,
  ID_TO_DTYPE,
  MAGIC_NUMBER,
  getBytesPerElement,
} from './types';

/**
 * Validate header and extract metadata
 */
function validateHeader(
  buffer: Uint8Array,
): Result<{ numBlocks: number; footerOffset: number }, Error> {
  if (buffer.length < HEADER_SIZE) {
    return err(new Error('File too small to contain valid header'));
  }

  const view = new DataView(buffer.buffer, buffer.byteOffset);

  // Check magic number
  for (let i = 0; i < 4; i++) {
    if (buffer[i] !== MAGIC_NUMBER[i]) {
      return err(new Error('Invalid magic number - not a .mbin file'));
    }
  }

  // Check version
  const version = view.getUint8(4);
  if (version !== FORMAT_VERSION) {
    return err(new Error(`Unsupported format version: ${version}`));
  }

  const numBlocks = view.getUint32(5, true);
  const footerOffset = Number(view.getBigUint64(9, true));

  return ok({ numBlocks, footerOffset });
}

/**
 * Deserialize footer
 */
function deserializeFooter(buffer: Uint8Array, footerOffset: number): Result<Footer, Error> {
  try {
    const view = new DataView(buffer.buffer, buffer.byteOffset);
    let offset = footerOffset;

    // Total rows
    const totalRows = Number(view.getBigUint64(offset, true));
    offset += 8;

    // Block index count
    const blockCount = view.getUint32(offset, true);
    offset += 4;

    // Block index entries
    const blockIndex: Footer['blockIndex'] = [];
    for (let i = 0; i < blockCount; i++) {
      const blockId = view.getUint32(offset, true);
      offset += 4;
      const fileOffset = Number(view.getBigUint64(offset, true));
      offset += 8;
      const rowCount = view.getUint32(offset, true);
      offset += 4;

      blockIndex.push({ blockId, fileOffset, rowCount });
    }

    // Column metadata count
    const columnCount = view.getUint32(offset, true);
    offset += 4;

    // Column metadata
    const decoder = new TextDecoder();
    const columnMetadata: Footer['columnMetadata'] = [];
    for (let i = 0; i < columnCount; i++) {
      const nameLength = view.getUint32(offset, true);
      offset += 4;
      const nameBytes = buffer.slice(offset, offset + nameLength);
      const name = decoder.decode(nameBytes);
      offset += nameLength;
      const dtypeId = view.getUint8(offset);
      const dtype = ID_TO_DTYPE[dtypeId];
      if (!dtype) {
        return err(new Error(`Unknown dtype ID: ${dtypeId}`));
      }
      offset += 1;
      const hasNulls = view.getUint8(offset) === 1;
      offset += 1;

      columnMetadata.push({ name, dtype, hasNulls });
    }

    return ok({ totalRows, blockIndex, columnMetadata });
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}

/**
 * Deserialize strings
 */
function deserializeStrings(
  buffer: Uint8Array,
  offset: number,
): { data: string[]; newOffset: number } {
  const view = new DataView(buffer.buffer, buffer.byteOffset);
  const decoder = new TextDecoder();
  let offsetLocal = offset;

  // Count
  const count = view.getUint32(offsetLocal, true);
  offsetLocal += 4;

  const strings: string[] = [];
  for (let i = 0; i < count; i++) {
    const length = view.getUint32(offsetLocal, true);
    offsetLocal += 4;
    const bytes = buffer.slice(offsetLocal, offsetLocal + length);
    strings.push(decoder.decode(bytes));
    offsetLocal += length;
  }

  return { data: strings, newOffset: offsetLocal };
}

/**
 * Deserialize a single block
 */
function deserializeBlock(
  buffer: Uint8Array,
  blockOffset: number,
  columnMetadata: Footer['columnMetadata'],
): Result<BinaryBlock, Error> {
  try {
    const view = new DataView(buffer.buffer, buffer.byteOffset);
    let offset = blockOffset;

    // Block metadata
    const blockId = view.getUint32(offset, true);
    offset += 4;
    const rowCount = view.getUint32(offset, true);
    offset += 4;
    offset += 4; // reserved
    offset += columnMetadata.length * 8; // skip column offsets

    // Read each column
    const columns: Record<string, BinaryBlock['columns'][string]> = {};

    for (const colMeta of columnMetadata) {
      // Column header
      const dtypeId = view.getUint8(offset);
      const dtype = ID_TO_DTYPE[dtypeId];
      if (!dtype) {
        return err(new Error(`Unknown dtype ID: ${dtypeId}`));
      }
      offset += 1;
      const hasNulls = view.getUint8(offset) === 1;
      offset += 1;
      offset += 2; // reserved

      // Column data
      let data: Int32Array | Float64Array | Uint8Array | BigInt64Array | string[];

      if (dtype === DType.String) {
        const stringResult = deserializeStrings(buffer, offset);
        data = stringResult.data;
        offset = stringResult.newOffset;
      } else {
        const bytesPerElement = getBytesPerElement(dtype);
        const dataLength = rowCount * bytesPerElement;
        const byteOffset = buffer.byteOffset + offset;

        if (dtype === DType.Int32) {
          if (byteOffset % Int32Array.BYTES_PER_ELEMENT === 0) {
            data = new Int32Array(buffer.buffer, byteOffset, rowCount);
          } else {
            const slice = buffer.slice(offset, offset + dataLength);
            const copy = new Uint8Array(slice);
            data = new Int32Array(copy.buffer, copy.byteOffset, rowCount);
          }
        } else if (dtype === DType.Float64) {
          if (byteOffset % Float64Array.BYTES_PER_ELEMENT === 0) {
            data = new Float64Array(buffer.buffer, byteOffset, rowCount);
          } else {
            const slice = buffer.slice(offset, offset + dataLength);
            const copy = new Uint8Array(slice);
            data = new Float64Array(copy.buffer, copy.byteOffset, rowCount);
          }
        } else if (dtype === DType.Date || dtype === DType.DateTime) {
          if (byteOffset % BigInt64Array.BYTES_PER_ELEMENT === 0) {
            data = new BigInt64Array(buffer.buffer, byteOffset, rowCount);
          } else {
            const slice = buffer.slice(offset, offset + dataLength);
            const copy = new Uint8Array(slice);
            data = new BigInt64Array(copy.buffer, copy.byteOffset, rowCount);
          }
        } else {
          data = new Uint8Array(buffer.buffer, byteOffset, dataLength);
        }

        offset += dataLength;
      }

      // Null bitmap if needed
      let nullBitmap: Uint8Array | undefined;
      if (hasNulls) {
        const bitmapSize = Math.ceil(rowCount / 8);
        nullBitmap = new Uint8Array(buffer.buffer, buffer.byteOffset + offset, bitmapSize);
        offset += bitmapSize;
      }

      columns[colMeta.name] = {
        dtype,
        data,
        hasNulls,
        nullBitmap,
      };
    }

    return ok({ blockId, rowCount, columns });
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}

/**
 * Read blocks from binary file
 */
export async function readBinaryBlocks(filePath: string): Promise<Result<BinaryReadResult, Error>> {
  try {
    const buffer = fs.readFileSync(filePath);

    // Validate header
    const headerValidation = validateHeader(buffer);
    if (!headerValidation.ok) {
      return err(headerValidation.error);
    }

    const { footerOffset } = headerValidation.data;

    // Read footer
    const footerResult = deserializeFooter(buffer, footerOffset);
    if (!footerResult.ok) {
      return err(footerResult.error);
    }

    const footer = footerResult.data;

    // Read blocks
    const blocks: BinaryBlock[] = [];
    for (const blockIdx of footer.blockIndex) {
      const blockResult = deserializeBlock(buffer, blockIdx.fileOffset, footer.columnMetadata);
      if (!blockResult.ok) {
        return err(blockResult.error);
      }
      blocks.push(blockResult.data);
    }

    // Build schema
    const schema: Record<string, DType> = {};
    for (const col of footer.columnMetadata) {
      schema[col.name] = col.dtype;
    }

    return ok({
      blocks,
      totalRows: footer.totalRows,
      schema,
    });
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}
