import * as fs from 'node:fs';
import { DType } from '../../types/dtypes';
import { type Result, err, ok } from '../../types/result';
import {
  type BinaryBlock,
  type BlockColumn,
  type BlockIndex,
  DTYPE_TO_ID,
  FORMAT_VERSION,
  type Footer,
  HEADER_SIZE,
  MAGIC_NUMBER,
} from './types';

/**
 * Create file header
 */
function createHeader(numBlocks: number, footerOffset: number): Uint8Array {
  const buffer = new Uint8Array(HEADER_SIZE);
  const view = new DataView(buffer.buffer);

  // Magic number (4 bytes)
  buffer.set(MAGIC_NUMBER, 0);

  // Version (1 byte)
  view.setUint8(4, FORMAT_VERSION);

  // Number of blocks (4 bytes)
  view.setUint32(5, numBlocks, true);

  // Footer offset (8 bytes)
  view.setBigUint64(9, BigInt(footerOffset), true);

  // Reserved (47 bytes)
  return buffer;
}

/**
 * Serialize block metadata
 */
function serializeBlockMetadata(block: BinaryBlock): Uint8Array {
  const numColumns = Object.keys(block.columns).length;
  const bufferSize = 12 + numColumns * 8; // blockId(4) + rowCount(4) + reserved(4) + offsets(8*n)
  const buffer = new Uint8Array(bufferSize);
  const view = new DataView(buffer.buffer);

  view.setUint32(0, block.blockId, true);
  view.setUint32(4, block.rowCount, true);
  view.setUint32(8, 0, true); // reserved

  // Column offsets (will be updated by caller if needed)
  let offset = 12;
  for (let i = 0; i < numColumns; i++) {
    view.setBigUint64(offset, BigInt(0), true);
    offset += 8;
  }

  return buffer;
}

/**
 * Serialize array of strings (length-prefixed)
 */
function serializeStrings(strings: string[]): Uint8Array {
  const encoder = new TextEncoder();
  const parts: Uint8Array[] = [];

  // Count header (4 bytes)
  const countBuffer = new Uint8Array(4);
  new DataView(countBuffer.buffer).setUint32(0, strings.length, true);
  parts.push(countBuffer);

  // Each string: length(4) + utf8_bytes
  for (const str of strings) {
    const bytes = encoder.encode(str);
    const lengthBuffer = new Uint8Array(4);
    new DataView(lengthBuffer.buffer).setUint32(0, bytes.length, true);
    parts.push(lengthBuffer);
    parts.push(bytes);
  }

  // Combine
  const totalLength = parts.reduce((sum, p) => sum + p.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }

  return result;
}

/**
 * Serialize a single column
 */
function serializeColumn(column: BlockColumn): Uint8Array {
  const parts: Uint8Array[] = [];

  // Column header: dtype(1) + hasNulls(1) + reserved(2)
  const header = new Uint8Array(4);
  const headerView = new DataView(header.buffer);
  headerView.setUint8(0, DTYPE_TO_ID[column.dtype] ?? 0);
  headerView.setUint8(1, column.hasNulls ? 1 : 0);
  parts.push(header);

  // Column data
  if (column.dtype === DType.String && Array.isArray(column.data)) {
    // String data (inline, length-prefixed)
    const stringData = serializeStrings(column.data);
    parts.push(stringData);
  } else {
    // TypedArray data
    const data = column.data as Int32Array | Float64Array | Uint8Array | BigInt64Array;
    const arrayData = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    parts.push(arrayData);
  }

  // Null bitmap if needed
  if (column.hasNulls && column.nullBitmap) {
    parts.push(column.nullBitmap);
  }

  // Combine all parts
  const totalLength = parts.reduce((sum, p) => sum + p.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }

  return result;
}

/**
 * Create footer
 */
function createFooter(blocks: BinaryBlock[], blockIndex: BlockIndex[]): Footer {
  const totalRows = blocks.reduce((sum, b) => sum + b.rowCount, 0);

  // Extract column metadata from first block
  const firstBlock = blocks[0];
  if (!firstBlock) {
    throw new Error('No blocks to write');
  }

  const columnMetadata = Object.entries(firstBlock.columns).map(([name, col]) => ({
    name,
    dtype: col.dtype,
    hasNulls: col.hasNulls,
  }));

  return {
    totalRows,
    blockIndex,
    columnMetadata,
  };
}

/**
 * Serialize footer
 */
function serializeFooter(footer: Footer): Uint8Array {
  const encoder = new TextEncoder();

  // Estimate size (rough)
  const estimatedSize = 8 + footer.blockIndex.length * 16 + footer.columnMetadata.length * 100;
  const buffer = new Uint8Array(estimatedSize * 2); // Extra space
  const view = new DataView(buffer.buffer);
  let offset = 0;

  // Total rows (8 bytes)
  view.setBigUint64(offset, BigInt(footer.totalRows), true);
  offset += 8;

  // Block index count (4 bytes)
  view.setUint32(offset, footer.blockIndex.length, true);
  offset += 4;

  // Block index entries
  for (const idx of footer.blockIndex) {
    view.setUint32(offset, idx.blockId, true);
    offset += 4;
    view.setBigUint64(offset, BigInt(idx.fileOffset), true);
    offset += 8;
    view.setUint32(offset, idx.rowCount, true);
    offset += 4;
  }

  // Column metadata count (4 bytes)
  view.setUint32(offset, footer.columnMetadata.length, true);
  offset += 4;

  // Column metadata entries
  for (const col of footer.columnMetadata) {
    const nameBytes = encoder.encode(col.name);
    view.setUint32(offset, nameBytes.length, true);
    offset += 4;
    buffer.set(nameBytes, offset);
    offset += nameBytes.length;
    view.setUint8(offset, DTYPE_TO_ID[col.dtype] ?? 0);
    offset += 1;
    view.setUint8(offset, col.hasNulls ? 1 : 0);
    offset += 1;
  }

  return buffer.slice(0, offset);
}

/**
 * Write blocks to binary file
 */
export async function writeBinaryBlocks(
  filePath: string,
  blocks: BinaryBlock[],
): Promise<Result<void, Error>> {
  try {
    const fd = fs.openSync(filePath, 'w');

    try {
      // Reserve space for header
      const headerBuffer = new Uint8Array(HEADER_SIZE);
      fs.writeSync(fd, headerBuffer);
      let currentOffset = HEADER_SIZE;

      const blockIndex: BlockIndex[] = [];

      // Write each block
      for (const block of blocks) {
        const blockStartOffset = currentOffset;

        // Write block metadata
        const blockMetadata = serializeBlockMetadata(block);
        fs.writeSync(fd, blockMetadata);
        currentOffset += blockMetadata.length;

        // Write each column
        for (const column of Object.values(block.columns)) {
          const columnData = serializeColumn(column);
          fs.writeSync(fd, columnData);
          currentOffset += columnData.length;
        }

        blockIndex.push({
          blockId: block.blockId,
          fileOffset: blockStartOffset,
          rowCount: block.rowCount,
        });
      }

      // Write footer
      const footer = createFooter(blocks, blockIndex);
      const footerData = serializeFooter(footer);
      const footerOffset = currentOffset;
      fs.writeSync(fd, footerData);

      // Write header with footer offset
      const header = createHeader(blocks.length, footerOffset);
      fs.writeSync(fd, header, 0, HEADER_SIZE, 0);

      return ok(undefined);
    } finally {
      fs.closeSync(fd);
    }
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}

/**
 * Streaming block writer for .mbin files
 */
export class BinaryBlockWriter {
  private readonly filePath: string;
  private fd: number | null;
  private currentOffset: number;
  private blockIndex: BlockIndex[] = [];
  private totalRows = 0;
  private numBlocks = 0;
  private firstBlock: BinaryBlock | null = null;

  constructor(filePath: string) {
    this.filePath = filePath;
    this.fd = fs.openSync(filePath, 'w');
    const headerBuffer = new Uint8Array(HEADER_SIZE);
    fs.writeSync(this.fd, headerBuffer);
    this.currentOffset = HEADER_SIZE;
  }

  writeBlock(block: BinaryBlock): Result<void, Error> {
    try {
      if (!this.fd) return err(new Error('Writer is closed'));

      if (!this.firstBlock) {
        this.firstBlock = block;
      }

      const blockStartOffset = this.currentOffset;
      const blockMetadata = serializeBlockMetadata(block);
      fs.writeSync(this.fd, blockMetadata);
      this.currentOffset += blockMetadata.length;

      for (const column of Object.values(block.columns)) {
        const columnData = serializeColumn(column);
        fs.writeSync(this.fd, columnData);
        this.currentOffset += columnData.length;
      }

      this.blockIndex.push({
        blockId: block.blockId,
        fileOffset: blockStartOffset,
        rowCount: block.rowCount,
      });

      this.totalRows += block.rowCount;
      this.numBlocks += 1;

      return ok(undefined);
    } catch (error) {
      return err(error instanceof Error ? error : new Error(String(error)));
    }
  }

  finalize(): Result<void, Error> {
    try {
      if (!this.fd) return err(new Error('Writer is closed'));
      if (!this.firstBlock) {
        return err(new Error('No blocks written'));
      }

      const columnMetadata = Object.entries(this.firstBlock.columns).map(([name, col]) => ({
        name,
        dtype: col.dtype,
        hasNulls: col.hasNulls,
      }));

      const footer: Footer = {
        totalRows: this.totalRows,
        blockIndex: this.blockIndex,
        columnMetadata,
      };

      const footerData = serializeFooter(footer);
      const footerOffset = this.currentOffset;
      fs.writeSync(this.fd, footerData);

      const header = createHeader(this.numBlocks, footerOffset);
      fs.writeSync(this.fd, header, 0, HEADER_SIZE, 0);

      fs.closeSync(this.fd);
      this.fd = null;
      return ok(undefined);
    } catch (error) {
      return err(error instanceof Error ? error : new Error(String(error)));
    }
  }

  abort(deleteFile = true): void {
    if (this.fd) {
      try {
        fs.closeSync(this.fd);
      } catch {
        // ignore
      }
      this.fd = null;
    }
    if (deleteFile) {
      try {
        fs.unlinkSync(this.filePath);
      } catch {
        // ignore
      }
    }
  }
}
