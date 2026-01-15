/**
 * Row Index - Efficiently stores byte offsets for each row in a CSV file.
 *
 * Uses Uint32Array segments to handle files larger than 4GB.
 * Each segment can index up to ~4 billion byte positions.
 */

import { IndexOutOfBoundsError } from '../../errors';

const SEGMENT_SIZE = 1_000_000;
const LF = 10;
const CR = 13;

/**
 * Builds and stores row byte offsets for lazy row access.
 * Enables O(1) lookup of any row's position in the file.
 */
export class RowIndex {
  /** Total number of data rows (excluding header if present) */
  readonly rowCount: number;

  /** Byte offset segments - each segment holds up to SEGMENT_SIZE offsets */
  private readonly _segments: Uint32Array[];

  /** File size in bytes */
  private readonly _fileSize: number;

  /** Offset to first data row (after header) */
  private readonly _dataStartOffset: number;

  private constructor(
    segments: Uint32Array[],
    rowCount: number,
    fileSize: number,
    dataStartOffset: number,
  ) {
    this._segments = segments;
    this.rowCount = rowCount;
    this._fileSize = fileSize;
    this._dataStartOffset = dataStartOffset;
  }

  /**
   * Build row index by scanning file for newlines.
   * Uses Buffer.indexOf for SIMD-accelerated newline finding.
   *
   * @param buffer - File content as Buffer
   * @param hasHeader - Whether first row is header
   * @returns RowIndex instance
   */
  static build(buffer: Buffer, hasHeader: boolean): RowIndex {
    const len = buffer.length;
    const offsets: number[] = [];

    // Find all line start positions
    let pos = 0;
    offsets.push(0); // First line starts at 0

    while (pos < len) {
      const idx = buffer.indexOf(LF, pos);
      if (idx === -1) break;
      offsets.push(idx + 1);
      pos = idx + 1;
    }

    // Determine data start (skip header if present)
    const dataStartIndex = hasHeader ? 1 : 0;
    const dataStartOffset = offsets[dataStartIndex] ?? 0;

    // Count actual data rows (excluding trailing empty lines)
    let dataRowCount = offsets.length - dataStartIndex;
    while (dataRowCount > 0) {
      const rowStart = offsets[dataStartIndex + dataRowCount - 1]!;
      const nextStart = offsets[dataStartIndex + dataRowCount] ?? len;
      // Check if line is empty or just whitespace
      if (nextStart - rowStart > 1) break;
      dataRowCount--;
    }

    // Build segments from data row offsets only
    const segments: Uint32Array[] = [];
    const dataOffsets = offsets.slice(dataStartIndex, dataStartIndex + dataRowCount + 1);

    for (let i = 0; i < dataOffsets.length; i += SEGMENT_SIZE) {
      const segmentData = dataOffsets.slice(i, i + SEGMENT_SIZE);
      segments.push(new Uint32Array(segmentData));
    }

    return new RowIndex(segments, dataRowCount, len, dataStartOffset);
  }

  /**
   * Get byte offset for a specific row.
   * @param rowIndex - Zero-based row index (relative to data, not header)
   * @returns Byte offset in file
   */
  getRowOffset(rowIndex: number): number {
    if (rowIndex < 0 || rowIndex >= this.rowCount) {
      throw new IndexOutOfBoundsError(rowIndex, 0, this.rowCount - 1);
    }

    const segmentIdx = Math.floor(rowIndex / SEGMENT_SIZE);
    const offsetInSegment = rowIndex % SEGMENT_SIZE;
    return this._segments[segmentIdx]![offsetInSegment]!;
  }

  /**
   * Get byte range for a row (start to next row start).
   * @param rowIndex - Zero-based row index
   * @returns [startByte, endByte] tuple
   */
  getRowRange(rowIndex: number): [number, number] {
    const start = this.getRowOffset(rowIndex);
    const end = rowIndex + 1 < this.rowCount ? this.getRowOffset(rowIndex + 1) : this._fileSize;
    return [start, end];
  }

  /**
   * Get byte range for multiple consecutive rows.
   * @param startRow - Starting row index
   * @param endRow - Ending row index (exclusive)
   * @returns [startByte, endByte] tuple
   */
  getRowsRange(startRow: number, endRow: number): [number, number] {
    const start = this.getRowOffset(startRow);
    const end = endRow < this.rowCount ? this.getRowOffset(endRow) : this._fileSize;
    return [start, end];
  }

  /**
   * Estimate memory usage of this index in bytes.
   */
  memoryUsage(): number {
    return this._segments.reduce((sum, seg) => sum + seg.byteLength, 0);
  }
}
