/**
 * Row Index - Efficiently stores byte offsets for each row in a CSV file.
 */

import { IndexOutOfBoundsError } from '../../errors';

const SEGMENT_SIZE = 1_000_000;
const LF = 10;
const DEFAULT_STRIDE = 100;

/**
 * Builds and stores row byte offsets for lazy row access.
 * Uses a sparse index (storing every Nth row) to minimize memory usage for large files.
 */
export class RowIndex {
  readonly rowCount: number;
  readonly stride: number;

  private readonly _segments: Float64Array[];
  private readonly _fileSize: number;
  private readonly _dataStartOffset: number;

  private constructor(
    segments: Float64Array[],
    rowCount: number,
    fileSize: number,
    dataStartOffset: number,
    stride: number,
  ) {
    this._segments = segments;
    this.rowCount = rowCount;
    this._fileSize = fileSize;
    this._dataStartOffset = dataStartOffset;
    this.stride = stride;
  }

  static async build(
    file: ReturnType<typeof Bun.file>,
    hasHeader: boolean,
    stride = DEFAULT_STRIDE,
  ): Promise<RowIndex> {
    const fileSize = file.size;
    const CHUNK_SIZE = 32 * 1024 * 1024; // 32MB chunks

    const segments: Float64Array[] = [];
    let currentSegment = new Float64Array(SEGMENT_SIZE);
    let countInSegment = 0;

    // total logical rows seen (including potential header)
    let totalRowCount = 0;

    // Helper to store offset if it matches stride
    const maybePushOffset = (offset: number) => {
      if (totalRowCount % stride === 0) {
        if (countInSegment === SEGMENT_SIZE) {
          segments.push(currentSegment);
          currentSegment = new Float64Array(SEGMENT_SIZE);
          countInSegment = 0;
        }
        currentSegment[countInSegment++] = offset;
      }
      totalRowCount++;
    };

    // Always push 0 as start of file (logical row 0)
    maybePushOffset(0);

    let currentOffset = 0;
    while (currentOffset < fileSize) {
      const readSize = Math.min(CHUNK_SIZE, fileSize - currentOffset);
      const buffer = Buffer.from(
        await file.slice(currentOffset, currentOffset + readSize).arrayBuffer(),
      );

      let posInChunk = 0;
      while (true) {
        const idx = buffer.indexOf(LF, posInChunk);
        if (idx === -1) break;

        const globalIdx = currentOffset + idx + 1;
        if (globalIdx < fileSize) {
          maybePushOffset(globalIdx);
        }
        posInChunk = idx + 1;
      }
      currentOffset += readSize;
    }

    if (countInSegment > 0) {
      segments.push(currentSegment.slice(0, countInSegment));
    }

    // Determine data start
    // Logic:
    // If hasHeader=true, row 0 is header. data starts at row 1.
    // If hasHeader=false, data starts at row 0.

    const dataStartIndex = hasHeader ? 1 : 0;
    const dataRowCount = Math.max(0, totalRowCount - dataStartIndex);

    // We need to know the offset of the first DATA row to skip header properly.
    // Since we only stored every Nth row, we might not have the exact offset for row 1.
    // BUT we pushed row 0 (offset 0). Row 1 (header end) might be at offset X.
    // If stride > 1, we likely didn't store row 1 unless stride=1.
    // However, for parsing, we just need to tell the parser "start at offset X and skip Y rows".
    // Or simpler: We always treat the file as raw rows and handling header skipping during parsing?
    // Current `RowIndex` design tries to abstract away the header.
    //
    // Let's find the approximate data start.
    // We know row 0 is at offset 0.
    // If we want row `dataStartIndex`, we look for the checkpoint <= `dataStartIndex`.
    // Since stride is usually 100, checkpoint 0 is at row 0.
    // So for row 1, we start at row 0 (offset 0) and skip 1 row.

    return new RowIndex(segments, dataRowCount, fileSize, 0, stride); // dataStartOffset not strictly needed if we compute loose ranges
  }

  /**
   * Get efficient range for reading.
   * Returns [startOffset, endOffset, skipRowsStart]
   * - startOffset: A byte offset <= the actual start of startRow
   * - endOffset: A byte offset >= the actual end of endRow
   * - skipRowsStart: How many rows to skip from the beginning of the returned buffer to reach startRow
   */
  getLooseRange(startRow: number, endRow: number, hasHeader = true): [number, number, number] {
    // logical indices in the file (0-based including header)
    const logicalStart = startRow + (hasHeader ? 1 : 0);
    const logicalEnd = endRow + (hasHeader ? 1 : 0);

    // Find stored checkpoint <= logicalStart
    // Checkpoint index = floor(logicalStart / stride)
    const cpStartIdx = Math.floor(logicalStart / this.stride);
    const startOffset = this._getCheckpointOffset(cpStartIdx);

    // Rows to skip after startOffset
    // stored row check point is (cpStartIdx * stride)
    const skipRows = logicalStart - cpStartIdx * this.stride;

    // Find stored checkpoint >= logicalEnd
    // We need to cover up to logicalEnd.
    // Checkpoint index = ceil(logicalEnd / stride)
    const cpEndIdx = Math.ceil(logicalEnd / this.stride);
    const endOffset = this._getCheckpointOffset(cpEndIdx); // might return fileSize if out of bounds

    return [startOffset, endOffset, skipRows];
  }

  private _getCheckpointOffset(idx: number): number {
    const segIdx = Math.floor(idx / SEGMENT_SIZE);
    const offIdx = idx % SEGMENT_SIZE;

    if (segIdx >= this._segments.length) return this._fileSize;

    const seg = this._segments[segIdx];
    if (!seg || offIdx >= seg.length) return this._fileSize;

    return seg[offIdx]!;
  }

  getRowOffset(rowIndex: number): number {
    throw new Error('Exact getRowOffset not supported in Sparse Index. Use getLooseRange.');
  }

  getRowsRange(startRow: number, endRow: number): [number, number] {
    throw new Error('Exact getRowsRange not supported in Sparse Index. Use getLooseRange.');
  }

  memoryUsage(): number {
    return this._segments.reduce((sum, seg) => sum + seg.byteLength, 0);
  }
}
