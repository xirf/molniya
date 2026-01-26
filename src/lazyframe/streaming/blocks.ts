import type { BinaryBlock } from '../../io/binary-format';
import type { ColumnarBatch, ColumnarBatchIterator } from '../../io/columnar-batch';
import type { DType } from '../../types/dtypes';

export function blocksToBatchIterator(
  blocks: BinaryBlock[],
  schema: Record<string, DType>,
): ColumnarBatchIterator {
  const columnOrder = Object.keys(schema);

  return {
    schema,
    columnOrder,
    batchSizeBytes: 0,
    async *[Symbol.asyncIterator](): AsyncGenerator<ColumnarBatch> {
      for (const block of blocks) {
        let byteSize = 0;
        for (const col of Object.values(block.columns)) {
          if (Array.isArray(col.data)) {
            byteSize += col.data.length * 4;
          } else {
            byteSize += col.data.byteLength;
          }
          if (col.hasNulls && col.nullBitmap) {
            byteSize += col.nullBitmap.byteLength;
          }
        }

        yield {
          rowCount: block.rowCount,
          byteSize,
          columns: block.columns,
        };
      }
    },
  };
}
