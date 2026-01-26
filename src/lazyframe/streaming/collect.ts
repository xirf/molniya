import { enableNullTracking, resizeColumn, setColumnValue } from '../../core/column';
import {
  type DataFrame,
  addColumn,
  createDataFrame,
  getColumn,
  getRowCount,
} from '../../dataframe/dataframe';
import type { ColumnarBatch, ColumnarBatchColumn } from '../../io/columnar-batch';
import { internString } from '../../memory/dictionary';
import { DType, getDTypeSize } from '../../types/dtypes';
import type { Result } from '../../types/result';
import { err, ok } from '../../types/result';
import { resizeNullBitmap, setNull } from '../../utils/nulls';
import { isBatchNull } from './batch-utils';

function appendBatchColumn(
  df: DataFrame,
  colName: string,
  batchColumn: ColumnarBatchColumn,
  startRow: number,
  batchRowCount: number,
): Result<void, Error> {
  const colResult = getColumn(df, colName);
  if (!colResult.ok) {
    return err(new Error(colResult.error));
  }

  const column = colResult.data;
  const dtype = column.dtype;

  if (dtype !== batchColumn.dtype) {
    return err(new Error(`Column dtype mismatch for ${colName}`));
  }

  if (dtype === DType.String) {
    if (batchColumn.hasNulls) {
      enableNullTracking(column);
    }

    const data = batchColumn.data as string[];
    for (let i = 0; i < batchRowCount; i++) {
      const rowIndex = startRow + i;
      if (
        batchColumn.hasNulls &&
        batchColumn.nullBitmap &&
        isBatchNull(batchColumn.nullBitmap, i)
      ) {
        if (column.nullBitmap) {
          setNull(column.nullBitmap, rowIndex);
        }
        continue;
      }

      const str = data[i] ?? '';
      const dictId = df.dictionary ? internString(df.dictionary, str) : 0;
      setColumnValue(column, rowIndex, dictId);
    }

    return ok(undefined);
  }

  const bytesPerElement = getDTypeSize(dtype);
  const data = batchColumn.data as Int32Array | Float64Array | Uint8Array | BigInt64Array;
  const bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  const destOffset = startRow * bytesPerElement;
  column.data.set(bytes, destOffset);

  if (batchColumn.hasNulls && batchColumn.nullBitmap) {
    enableNullTracking(column);
    for (let i = 0; i < batchRowCount; i++) {
      if (isBatchNull(batchColumn.nullBitmap, i)) {
        if (column.nullBitmap) {
          setNull(column.nullBitmap, startRow + i);
        }
      }
    }
  }

  return ok(undefined);
}

function ensureColumnsForBatch(
  df: DataFrame,
  batch: ColumnarBatch,
  newRowCount: number,
): Result<void, Error> {
  if (getRowCount(df) === 0) {
    for (const [name, col] of Object.entries(batch.columns)) {
      const addResult = addColumn(df, name, col.dtype, newRowCount);
      if (!addResult.ok) {
        return err(new Error(addResult.error));
      }
    }
    return ok(undefined);
  }

  for (const [name] of Object.entries(batch.columns)) {
    const colResult = getColumn(df, name);
    if (!colResult.ok) {
      return err(new Error(colResult.error));
    }

    const existing = colResult.data;
    const resizeResult = resizeColumn(existing, newRowCount);
    if (!resizeResult.ok) {
      return err(new Error(resizeResult.error));
    }

    const resized = resizeResult.data;
    if (existing.nullBitmap) {
      resized.nullBitmap = resizeNullBitmap(existing.nullBitmap, newRowCount);
    }

    df.columns.set(name, resized);
  }

  return ok(undefined);
}

export async function collectBatchesToDataFrame(
  iterator: AsyncIterable<ColumnarBatch>,
): Promise<Result<DataFrame, Error>> {
  try {
    const df = createDataFrame();

    for await (const batch of iterator) {
      const existingRows = getRowCount(df);
      const newRowCount = existingRows + batch.rowCount;

      const ensureResult = ensureColumnsForBatch(df, batch, newRowCount);
      if (!ensureResult.ok) return ensureResult;

      for (const [name, column] of Object.entries(batch.columns)) {
        const appendResult = appendBatchColumn(df, name, column, existingRows, batch.rowCount);
        if (!appendResult.ok) return appendResult;
      }
    }

    return ok(df);
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}
