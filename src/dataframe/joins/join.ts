import { enableNullTracking, setColumnValue } from '../../core/column';
import { type Result, err, ok } from '../../types/result';
import {
  type DataFrame,
  addColumn,
  createDataFrame,
  getColumn,
  getColumnNames,
  getRowCount,
} from '../dataframe';
import { merge } from './merge';
import type { JoinType } from './types';

/**
 * Join two DataFrames on their index (row numbers)
 * This is a convenience wrapper around merge() that joins on row position
 *
 * @param left - Left DataFrame
 * @param right - Right DataFrame
 * @param options - Join options
 * @returns Result with joined DataFrame or error
 */
export function join(
  left: DataFrame,
  right: DataFrame,
  options?: {
    /** Join type */
    how?: JoinType;
    /** Suffixes for overlapping column names [left_suffix, right_suffix] */
    suffixes?: [string, string];
  },
): Result<DataFrame, Error> {
  const how = options?.how ?? 'left';
  const suffixes = options?.suffixes ?? ['_x', '_y'];

  // For index-based join, we need to create temporary index columns
  const leftRowCount = getRowCount(left);
  const rightRowCount = getRowCount(right);

  // Add temporary index column to both DataFrames
  const leftWithIndex = createDataFrame();
  leftWithIndex.dictionary = left.dictionary;

  // Add index column
  const leftIndexResult = addColumn(leftWithIndex, '__index__', 'int32', leftRowCount);
  if (!leftIndexResult.ok) {
    return err(new Error(leftIndexResult.error));
  }

  // Copy all existing columns
  for (const colName of getColumnNames(left)) {
    const col = getColumn(left, colName);
    if (!col.ok) continue;

    const addResult = addColumn(leftWithIndex, colName, col.data.dtype, leftRowCount);
    if (!addResult.ok) continue;

    const destCol = getColumn(leftWithIndex, colName);
    if (!destCol.ok) continue;

    const bytesPerElement = col.data.data.byteLength / col.data.length;
    for (let b = 0; b < col.data.data.byteLength; b++) {
      destCol.data.data[b] = col.data.data[b]!;
    }

    if (col.data.nullBitmap) {
      enableNullTracking(destCol.data);
      if (destCol.data.nullBitmap) {
        for (let b = 0; b < col.data.nullBitmap.data.byteLength; b++) {
          destCol.data.nullBitmap.data[b] = col.data.nullBitmap.data[b]!;
        }
      }
    }
  }

  // Fill index column
  const leftIndexCol = getColumn(leftWithIndex, '__index__');
  if (leftIndexCol.ok) {
    for (let i = 0; i < leftRowCount; i++) {
      setColumnValue(leftIndexCol.data, i, i);
    }
  }

  // Same for right DataFrame
  const rightWithIndex = createDataFrame();
  rightWithIndex.dictionary = right.dictionary;

  const rightIndexResult = addColumn(rightWithIndex, '__index__', 'int32', rightRowCount);
  if (!rightIndexResult.ok) {
    return err(new Error(rightIndexResult.error));
  }

  for (const colName of getColumnNames(right)) {
    const col = getColumn(right, colName);
    if (!col.ok) continue;

    const addResult = addColumn(rightWithIndex, colName, col.data.dtype, rightRowCount);
    if (!addResult.ok) continue;

    const destCol = getColumn(rightWithIndex, colName);
    if (!destCol.ok) continue;

    for (let b = 0; b < col.data.data.byteLength; b++) {
      destCol.data.data[b] = col.data.data[b]!;
    }

    if (col.data.nullBitmap) {
      enableNullTracking(destCol.data);
      if (destCol.data.nullBitmap) {
        for (let b = 0; b < col.data.nullBitmap.data.byteLength; b++) {
          destCol.data.nullBitmap.data[b] = col.data.nullBitmap.data[b]!;
        }
      }
    }
  }

  const rightIndexCol = getColumn(rightWithIndex, '__index__');
  if (rightIndexCol.ok) {
    for (let i = 0; i < rightRowCount; i++) {
      setColumnValue(rightIndexCol.data, i, i);
    }
  }

  // Perform merge on index columns
  const mergeResult = merge(leftWithIndex, rightWithIndex, {
    on: '__index__',
    how,
    suffixes,
  });

  if (!mergeResult.ok) {
    return mergeResult;
  }

  // Remove the temporary index column from result
  const resultDf = createDataFrame();
  resultDf.dictionary = mergeResult.data.dictionary;

  const resultRowCount = getRowCount(mergeResult.data);
  for (const colName of getColumnNames(mergeResult.data)) {
    if (colName === '__index__') continue; // Skip index column

    const col = getColumn(mergeResult.data, colName);
    if (!col.ok) continue;

    const addResult = addColumn(resultDf, colName, col.data.dtype, resultRowCount);
    if (!addResult.ok) continue;

    const destCol = getColumn(resultDf, colName);
    if (!destCol.ok) continue;

    for (let b = 0; b < col.data.data.byteLength; b++) {
      destCol.data.data[b] = col.data.data[b]!;
    }

    if (col.data.nullBitmap) {
      enableNullTracking(destCol.data);
      if (destCol.data.nullBitmap) {
        for (let b = 0; b < col.data.nullBitmap.data.byteLength; b++) {
          destCol.data.nullBitmap.data[b] = col.data.nullBitmap.data[b]!;
        }
      }
    }
  }

  return ok(resultDf);
}
