import { enableNullTracking, getColumnValue, setColumnValue } from '../../core/column';
import { getString, internString } from '../../memory/dictionary';
import { DType } from '../../types/dtypes';
import { type Result, err, ok } from '../../types/result';
import { isNull, setNull } from '../../utils/nulls';
import {
  type DataFrame,
  addColumn,
  createDataFrame,
  getColumn,
  getColumnNames,
  getRowCount,
} from '../dataframe';

/**
 * Strip whitespace from strings in a column
 * Returns a new DataFrame with trimmed strings
 *
 * @param df - Source DataFrame
 * @param columnName - Name of string column to transform
 * @returns Result with new DataFrame or error
 */
export function strStrip(df: DataFrame, columnName: string): Result<DataFrame, Error> {
  const colResult = getColumn(df, columnName);
  if (!colResult.ok) {
    return err(new Error(colResult.error));
  }

  const sourceCol = colResult.data;
  if (sourceCol.dtype !== DType.String) {
    return err(new Error(`Column '${columnName}' must be String type for str operations`));
  }

  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary;

  const rowCount = getRowCount(df);
  for (const colName of getColumnNames(df)) {
    const col = getColumn(df, colName);
    if (!col.ok) continue;

    const addResult = addColumn(resultDf, colName, col.data.dtype, rowCount);
    if (!addResult.ok) continue;

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) continue;
    const destCol = destColResult.data;

    if (col.data.nullBitmap) {
      enableNullTracking(destCol);
    }

    if (colName === columnName) {
      for (let i = 0; i < rowCount; i++) {
        if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, i)) {
          if (destCol.nullBitmap) setNull(destCol.nullBitmap, i);
          continue;
        }

        const dictId = getColumnValue(sourceCol, i);
        if (dictId === undefined) continue;

        const str = getString(df.dictionary!, Number(dictId));
        const transformed = str!.trim();
        const newId = internString(resultDf.dictionary!, transformed);

        setColumnValue(destCol, i, newId);
      }
    } else {
      const bytesPerElement = col.data.data.byteLength / col.data.length;
      const totalBytes = rowCount * bytesPerElement;

      for (let b = 0; b < totalBytes; b++) {
        destCol.data[b] = col.data.data[b]!;
      }

      if (col.data.nullBitmap && destCol.nullBitmap) {
        const bitmapBytes = col.data.nullBitmap.data.byteLength;
        for (let b = 0; b < bitmapBytes; b++) {
          destCol.nullBitmap.data[b] = col.data.nullBitmap.data[b]!;
        }
      }
    }
  }

  return ok(resultDf);
}
