import { enableNullTracking, getColumnValue, setColumnValue } from '../../core/column';
import { getString } from '../../memory/dictionary';
import { DType } from '../../types/dtypes';
import { type Result, err, ok } from '../../types/result';
import { isNull, setNull } from '../../utils/nulls';
import { type DataFrame, addColumn, createDataFrame, getColumn, getRowCount } from '../dataframe';

/**
 * Get length of strings
 * Returns a new DataFrame with Int32 column containing string lengths
 *
 * @param df - Source DataFrame
 * @param columnName - Name of string column
 * @returns Result with new DataFrame or error
 */
export function strLen(df: DataFrame, columnName: string): Result<DataFrame, Error> {
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
  const resultColName = `${columnName}_len`;
  const addResult = addColumn(resultDf, resultColName, DType.Int32, rowCount);
  if (!addResult.ok) {
    return err(new Error(addResult.error));
  }

  const destColResult = getColumn(resultDf, resultColName);
  if (!destColResult.ok) {
    return err(new Error(destColResult.error));
  }
  const destCol = destColResult.data;

  if (sourceCol.nullBitmap) {
    enableNullTracking(destCol);
  }

  for (let i = 0; i < rowCount; i++) {
    if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, i)) {
      if (destCol.nullBitmap) setNull(destCol.nullBitmap, i);
      continue;
    }

    const dictId = getColumnValue(sourceCol, i);
    if (dictId === undefined) continue;

    const str = getString(df.dictionary!, Number(dictId))!;
    setColumnValue(destCol, i, str.length);
  }

  return ok(resultDf);
}
