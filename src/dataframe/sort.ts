import { enableNullTracking, getColumnValue, setColumnValue } from '../core/column';
import type { Result } from '../types/result';
import { err, ok } from '../types/result';
import { isNull, setNull } from '../utils/nulls';
import { type SortDirection, type SortSpec, createRowIndices, sortByColumns } from '../utils/sort';
import { type DataFrame, addColumn, createDataFrame, getColumn, getColumnNames } from './dataframe';

export interface SortOptions {
  columns: string[];
  directions?: SortDirection[];
}

export function sortDataFrame(df: DataFrame, options: SortOptions): Result<DataFrame, Error> {
  const { columns, directions } = options;
  const columnNames = getColumnNames(df);

  for (const name of columns) {
    if (!columnNames.includes(name)) {
      return err(new Error(`Column '${name}' not found in DataFrame`));
    }
  }

  const rowCount = columnNames.length === 0 ? 0 : (df.columns.get(columnNames[0]!)?.length ?? 0);
  const indices = createRowIndices(rowCount);
  const sortSpecs: SortSpec[] = [];

  for (let i = 0; i < columns.length; i++) {
    const name = columns[i];
    if (!name) continue;
    const colResult = getColumn(df, name);
    if (!colResult.ok) {
      return err(new Error(colResult.error));
    }
    sortSpecs.push({ column: colResult.data, direction: directions?.[i] ?? 'asc' });
  }

  sortByColumns(indices, sortSpecs);

  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary;

  for (const name of columnNames) {
    const sourceColResult = getColumn(df, name);
    if (!sourceColResult.ok) return err(new Error(sourceColResult.error));
    const sourceCol = sourceColResult.data;

    const addResult = addColumn(resultDf, name, sourceCol.dtype, rowCount);
    if (!addResult.ok) return err(new Error(addResult.error));

    const destColResult = getColumn(resultDf, name);
    if (!destColResult.ok) return err(new Error(destColResult.error));
    const destCol = destColResult.data;

    if (sourceCol.nullBitmap) {
      enableNullTracking(destCol);
    }

    for (let destIdx = 0; destIdx < rowCount; destIdx++) {
      const sourceIdx = indices[destIdx];
      if (sourceIdx === undefined) continue;

      if (sourceCol.nullBitmap && isNull(sourceCol.nullBitmap, sourceIdx)) {
        if (destCol.nullBitmap) setNull(destCol.nullBitmap, destIdx);
        continue;
      }

      const value = getColumnValue(sourceCol, sourceIdx);
      if (value !== undefined) {
        setColumnValue(destCol, destIdx, value);
      }
    }
  }

  return ok(resultDf);
}
