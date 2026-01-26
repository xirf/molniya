import { enableNullTracking } from '../../core/column';
import { getDTypeSize } from '../../types/dtypes';
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
 * Concatenate DataFrames vertically (row-wise) or horizontally (column-wise)
 * Returns a new DataFrame with combined data
 *
 * @param dfs - Array of DataFrames to concatenate
 * @param options - Concatenation options
 * @returns Result with concatenated DataFrame or error
 */
export function concat(
  dfs: DataFrame[],
  options?: {
    /** Concatenation axis: 0=rows (vertical), 1=columns (horizontal) */
    axis?: 0 | 1;
    /** Whether to ignore index and reset it */
    ignoreIndex?: boolean;
  },
): Result<DataFrame, Error> {
  if (dfs.length === 0) {
    return err(new Error('Cannot concatenate empty array of DataFrames'));
  }

  if (dfs.length === 1) {
    return ok(dfs[0]!);
  }

  const axis = options?.axis ?? 0;

  if (axis === 0) {
    return concatRows(dfs);
  }
  return concatColumns(dfs);
}

/**
 * Concatenate DataFrames vertically (row-wise)
 */
function concatRows(dfs: DataFrame[]): Result<DataFrame, Error> {
  // Validate all DataFrames have same columns
  const firstCols = getColumnNames(dfs[0]!);
  const firstColSet = new Set(firstCols);

  for (let i = 1; i < dfs.length; i++) {
    const dfCols = getColumnNames(dfs[i]!);
    if (dfCols.length !== firstCols.length) {
      return err(new Error(`DataFrame ${i} has different number of columns`));
    }

    for (const col of dfCols) {
      if (!firstColSet.has(col)) {
        return err(new Error(`DataFrame ${i} has different columns`));
      }
    }
  }

  // Calculate total row count
  const totalRows = dfs.reduce((sum, df) => sum + getRowCount(df), 0);

  // Create result DataFrame
  const resultDf = createDataFrame();
  resultDf.dictionary = dfs[0]!.dictionary; // Share dictionary

  // For each column, concatenate all values
  for (const colName of firstCols) {
    const firstColResult = getColumn(dfs[0]!, colName);
    if (!firstColResult.ok) {
      return err(new Error(firstColResult.error));
    }

    const firstCol = firstColResult.data;
    const addResult = addColumn(resultDf, colName, firstCol.dtype, totalRows);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, colName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }

    const destCol = destColResult.data;

    // Check if any source has null bitmap
    const hasNullBitmap = dfs.some((df) => {
      const col = getColumn(df, colName);
      return col.ok && col.data.nullBitmap !== undefined;
    });

    if (hasNullBitmap) {
      enableNullTracking(destCol);
    }

    const bytesPerElement = getDTypeSize(firstCol.dtype);
    let destRow = 0;

    // Copy from each DataFrame
    for (const df of dfs) {
      const sourceColResult = getColumn(df, colName);
      if (!sourceColResult.ok) {
        return err(new Error(sourceColResult.error));
      }

      const sourceCol = sourceColResult.data;
      const sourceRowCount = sourceCol.length;

      // Copy all rows
      for (let sourceRow = 0; sourceRow < sourceRowCount; sourceRow++) {
        const sourceOffset = sourceRow * bytesPerElement;
        const destOffset = destRow * bytesPerElement;

        // Copy bytes
        for (let b = 0; b < bytesPerElement; b++) {
          destCol.data[destOffset + b] = sourceCol.data[sourceOffset + b]!;
        }

        // Copy null status
        if (sourceCol.nullBitmap && destCol.nullBitmap) {
          if (isNull(sourceCol.nullBitmap, sourceRow)) {
            setNull(destCol.nullBitmap, destRow);
          }
        }

        destRow++;
      }
    }
  }

  return ok(resultDf);
}

/**
 * Concatenate DataFrames horizontally (column-wise)
 */
function concatColumns(dfs: DataFrame[]): Result<DataFrame, Error> {
  // Validate all DataFrames have same row count
  const firstRowCount = getRowCount(dfs[0]!);

  for (let i = 1; i < dfs.length; i++) {
    const rowCount = getRowCount(dfs[i]!);
    if (rowCount !== firstRowCount) {
      return err(new Error(`DataFrame ${i} has ${rowCount} rows, expected ${firstRowCount}`));
    }
  }

  // Check for duplicate column names
  const allColumns: string[] = [];
  for (const df of dfs) {
    allColumns.push(...getColumnNames(df));
  }

  const columnSet = new Set<string>();
  for (const col of allColumns) {
    if (columnSet.has(col)) {
      return err(new Error(`Duplicate column name: '${col}'`));
    }
    columnSet.add(col);
  }

  // Create result DataFrame
  const resultDf = createDataFrame();
  resultDf.dictionary = dfs[0]!.dictionary; // Share dictionary

  // Copy all columns from all DataFrames
  for (const df of dfs) {
    const columnNames = getColumnNames(df);

    for (const colName of columnNames) {
      const sourceColResult = getColumn(df, colName);
      if (!sourceColResult.ok) {
        return err(new Error(sourceColResult.error));
      }

      const sourceCol = sourceColResult.data;
      const addResult = addColumn(resultDf, colName, sourceCol.dtype, firstRowCount);
      if (!addResult.ok) {
        return err(new Error(addResult.error));
      }

      const destColResult = getColumn(resultDf, colName);
      if (!destColResult.ok) {
        return err(new Error(destColResult.error));
      }

      const destCol = destColResult.data;

      if (sourceCol.nullBitmap) {
        enableNullTracking(destCol);
      }

      const bytesPerElement = getDTypeSize(sourceCol.dtype);
      const totalBytes = firstRowCount * bytesPerElement;

      // Copy all data
      for (let b = 0; b < totalBytes; b++) {
        destCol.data[b] = sourceCol.data[b]!;
      }

      // Copy null bitmap
      if (sourceCol.nullBitmap && destCol.nullBitmap) {
        const bitmapBytes = sourceCol.nullBitmap.data.byteLength;
        for (let b = 0; b < bitmapBytes; b++) {
          destCol.nullBitmap.data[b] = sourceCol.nullBitmap.data[b]!;
        }
      }
    }
  }

  return ok(resultDf);
}
