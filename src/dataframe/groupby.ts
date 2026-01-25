import type { Column } from '../core/column';
import { getColumnValue, setColumnValue } from '../core/column';
import { DType } from '../types/dtypes';
import { type Result, err, ok, unwrapErr } from '../types/result';
import { createRowIndices, findGroupBoundaries, sortByColumns } from '../utils/sort';
import { type DataFrame, addColumn, createDataFrame, getColumn } from './dataframe';

/**
 * Aggregation functions supported by GroupBy
 */
export type AggFunc = 'count' | 'sum' | 'mean' | 'min' | 'max' | 'first' | 'last';

/**
 * Aggregation specification for GroupBy
 */
export interface AggSpec {
  /** Column to aggregate */
  col: string;
  /** Aggregation function */
  func: AggFunc;
  /** Output column name */
  outName: string;
}

/**
 * GroupBy operation using pre-sort strategy
 *
 * Algorithm:
 * 1. Sort row indices by group key columns
 * 2. Find group boundaries in sorted indices
 * 3. For each group, compute aggregations
 * 4. Return new DataFrame with one row per group
 *
 * @param df - Source DataFrame
 * @param groupKeys - Column names to group by
 * @param aggregations - Aggregation specifications
 * @returns Result with aggregated DataFrame or error
 */
export function groupby<T>(
  df: DataFrame<T>,
  groupKeys: string[],
  aggregations: AggSpec[],
): Result<DataFrame<T>, Error> {
  // Validate group key columns exist
  const groupKeyCols = [];
  for (const keyName of groupKeys) {
    const colResult = getColumn(df as DataFrame<Record<string, unknown>>, keyName);
    if (!colResult.ok) {
      return err(new Error(`Group key column '${keyName}' not found`));
    }
    groupKeyCols.push(colResult.data);
  }

  // Validate aggregation columns exist
  for (const agg of aggregations) {
    const colResult = getColumn(df as DataFrame<Record<string, unknown>>, agg.col);
    if (!colResult.ok) {
      return err(new Error(`Aggregation column '${agg.col}' not found`));
    }
  }

  // Handle empty DataFrame
  const rowCount = groupKeyCols[0]?.length ?? 0;
  if (rowCount === 0) {
    // Create empty result with correct schema
    const emptyResult = createDataFrame();
    emptyResult.dictionary = df.dictionary;

    // Add group key columns
    for (let i = 0; i < groupKeys.length; i++) {
      const keyCol = groupKeyCols[i];
      const keyName = groupKeys[i];
      if (!keyCol || !keyName) continue;
      addColumn(emptyResult, keyName, keyCol.dtype, 0);
    }

    // Add aggregation columns
    for (const agg of aggregations) {
      const srcColResult = getColumn(df as DataFrame<Record<string, unknown>>, agg.col);
      if (!srcColResult.ok) continue; // Already validated above
      const srcCol = srcColResult.data;

      // Determine output dtype
      const outDtype =
        agg.func === 'count' ? DType.Int32 : agg.func === 'mean' ? DType.Float64 : srcCol.dtype;

      addColumn(emptyResult, agg.outName, outDtype, 0);
    }

    return ok(emptyResult);
  }

  // Step 1: Create and sort row indices by group keys
  const indices = createRowIndices(rowCount);

  if (groupKeyCols.length > 0) {
    const sortSpecs = groupKeyCols.map((col) => ({ column: col, direction: 'asc' as const }));
    sortByColumns(indices, sortSpecs);
  }

  // Step 2: Find group boundaries
  const boundaries = findGroupBoundaries(indices, groupKeyCols);
  const groupCount = boundaries.length;

  // Step 3: Create result DataFrame
  const resultDf = createDataFrame();
  resultDf.dictionary = df.dictionary; // Share dictionary for string columns

  // Add group key columns to result
  for (let i = 0; i < groupKeys.length; i++) {
    const keyName = groupKeys[i];
    const keyCol = groupKeyCols[i];
    if (!keyName || !keyCol) continue;
    const addResult = addColumn(resultDf, keyName, keyCol.dtype, groupCount);

    if (!addResult.ok) {
      return err(new Error(unwrapErr(addResult)));
    }

    const resultColResult = getColumn(resultDf as DataFrame<Record<string, unknown>>, keyName);
    if (!resultColResult.ok) {
      return err(new Error(unwrapErr(resultColResult)));
    }
    const resultCol = resultColResult.data;

    // Fill group key values (take first value from each group)
    for (let groupIdx = 0; groupIdx < groupCount; groupIdx++) {
      const boundary = boundaries[groupIdx];
      if (!boundary) continue;
      const [start] = boundary;
      const firstRowIdx = indices[start];
      if (firstRowIdx === undefined) continue;
      const keyValue = getColumnValue(keyCol, firstRowIdx);
      if (keyValue === undefined) continue;
      setColumnValue(resultCol, groupIdx, keyValue);
    }
  }

  // Step 4: Add aggregation columns to result
  for (const agg of aggregations) {
    const sourceColResult = getColumn(df as DataFrame<Record<string, unknown>>, agg.col);
    if (!sourceColResult.ok) {
      return err(new Error(unwrapErr(sourceColResult)));
    }
    const sourceCol = sourceColResult.data;

    // Determine output dtype based on aggregation function
    let outputDtype: DType;
    switch (agg.func) {
      case 'count':
        outputDtype = DType.Int32;
        break;
      case 'mean':
        outputDtype = DType.Float64;
        break;
      case 'sum':
        // Sum preserves dtype (except DateTime/Date which don't make sense to sum)
        outputDtype = sourceCol.dtype === DType.Float64 ? DType.Float64 : DType.Int32;
        break;
      case 'min':
      case 'max':
      case 'first':
      case 'last':
        // These preserve the source dtype
        outputDtype = sourceCol.dtype;
        break;
    }

    const addResult = addColumn(resultDf, agg.outName, outputDtype, groupCount);
    if (!addResult.ok) {
      return err(new Error(unwrapErr(addResult)));
    }

    const resultColResult = getColumn(resultDf as DataFrame<Record<string, unknown>>, agg.outName);
    if (!resultColResult.ok) {
      return err(new Error(unwrapErr(resultColResult)));
    }
    const resultCol = resultColResult.data;

    // Step 5: Compute aggregation for each group
    for (let groupIdx = 0; groupIdx < groupCount; groupIdx++) {
      const boundary = boundaries[groupIdx];
      if (!boundary) continue;
      const [start, end] = boundary;

      const aggResult = computeAggregation(sourceCol, indices, start, end, agg.func);

      setColumnValue(resultCol, groupIdx, aggResult);
    }
  }

  return ok(resultDf);
}

/**
 * Compute aggregation for a single group
 * HOT FUNCTION: Zero object creation
 *
 * @param column - Source column to aggregate
 * @param indices - Sorted row indices
 * @param start - Start index in indices array (inclusive)
 * @param end - End index in indices array (exclusive)
 * @param func - Aggregation function
 * @returns Aggregated value
 */
function computeAggregation(
  column: Column,
  indices: number[],
  start: number,
  end: number,
  func: AggFunc,
): number | bigint {
  const groupSize = end - start;

  switch (func) {
    case 'count':
      return groupSize;

    case 'sum': {
      if (column.dtype === DType.Float64) {
        let sum = 0;
        for (let i = start; i < end; i++) {
          const rowIdx = indices[i];
          if (rowIdx === undefined) continue;
          const value = getColumnValue(column, rowIdx);
          sum += typeof value === 'number' ? value : 0;
        }
        return sum;
      }
      if (column.dtype === DType.Int32) {
        let sum = 0;
        for (let i = start; i < end; i++) {
          const rowIdx = indices[i];
          if (rowIdx === undefined) continue;
          const value = getColumnValue(column, rowIdx);
          sum += typeof value === 'number' ? value : 0;
        }
        return sum;
      }
      return 0;
    }

    case 'mean': {
      if (column.dtype === DType.Float64 || column.dtype === DType.Int32) {
        let sum = 0;
        for (let i = start; i < end; i++) {
          const rowIdx = indices[i];
          if (rowIdx === undefined) continue;
          const value = getColumnValue(column, rowIdx);
          sum += typeof value === 'number' ? value : 0;
        }
        return sum / groupSize;
      }
      return 0;
    }

    case 'min': {
      const firstRowIdx = indices[start];
      if (firstRowIdx === undefined) return 0;
      let minVal = getColumnValue(column, firstRowIdx);
      if (minVal === undefined) return 0;

      for (let i = start + 1; i < end; i++) {
        const rowIdx = indices[i];
        if (rowIdx === undefined) continue;
        const val = getColumnValue(column, rowIdx);

        if (typeof minVal === 'number' && typeof val === 'number') {
          if (val < minVal) minVal = val;
        } else if (typeof minVal === 'bigint' && typeof val === 'bigint') {
          if (val < minVal) minVal = val;
        }
      }

      return minVal;
    }

    case 'max': {
      const firstRowIdx = indices[start];
      if (firstRowIdx === undefined) return 0;
      let maxVal = getColumnValue(column, firstRowIdx);
      if (maxVal === undefined) return 0;

      for (let i = start + 1; i < end; i++) {
        const rowIdx = indices[i];
        if (rowIdx === undefined) continue;
        const val = getColumnValue(column, rowIdx);

        if (typeof maxVal === 'number' && typeof val === 'number') {
          if (val > maxVal) maxVal = val;
        } else if (typeof maxVal === 'bigint' && typeof val === 'bigint') {
          if (val > maxVal) maxVal = val;
        }
      }

      return maxVal;
    }

    case 'first': {
      const firstRowIdx = indices[start];
      if (firstRowIdx === undefined) return 0;
      const value = getColumnValue(column, firstRowIdx);
      return value !== undefined ? value : 0;
    }

    case 'last': {
      const lastRowIdx = indices[end - 1];
      if (lastRowIdx === undefined) return 0;
      const value = getColumnValue(column, lastRowIdx);
      return value !== undefined ? value : 0;
    }
  }
}
