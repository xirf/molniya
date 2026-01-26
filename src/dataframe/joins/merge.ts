import { enableNullTracking, getColumnValue } from '../../core/column';
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
import type { JoinType } from './types';

/**
 * Merge two DataFrames using SQL-style joins
 * Returns a new DataFrame with rows from both DataFrames matched on key columns
 *
 * @param left - Left DataFrame
 * @param right - Right DataFrame
 * @param options - Merge options
 * @returns Result with merged DataFrame or error
 */
export function merge(
  left: DataFrame,
  right: DataFrame,
  options: {
    /** Column name(s) to join on */
    on?: string | string[];
    /** Left DataFrame column(s) to join on */
    leftOn?: string | string[];
    /** Right DataFrame column(s) to join on */
    rightOn?: string | string[];
    /** Join type */
    how?: JoinType;
    /** Suffixes for overlapping column names [left_suffix, right_suffix] */
    suffixes?: [string, string];
  },
): Result<DataFrame, Error> {
  const how = options.how ?? 'inner';
  const suffixes = options.suffixes ?? ['_x', '_y'];

  // Determine join keys
  let leftKeys: string[];
  let rightKeys: string[];

  if (options.on) {
    const onKeys = Array.isArray(options.on) ? options.on : [options.on];
    leftKeys = onKeys;
    rightKeys = onKeys;
  } else if (options.leftOn && options.rightOn) {
    leftKeys = Array.isArray(options.leftOn) ? options.leftOn : [options.leftOn];
    rightKeys = Array.isArray(options.rightOn) ? options.rightOn : [options.rightOn];
  } else {
    return err(new Error('Must specify either "on" or both "leftOn" and "rightOn"'));
  }

  if (leftKeys.length !== rightKeys.length) {
    return err(new Error('leftOn and rightOn must have same length'));
  }

  // Validate join keys exist
  for (const key of leftKeys) {
    const col = getColumn(left, key);
    if (!col.ok) {
      return err(new Error(`Left DataFrame missing join key: '${key}'`));
    }
  }

  for (const key of rightKeys) {
    const col = getColumn(right, key);
    if (!col.ok) {
      return err(new Error(`Right DataFrame missing join key: '${key}'`));
    }
  }

  // Build hash map for right DataFrame (for efficient lookup)
  const rightRowCount = getRowCount(right);
  const rightHashMap = new Map<string, number[]>();

  for (let rightRow = 0; rightRow < rightRowCount; rightRow++) {
    const keyValues: (number | bigint)[] = [];
    let hasNull = false;

    for (const rightKey of rightKeys) {
      const colResult = getColumn(right, rightKey);
      if (!colResult.ok) continue;

      const col = colResult.data;

      // Check for null
      if (col.nullBitmap && isNull(col.nullBitmap, rightRow)) {
        hasNull = true;
        break;
      }

      const value = getColumnValue(col, rightRow);
      if (value === undefined) {
        hasNull = true;
        break;
      }
      keyValues.push(value);
    }

    // Skip rows with null keys
    if (hasNull) continue;

    const hashKey = keyValues.join('|');
    if (!rightHashMap.has(hashKey)) {
      rightHashMap.set(hashKey, []);
    }
    rightHashMap.get(hashKey)!.push(rightRow);
  }

  // Find matching rows
  const leftRowCount = getRowCount(left);
  const matches: Array<{ leftRow: number; rightRow: number | null }> = [];

  for (let leftRow = 0; leftRow < leftRowCount; leftRow++) {
    const keyValues: (number | bigint)[] = [];
    let hasNull = false;

    for (const leftKey of leftKeys) {
      const colResult = getColumn(left, leftKey);
      if (!colResult.ok) continue;

      const col = colResult.data;

      // Check for null
      if (col.nullBitmap && isNull(col.nullBitmap, leftRow)) {
        hasNull = true;
        break;
      }

      const value = getColumnValue(col, leftRow);
      if (value === undefined) {
        hasNull = true;
        break;
      }
      keyValues.push(value);
    }

    const hashKey = keyValues.join('|');
    const rightMatches = hasNull ? [] : (rightHashMap.get(hashKey) ?? []);

    if (rightMatches.length > 0) {
      // Inner/Left/Outer: Add all matches
      for (const rightRow of rightMatches) {
        matches.push({ leftRow, rightRow });
      }
    } else if (how === 'left' || how === 'outer') {
      // Left/Outer join: Keep left row with null right side
      matches.push({ leftRow, rightRow: null });
    }
  }

  // Right/Outer join: Add unmatched right rows
  if (how === 'right' || how === 'outer') {
    const matchedRightRows = new Set(
      matches.map((m) => m.rightRow).filter((r) => r !== null) as number[],
    );

    for (let rightRow = 0; rightRow < rightRowCount; rightRow++) {
      if (!matchedRightRows.has(rightRow)) {
        matches.push({ leftRow: -1, rightRow });
      }
    }
  }

  // Build result DataFrame
  const resultDf = createDataFrame();
  resultDf.dictionary = left.dictionary; // Share dictionary (will merge if needed)

  const resultRowCount = matches.length;

  // Get all column names
  const leftColumns = getColumnNames(left);
  const rightColumns = getColumnNames(right);

  // Determine which columns overlap (excluding join keys)
  const rightKeySet = new Set(rightKeys);
  const leftColumnSet = new Set(leftColumns);
  const overlappingCols = rightColumns.filter(
    (col) => !rightKeySet.has(col) && leftColumnSet.has(col),
  );

  // Add left columns
  for (const colName of leftColumns) {
    const sourceColResult = getColumn(left, colName);
    if (!sourceColResult.ok) {
      return err(new Error(sourceColResult.error));
    }

    const sourceCol = sourceColResult.data;
    const addResult = addColumn(resultDf, colName, sourceCol.dtype, resultRowCount);
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

    // Copy data from left DataFrame
    for (let i = 0; i < resultRowCount; i++) {
      const { leftRow } = matches[i]!;

      if (leftRow === -1) {
        // No left match (right join only)
        if (destCol.nullBitmap) {
          setNull(destCol.nullBitmap, i);
        }
      } else {
        // Copy value
        const sourceOffset = leftRow * bytesPerElement;
        const destOffset = i * bytesPerElement;

        for (let b = 0; b < bytesPerElement; b++) {
          destCol.data[destOffset + b] = sourceCol.data[sourceOffset + b]!;
        }

        // Copy null status
        if (sourceCol.nullBitmap && destCol.nullBitmap) {
          if (isNull(sourceCol.nullBitmap, leftRow)) {
            setNull(destCol.nullBitmap, i);
          }
        }
      }
    }
  }

  // Add right columns (excluding join keys and with suffix handling)
  for (const colName of rightColumns) {
    // Skip join keys (already in result from left)
    if (rightKeySet.has(colName)) {
      continue;
    }

    // Determine final column name (add suffix if overlapping)
    const finalName = overlappingCols.includes(colName) ? `${colName}${suffixes[1]}` : colName;

    // If overlapping, also rename the left column
    if (overlappingCols.includes(colName)) {
      // Rename left column by recreating it
      const leftColResult = getColumn(resultDf, colName);
      if (leftColResult.ok) {
        const leftCol = leftColResult.data;
        const newLeftName = `${colName}${suffixes[0]}`;

        // Remove old column and add with new name
        const leftColIndex = resultDf.columnOrder.indexOf(colName);
        resultDf.columns.delete(colName);
        resultDf.columnOrder[leftColIndex] = newLeftName;
        resultDf.columns.set(newLeftName, leftCol);
        leftCol.name = newLeftName;
      }
    }

    const sourceColResult = getColumn(right, colName);
    if (!sourceColResult.ok) {
      return err(new Error(sourceColResult.error));
    }

    const sourceCol = sourceColResult.data;
    const addResult = addColumn(resultDf, finalName, sourceCol.dtype, resultRowCount);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }

    const destColResult = getColumn(resultDf, finalName);
    if (!destColResult.ok) {
      return err(new Error(destColResult.error));
    }

    const destCol = destColResult.data;

    // Enable null tracking if source has it, OR if doing outer join (will have nulls)
    if (sourceCol.nullBitmap || how === 'left' || how === 'outer') {
      enableNullTracking(destCol);
    }

    const bytesPerElement = getDTypeSize(sourceCol.dtype);

    // Copy data from right DataFrame
    for (let i = 0; i < resultRowCount; i++) {
      const { rightRow } = matches[i]!;

      if (rightRow === null) {
        // No right match (left join only)
        if (destCol.nullBitmap) {
          setNull(destCol.nullBitmap, i);
        }
      } else {
        // Copy value
        const sourceOffset = rightRow * bytesPerElement;
        const destOffset = i * bytesPerElement;

        for (let b = 0; b < bytesPerElement; b++) {
          destCol.data[destOffset + b] = sourceCol.data[sourceOffset + b]!;
        }

        // Copy null status
        if (sourceCol.nullBitmap && destCol.nullBitmap) {
          if (isNull(sourceCol.nullBitmap, rightRow)) {
            setNull(destCol.nullBitmap, i);
          }
        }
      }
    }
  }

  return ok(resultDf);
}
