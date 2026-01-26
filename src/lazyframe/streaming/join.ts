import { enableNullTracking, setColumnValue } from '../../core/column';
import { type DataFrame, addColumn, createDataFrame, getColumn } from '../../dataframe/dataframe';
import type { JoinType } from '../../dataframe/joins/types';
import type {
  ColumnarBatch,
  ColumnarBatchColumn,
  ColumnarBatchIterator,
} from '../../io/columnar-batch';
import { internString } from '../../memory/dictionary';
import { DType } from '../../types/dtypes';
import type { Result } from '../../types/result';
import { err, ok } from '../../types/result';
import { setNull } from '../../utils/nulls';
import { getBatchValue, isBatchNull } from './batch-utils';

interface JoinKeySpec {
  leftKeys: string[];
  rightKeys: string[];
  suffixes: [string, string];
}

interface OutputColumnSpec {
  name: string;
  dtype: DType;
  source: 'left' | 'right';
  sourceName: string;
}

interface StoredColumn {
  dtype: DType;
  values: Array<number | bigint | string | boolean | null>;
  hasNulls: boolean;
}

function buildKeyString(
  row: number,
  keyColumns: ColumnarBatchColumn[],
  keyNames: string[],
): string | null {
  let key = '';
  for (let i = 0; i < keyColumns.length; i++) {
    const column = keyColumns[i];
    const keyName = keyNames[i];
    if (!column || !keyName) continue;
    if (column.hasNulls && column.nullBitmap && isBatchNull(column.nullBitmap, row)) {
      return null;
    }
    const value = getBatchValue(column, row);
    const part = typeof value === 'bigint' ? `bigint:${value.toString()}` : String(value);
    key = i === 0 ? part : `${key}|${part}`;
  }
  return key;
}

function resolveJoinKeys(options: {
  on?: string | string[];
  leftOn?: string | string[];
  rightOn?: string | string[];
}): Result<{ leftKeys: string[]; rightKeys: string[] }, Error> {
  if (options.on) {
    const keys = Array.isArray(options.on) ? options.on : [options.on];
    return ok({ leftKeys: keys, rightKeys: keys });
  }

  if (options.leftOn && options.rightOn) {
    const leftKeys = Array.isArray(options.leftOn) ? options.leftOn : [options.leftOn];
    const rightKeys = Array.isArray(options.rightOn) ? options.rightOn : [options.rightOn];
    if (leftKeys.length !== rightKeys.length) {
      return err(new Error('leftOn and rightOn must have same length'));
    }
    return ok({ leftKeys, rightKeys });
  }

  return err(new Error('Must specify either "on" or both "leftOn" and "rightOn"'));
}

function buildOutputColumns(
  leftSchema: Record<string, DType>,
  rightSchema: Record<string, DType>,
  leftOrder: string[],
  rightOrder: string[],
  join: JoinKeySpec,
): OutputColumnSpec[] {
  const rightKeySet = new Set(join.rightKeys);
  const leftNameSet = new Set(leftOrder);
  const overlapping = rightOrder.filter((name) => !rightKeySet.has(name) && leftNameSet.has(name));

  const output: OutputColumnSpec[] = [];

  for (const name of leftOrder) {
    const dtype = leftSchema[name];
    if (!dtype) continue;
    const finalName = overlapping.includes(name) ? `${name}${join.suffixes[0]}` : name;
    output.push({ name: finalName, dtype, source: 'left', sourceName: name });
  }

  for (const name of rightOrder) {
    if (rightKeySet.has(name)) continue;
    const dtype = rightSchema[name];
    if (!dtype) continue;
    const finalName = overlapping.includes(name) ? `${name}${join.suffixes[1]}` : name;
    output.push({ name: finalName, dtype, source: 'right', sourceName: name });
  }

  return output;
}

function readRowValue(
  column: ColumnarBatchColumn,
  row: number,
): number | bigint | string | boolean | null {
  if (column.hasNulls && column.nullBitmap && isBatchNull(column.nullBitmap, row)) {
    return null;
  }
  return getBatchValue(column, row);
}

function readStoredValue(
  column: StoredColumn,
  row: number,
): number | bigint | string | boolean | null {
  return column.values[row] ?? null;
}

function appendValue(
  values: Array<number | bigint | string | boolean | null>,
  hasNulls: { value: boolean },
  value: number | bigint | string | boolean | null,
): void {
  if (value === null) {
    hasNulls.value = true;
  }
  values.push(value);
}

export async function innerJoinBatchIterators(
  left: ColumnarBatchIterator,
  right: ColumnarBatchIterator,
  options: {
    on?: string | string[];
    leftOn?: string | string[];
    rightOn?: string | string[];
    suffixes?: [string, string];
    how?: JoinType;
  },
): Promise<Result<DataFrame, Error>> {
  try {
    const joinKeysResult = resolveJoinKeys(options);
    if (!joinKeysResult.ok) return err(joinKeysResult.error);

    const joinKeys = joinKeysResult.data;
    const suffixes = options.suffixes ?? ['_x', '_y'];
    const how = options.how ?? 'inner';

    let leftOrder = left.columnOrder;
    let rightOrder = right.columnOrder;
    let rightValueColumns: string[] = [];
    const rightStore: Record<string, StoredColumn> = {};

    const rightHash = new Map<string, number[]>();
    const matchedRight: boolean[] = [];
    let rightStoredCount = 0;

    for await (const batch of right) {
      if (rightOrder.length === 0) {
        rightOrder = right.columnOrder.length > 0 ? right.columnOrder : Object.keys(batch.columns);
        rightValueColumns = rightOrder.filter((name) => !joinKeys.rightKeys.includes(name));
        for (const name of rightValueColumns) {
          const dtype = right.schema[name];
          if (!dtype) continue;
          rightStore[name] = { dtype, values: [], hasNulls: false };
        }
      }
      const keyColumns = joinKeys.rightKeys.map((key) => {
        const col = batch.columns[key];
        if (!col) throw new Error(`Right join key column '${key}' not found in batch`);
        return col;
      });

      const valueColumns = rightValueColumns.map((name) => {
        const col = batch.columns[name];
        if (!col) throw new Error(`Right column '${name}' not found in batch`);
        return col;
      });

      for (let row = 0; row < batch.rowCount; row++) {
        for (let i = 0; i < rightValueColumns.length; i++) {
          const columnName = rightValueColumns[i];
          if (!columnName) continue;
          const column = valueColumns[i];
          if (!column) continue;
          const stored = rightStore[columnName];
          if (!stored) continue;
          const value = readRowValue(column, row);
          if (value === null) stored.hasNulls = true;
          stored.values.push(value);
        }

        const key = buildKeyString(row, keyColumns, joinKeys.rightKeys);
        if (key !== null) {
          const matches = rightHash.get(key);
          if (matches) {
            matches.push(rightStoredCount);
          } else {
            rightHash.set(key, [rightStoredCount]);
          }
        }
        matchedRight.push(false);
        rightStoredCount += 1;
      }
    }

    if (rightOrder.length === 0) {
      rightOrder = right.columnOrder.length > 0 ? right.columnOrder : Object.keys(right.schema);
      rightValueColumns = rightOrder.filter((name) => !joinKeys.rightKeys.includes(name));
      for (const name of rightValueColumns) {
        if (rightStore[name]) continue;
        const dtype = right.schema[name];
        if (!dtype) continue;
        rightStore[name] = { dtype, values: [], hasNulls: false };
      }
    }

    let outputSpecs: OutputColumnSpec[] = [];
    let outputValues: Array<Array<number | bigint | string | boolean | null>> = [];
    let outputNulls: Array<{ value: boolean }> = [];

    for await (const batch of left) {
      if (leftOrder.length === 0) {
        leftOrder = left.columnOrder.length > 0 ? left.columnOrder : Object.keys(batch.columns);
      }
      if (outputSpecs.length === 0) {
        outputSpecs = buildOutputColumns(left.schema, right.schema, leftOrder, rightOrder, {
          leftKeys: joinKeys.leftKeys,
          rightKeys: joinKeys.rightKeys,
          suffixes,
        });
        outputValues = outputSpecs.map(
          () => [] as Array<number | bigint | string | boolean | null>,
        );
        outputNulls = outputSpecs.map(() => ({ value: false }));
      }
      const keyColumns = joinKeys.leftKeys.map((key) => {
        const col = batch.columns[key];
        if (!col) throw new Error(`Left join key column '${key}' not found in batch`);
        return col;
      });

      const leftColumns = leftOrder.map((name) => {
        const col = batch.columns[name];
        if (!col) throw new Error(`Left column '${name}' not found in batch`);
        return col;
      });

      for (let row = 0; row < batch.rowCount; row++) {
        const key = buildKeyString(row, keyColumns, joinKeys.leftKeys);
        const matches = key === null ? [] : (rightHash.get(key) ?? []);

        const leftRowValues = leftColumns.map((column) => readRowValue(column, row));

        if (matches.length === 0) {
          if (how === 'left' || how === 'outer') {
            let outputIndex = 0;
            for (let i = 0; i < leftRowValues.length; i++) {
              const value = leftRowValues[i] ?? null;
              appendValue(outputValues[outputIndex]!, outputNulls[outputIndex]!, value);
              outputIndex += 1;
            }
            for (const columnName of rightValueColumns) {
              const stored = rightStore[columnName];
              if (!stored) {
                outputIndex += 1;
                continue;
              }
              appendValue(outputValues[outputIndex]!, outputNulls[outputIndex]!, null);
              outputIndex += 1;
            }
          }
          continue;
        }

        for (const matchIndex of matches) {
          matchedRight[matchIndex] = true;
          let outputIndex = 0;

          for (let i = 0; i < leftRowValues.length; i++) {
            const value = leftRowValues[i] ?? null;
            appendValue(outputValues[outputIndex]!, outputNulls[outputIndex]!, value);
            outputIndex += 1;
          }

          for (const columnName of rightValueColumns) {
            const stored = rightStore[columnName];
            if (!stored) {
              outputIndex += 1;
              continue;
            }
            const value = readStoredValue(stored, matchIndex);
            appendValue(outputValues[outputIndex]!, outputNulls[outputIndex]!, value);
            outputIndex += 1;
          }
        }
      }
    }

    if (outputSpecs.length === 0) {
      leftOrder = left.columnOrder.length > 0 ? left.columnOrder : Object.keys(left.schema);
      outputSpecs = buildOutputColumns(left.schema, right.schema, leftOrder, rightOrder, {
        leftKeys: joinKeys.leftKeys,
        rightKeys: joinKeys.rightKeys,
        suffixes,
      });
      outputValues = outputSpecs.map(() => [] as Array<number | bigint | string | boolean | null>);
      outputNulls = outputSpecs.map(() => ({ value: false }));
    }

    if (how === 'right' || how === 'outer') {
      const leftNullRow = new Array<number | bigint | string | boolean | null>(
        leftOrder.length,
      ).fill(null);
      for (let row = 0; row < rightStoredCount; row++) {
        if (matchedRight[row]) continue;
        let outputIndex = 0;
        for (let i = 0; i < leftNullRow.length; i++) {
          appendValue(outputValues[outputIndex]!, outputNulls[outputIndex]!, null);
          outputIndex += 1;
        }
        for (const columnName of rightValueColumns) {
          const stored = rightStore[columnName];
          if (!stored) {
            outputIndex += 1;
            continue;
          }
          const value = readStoredValue(stored, row);
          appendValue(outputValues[outputIndex]!, outputNulls[outputIndex]!, value);
          outputIndex += 1;
        }
      }
    }

    const resultDf = createDataFrame();
    const rowCount = outputValues[0]?.length ?? 0;

    for (let i = 0; i < outputSpecs.length; i++) {
      const spec = outputSpecs[i];
      if (!spec) continue;
      const addResult = addColumn(resultDf, spec.name, spec.dtype, rowCount);
      if (!addResult.ok) return err(new Error(addResult.error));

      const colResult = getColumn(resultDf, spec.name);
      if (!colResult.ok) return err(new Error(colResult.error));

      const column = colResult.data;
      const values = outputValues[i] ?? [];
      const hasNulls = outputNulls[i]?.value ?? false;

      if (hasNulls) {
        enableNullTracking(column);
      }

      for (let row = 0; row < rowCount; row++) {
        const value = values[row] ?? null;
        if (value === null) {
          if (column.nullBitmap) {
            setNull(column.nullBitmap, row);
          }
          continue;
        }

        if (spec.dtype === DType.String) {
          const dictId = resultDf.dictionary ? internString(resultDf.dictionary, String(value)) : 0;
          setColumnValue(column, row, dictId);
        } else if (spec.dtype === DType.Bool) {
          setColumnValue(column, row, value ? 1 : 0);
        } else if (spec.dtype === DType.Date || spec.dtype === DType.DateTime) {
          setColumnValue(column, row, typeof value === 'bigint' ? value : BigInt(value));
        } else {
          setColumnValue(column, row, Number(value));
        }
      }
    }

    return ok(resultDf);
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}
