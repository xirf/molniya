import { setColumnValue } from '../../core/column';
import { type DataFrame, addColumn, createDataFrame, getColumn } from '../../dataframe/dataframe';
import type { AggFunc, AggSpec } from '../../dataframe/groupby';
import type { ColumnarBatch, ColumnarBatchIterator } from '../../io/columnar-batch';
import { internString } from '../../memory/dictionary';
import { DType } from '../../types/dtypes';
import type { Result } from '../../types/result';
import { err, ok } from '../../types/result';
import { getBatchValue } from './batch-utils';

interface GroupAggState {
  keyValues: Array<number | bigint | string | boolean | null>;
  aggs: Array<{
    spec: AggSpec;
    dtype: DType;
    count: number;
    sum: number;
    min?: number | bigint | string | boolean | null;
    max?: number | bigint | string | boolean | null;
    first?: number | bigint | string | boolean | null;
    last?: number | bigint | string | boolean | null;
  }>;
}

function getAggOutputDType(srcDtype: DType, func: AggFunc): DType {
  if (func === 'count') return DType.Int32;
  if (func === 'mean') return DType.Float64;
  return func === 'sum' ? (srcDtype === DType.Float64 ? DType.Float64 : DType.Int32) : srcDtype;
}

function buildKeyString(
  row: number,
  keyColumns: ColumnarBatch['columns'][string][],
  keyValues: Array<number | bigint | string | boolean | null>,
): string {
  let key = '';
  for (let i = 0; i < keyColumns.length; i++) {
    const column = keyColumns[i];
    if (!column) continue;
    const value = getBatchValue(column, row);
    keyValues[i] = value;
    const part = typeof value === 'bigint' ? `bigint:${value.toString()}` : String(value);
    key = i === 0 ? part : `${key}|${part}`;
  }
  return key;
}

function sumFloat64SIMD(data: Float64Array): number {
  let sum = 0;
  let i = 0;
  const len = data.length;
  for (; i + 3 < len; i += 4) {
    const v0 = data[i] ?? 0;
    const v1 = data[i + 1] ?? 0;
    const v2 = data[i + 2] ?? 0;
    const v3 = data[i + 3] ?? 0;
    sum += v0 + v1 + v2 + v3;
  }
  for (; i < len; i++) {
    sum += data[i] ?? 0;
  }
  return sum;
}

function sumInt32SIMD(data: Int32Array): number {
  let sum = 0;
  let i = 0;
  const len = data.length;
  for (; i + 3 < len; i += 4) {
    const v0 = data[i] ?? 0;
    const v1 = data[i + 1] ?? 0;
    const v2 = data[i + 2] ?? 0;
    const v3 = data[i + 3] ?? 0;
    sum += v0 + v1 + v2 + v3;
  }
  for (; i < len; i++) {
    sum += data[i] ?? 0;
  }
  return sum;
}

function minFloat64(data: Float64Array): number {
  let min = data.length > 0 ? (data[0] ?? 0) : 0;
  for (let i = 1; i < data.length; i++) {
    const value = data[i];
    if (value !== undefined && value < min) min = value;
  }
  return min;
}

function maxFloat64(data: Float64Array): number {
  let max = data.length > 0 ? (data[0] ?? 0) : 0;
  for (let i = 1; i < data.length; i++) {
    const value = data[i];
    if (value !== undefined && value > max) max = value;
  }
  return max;
}

function minInt32(data: Int32Array): number {
  let min = data.length > 0 ? (data[0] ?? 0) : 0;
  for (let i = 1; i < data.length; i++) {
    const value = data[i];
    if (value !== undefined && value < min) min = value;
  }
  return min;
}

function maxInt32(data: Int32Array): number {
  let max = data.length > 0 ? (data[0] ?? 0) : 0;
  for (let i = 1; i < data.length; i++) {
    const value = data[i];
    if (value !== undefined && value > max) max = value;
  }
  return max;
}

function createAggState(
  aggregations: AggSpec[],
  aggDtypes: DType[],
  keyValues: Array<number | bigint | string | boolean | null>,
): GroupAggState {
  return {
    keyValues,
    aggs: aggregations.map((spec, index) => ({
      spec,
      dtype: aggDtypes[index] ?? DType.Float64,
      count: 0,
      sum: 0,
    })),
  };
}

function updateGlobalAggregates(
  state: GroupAggState,
  batch: ColumnarBatch,
  aggColumns: ColumnarBatch['columns'][string][],
): void {
  for (let i = 0; i < state.aggs.length; i++) {
    const agg = state.aggs[i];
    if (!agg) continue;
    const column = aggColumns[i];
    if (!column) {
      throw new Error(`Aggregation column '${agg.spec.col}' not found in batch`);
    }

    switch (agg.spec.func) {
      case 'count':
        agg.count += batch.rowCount;
        break;
      case 'sum': {
        if (column.dtype === DType.Float64) {
          agg.sum += sumFloat64SIMD(column.data as Float64Array);
        } else if (column.dtype === DType.Int32) {
          agg.sum += sumInt32SIMD(column.data as Int32Array);
        }
        break;
      }
      case 'mean': {
        if (column.dtype === DType.Float64) {
          agg.sum += sumFloat64SIMD(column.data as Float64Array);
          agg.count += batch.rowCount;
        } else if (column.dtype === DType.Int32) {
          agg.sum += sumInt32SIMD(column.data as Int32Array);
          agg.count += batch.rowCount;
        }
        break;
      }
      case 'min': {
        const value =
          column.dtype === DType.Float64
            ? minFloat64(column.data as Float64Array)
            : column.dtype === DType.Int32
              ? minInt32(column.data as Int32Array)
              : getBatchValue(column, 0);
        if (agg.min === undefined || agg.min === null) {
          agg.min = value;
        } else if (typeof agg.min === 'number' && typeof value === 'number') {
          if (value < agg.min) agg.min = value;
        }
        break;
      }
      case 'max': {
        const value =
          column.dtype === DType.Float64
            ? maxFloat64(column.data as Float64Array)
            : column.dtype === DType.Int32
              ? maxInt32(column.data as Int32Array)
              : getBatchValue(column, 0);
        if (agg.max === undefined || agg.max === null) {
          agg.max = value;
        } else if (typeof agg.max === 'number' && typeof value === 'number') {
          if (value > agg.max) agg.max = value;
        }
        break;
      }
      case 'first':
        if (agg.first === undefined) {
          agg.first = getBatchValue(column, 0);
        }
        break;
      case 'last':
        if (batch.rowCount > 0) {
          agg.last = getBatchValue(column, batch.rowCount - 1);
        }
        break;
    }
  }
}

export async function aggregateBatchesToDataFrame(
  iterator: ColumnarBatchIterator,
  groupKeys: string[],
  aggregations: AggSpec[],
): Promise<Result<DataFrame, Error>> {
  try {
    const groups = new Map<string, GroupAggState>();
    let globalState: GroupAggState | null = null;
    let schemaInitialized = false;
    let aggDtypes: DType[] = [];
    let keyDtypes: DType[] = groupKeys.map((key) => iterator.schema[key] ?? DType.String);

    for await (const batch of iterator) {
      const keyColumns = groupKeys.map((key) => {
        const col = batch.columns[key];
        if (!col) {
          throw new Error(`Group key column '${key}' not found in batch`);
        }
        return col;
      });
      const aggColumns = aggregations.map((agg) => {
        const col = batch.columns[agg.col];
        if (!col) {
          throw new Error(`Aggregation column '${agg.col}' not found in batch`);
        }
        return col;
      });

      if (!schemaInitialized) {
        keyDtypes = groupKeys.map((key) => {
          const col = batch.columns[key];
          if (!col) {
            throw new Error(`Group key column '${key}' not found in batch`);
          }
          return col.dtype;
        });
        aggDtypes = aggregations.map((agg) => {
          const col = batch.columns[agg.col];
          if (!col) {
            throw new Error(`Aggregation column '${agg.col}' not found in batch`);
          }
          return getAggOutputDType(col.dtype, agg.func);
        });
        schemaInitialized = true;
      }

      if (groupKeys.length === 0) {
        if (!globalState) {
          globalState = createAggState(aggregations, aggDtypes, []);
        }
        updateGlobalAggregates(globalState, batch, aggColumns);
        continue;
      }

      const keyValues = new Array<number | bigint | string | boolean | null>(groupKeys.length);

      for (let row = 0; row < batch.rowCount; row++) {
        const key = buildKeyString(row, keyColumns, keyValues);
        let state = groups.get(key);
        if (!state) {
          state = createAggState(aggregations, aggDtypes, keyValues.slice());
          groups.set(key, state);
        }

        for (let i = 0; i < state.aggs.length; i++) {
          const agg = state.aggs[i];
          if (!agg) continue;
          const aggColumn = aggColumns[i];
          if (!aggColumn) {
            throw new Error(`Aggregation column '${agg.spec.col}' not found in batch`);
          }
          const value = getBatchValue(aggColumn, row);
          switch (agg.spec.func) {
            case 'count':
              agg.count += 1;
              break;
            case 'sum':
              if (typeof value === 'number') agg.sum += value;
              break;
            case 'mean':
              if (typeof value === 'number') {
                agg.sum += value;
                agg.count += 1;
              }
              break;
            case 'min':
              if (agg.min === undefined || agg.min === null) {
                agg.min = value;
              } else if (typeof agg.min === 'number' && typeof value === 'number') {
                if (value < agg.min) agg.min = value;
              } else if (typeof agg.min === 'bigint' && typeof value === 'bigint') {
                if (value < agg.min) agg.min = value;
              }
              break;
            case 'max':
              if (agg.max === undefined || agg.max === null) {
                agg.max = value;
              } else if (typeof agg.max === 'number' && typeof value === 'number') {
                if (value > agg.max) agg.max = value;
              } else if (typeof agg.max === 'bigint' && typeof value === 'bigint') {
                if (value > agg.max) agg.max = value;
              }
              break;
            case 'first':
              if (agg.first === undefined) agg.first = value;
              break;
            case 'last':
              agg.last = value;
              break;
          }
        }
      }
    }

    const groupedStates =
      groupKeys.length === 0 ? (globalState ? [globalState] : []) : [...groups.values()];
    const resultDf = createDataFrame();
    const groupCount = groupedStates.length;

    for (let i = 0; i < groupKeys.length; i++) {
      const key = groupKeys[i];
      if (!key) continue;
      const dtype = keyDtypes[i] ?? DType.String;
      const addResult = addColumn(resultDf, key, dtype, groupCount);
      if (!addResult.ok) return err(new Error(addResult.error));
    }

    for (let i = 0; i < aggregations.length; i++) {
      const agg = aggregations[i];
      if (!agg) continue;
      const dtype = aggDtypes[i] ?? DType.Float64;
      const addResult = addColumn(resultDf, agg.outName, dtype, groupCount);
      if (!addResult.ok) return err(new Error(addResult.error));
    }

    let rowIndex = 0;
    for (const group of groupedStates) {
      for (let i = 0; i < groupKeys.length; i++) {
        const keyName = groupKeys[i];
        if (!keyName) continue;
        const value = group.keyValues[i];
        const colResult = getColumn(resultDf, keyName);
        if (!colResult.ok) return err(new Error(colResult.error));
        if (colResult.data.dtype === DType.String) {
          const dictId = resultDf.dictionary ? internString(resultDf.dictionary, String(value)) : 0;
          setColumnValue(colResult.data, rowIndex, dictId);
        } else if (colResult.data.dtype === DType.Bool) {
          setColumnValue(colResult.data, rowIndex, value ? 1 : 0);
        } else if (colResult.data.dtype === DType.Date || colResult.data.dtype === DType.DateTime) {
          setColumnValue(colResult.data, rowIndex, (value as bigint) ?? 0n);
        } else {
          setColumnValue(colResult.data, rowIndex, (value as number) ?? 0);
        }
      }

      for (let i = 0; i < group.aggs.length; i++) {
        const aggState = group.aggs[i];
        if (!aggState) continue;
        const outName = aggState.spec.outName;
        const colResult = getColumn(resultDf, outName);
        if (!colResult.ok) return err(new Error(colResult.error));

        let value: number | bigint | string | boolean | null = null;
        switch (aggState.spec.func) {
          case 'count':
            value = aggState.count;
            break;
          case 'sum':
            value = aggState.sum;
            break;
          case 'mean':
            value = aggState.count === 0 ? 0 : aggState.sum / aggState.count;
            break;
          case 'min':
            value = aggState.min ?? 0;
            break;
          case 'max':
            value = aggState.max ?? 0;
            break;
          case 'first':
            value = aggState.first ?? 0;
            break;
          case 'last':
            value = aggState.last ?? 0;
            break;
        }

        if (colResult.data.dtype === DType.String) {
          const dictId = resultDf.dictionary ? internString(resultDf.dictionary, String(value)) : 0;
          setColumnValue(colResult.data, rowIndex, dictId);
        } else if (colResult.data.dtype === DType.Bool) {
          setColumnValue(colResult.data, rowIndex, value ? 1 : 0);
        } else if (colResult.data.dtype === DType.Date || colResult.data.dtype === DType.DateTime) {
          setColumnValue(colResult.data, rowIndex, (value as bigint) ?? 0n);
        } else {
          setColumnValue(colResult.data, rowIndex, (value as number) ?? 0);
        }
      }

      rowIndex += 1;
    }

    return ok(resultDf);
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}
