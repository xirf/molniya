import { enableNullTracking, setColumnValue } from '../../core/column';
import { type DataFrame, addColumn, createDataFrame, getColumn } from '../../dataframe/dataframe';
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

function buildRowKey(row: number, columns: ColumnarBatchColumn[], columnNames: string[]): string {
  let key = '';
  for (let i = 0; i < columns.length; i++) {
    const column = columns[i];
    const name = columnNames[i];
    if (!column || !name) continue;
    if (column.hasNulls && column.nullBitmap && isBatchNull(column.nullBitmap, row)) {
      key = i === 0 ? 'NULL' : `${key}|NULL`;
      continue;
    }
    const value = getBatchValue(column, row);
    const part = typeof value === 'bigint' ? `bigint:${value.toString()}` : String(value);
    key = i === 0 ? part : `${key}|${part}`;
  }
  return key;
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

export async function distinctBatchesToDataFrame(
  iterator: ColumnarBatchIterator,
  subset?: string[],
): Promise<Result<DataFrame, Error>> {
  try {
    const df = createDataFrame();
    const seen = new Set<string>();
    let outputOrder = iterator.columnOrder;
    let outputDtypes: DType[] = [];
    let outputValues: Array<Array<number | bigint | string | boolean | null>> = [];
    let outputNulls: Array<{ value: boolean }> = [];
    let keyColumnNames: string[] = subset ?? [];
    let keyColumns: ColumnarBatchColumn[] = [];

    for await (const batch of iterator) {
      if (outputOrder.length === 0) {
        outputOrder =
          iterator.columnOrder.length > 0 ? iterator.columnOrder : Object.keys(batch.columns);
      }
      if (outputValues.length === 0) {
        outputDtypes = outputOrder.map((name) => batch.columns[name]?.dtype ?? DType.String);
        outputValues = outputOrder.map(
          () => [] as Array<number | bigint | string | boolean | null>,
        );
        outputNulls = outputOrder.map(() => ({ value: false }));
      }

      if (keyColumnNames.length === 0) {
        keyColumnNames = subset && subset.length > 0 ? subset : outputOrder;
      }

      if (keyColumns.length === 0 || keyColumns.length !== keyColumnNames.length) {
        keyColumns = keyColumnNames.map((name) => {
          const col = batch.columns[name];
          if (!col) {
            throw new Error(`Distinct column '${name}' not found in batch`);
          }
          return col;
        });
      }

      const outputColumns = outputOrder.map((name) => {
        const col = batch.columns[name];
        if (!col) throw new Error(`Distinct output column '${name}' not found in batch`);
        return col;
      });

      for (let row = 0; row < batch.rowCount; row++) {
        const key = buildRowKey(row, keyColumns, keyColumnNames);
        if (seen.has(key)) continue;
        seen.add(key);

        for (let colIndex = 0; colIndex < outputColumns.length; colIndex++) {
          const col = outputColumns[colIndex];
          if (!col) continue;
          const value = readRowValue(col, row);
          if (value === null) outputNulls[colIndex]!.value = true;
          outputValues[colIndex]!.push(value);
        }
      }
    }

    const rowCount = outputValues[0]?.length ?? 0;
    for (let i = 0; i < outputOrder.length; i++) {
      const name = outputOrder[i];
      if (!name) continue;
      const dtype = outputDtypes[i] ?? DType.String;
      const addResult = addColumn(df, name, dtype, rowCount);
      if (!addResult.ok) return err(new Error(addResult.error));

      const colResult = getColumn(df, name);
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
          if (column.nullBitmap) setNull(column.nullBitmap, row);
          continue;
        }

        if (dtype === DType.String) {
          const dictId = df.dictionary ? internString(df.dictionary, String(value)) : 0;
          setColumnValue(column, row, dictId);
        } else if (dtype === DType.Bool) {
          setColumnValue(column, row, value ? 1 : 0);
        } else if (dtype === DType.Date || dtype === DType.DateTime) {
          setColumnValue(column, row, typeof value === 'bigint' ? value : BigInt(value));
        } else {
          setColumnValue(column, row, Number(value));
        }
      }
    }

    return ok(df);
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}
