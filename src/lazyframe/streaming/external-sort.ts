import * as fs from 'node:fs';
import * as path from 'node:path';
import { enableNullTracking, setColumnValue } from '../../core/column';
import { type DataFrame, addColumn, createDataFrame, getColumn } from '../../dataframe/dataframe';
import { readBinaryBlocks, writeBinaryBlocks } from '../../io/binary-format';
import type { BinaryBlock, BlockColumn } from '../../io/binary-format/types';
import type { ColumnarBatchColumn, ColumnarBatchIterator } from '../../io/columnar-batch';
import { internString } from '../../memory/dictionary';
import { DType } from '../../types/dtypes';
import type { Result } from '../../types/result';
import { err, ok } from '../../types/result';
import { setNull } from '../../utils/nulls';
import type { SortDirection } from '../../utils/sort';
import { getBatchValue, isBatchNull } from './batch-utils';

export interface ExternalSortOptions {
  runBytes?: number;
  tempDir?: string;
}

interface RunData {
  columns: Record<string, Array<number | bigint | string | boolean | null>>;
  rowCount: number;
}

function estimateValueBytes(
  value: number | bigint | string | boolean | null,
  dtype: DType,
): number {
  if (value === null) return 1;
  switch (dtype) {
    case DType.Int32:
      return 4;
    case DType.Float64:
    case DType.DateTime:
    case DType.Date:
      return 8;
    case DType.Bool:
      return 1;
    case DType.String:
      return String(value).length * 2 + 4;
    default:
      return 4;
  }
}

function compareValues(
  a: number | bigint | string | boolean | null,
  b: number | bigint | string | boolean | null,
): number {
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;

  if (typeof a === 'number' && typeof b === 'number') return a - b;
  if (typeof a === 'bigint' && typeof b === 'bigint') return a < b ? -1 : a > b ? 1 : 0;
  if (typeof a === 'boolean' && typeof b === 'boolean') return Number(a) - Number(b);
  if (typeof a === 'string' && typeof b === 'string') return a.localeCompare(b);

  return String(a).localeCompare(String(b));
}

function compareRows(
  runs: RunData[],
  runIndexA: number,
  rowA: number,
  runIndexB: number,
  rowB: number,
  sortColumns: string[],
  directions: SortDirection[],
): number {
  const runA = runs[runIndexA];
  const runB = runs[runIndexB];
  if (!runA || !runB) return 0;

  for (let i = 0; i < sortColumns.length; i++) {
    const col = sortColumns[i];
    if (!col) continue;
    const dir = directions[i] ?? 'asc';
    const aVal = runA.columns[col]?.[rowA] ?? null;
    const bVal = runB.columns[col]?.[rowB] ?? null;
    const cmp = compareValues(aVal, bVal);
    if (cmp !== 0) return dir === 'asc' ? cmp : -cmp;
  }

  return 0;
}

function buildBlockColumns(
  schema: Record<string, DType>,
  columnOrder: string[],
  rows: Record<string, Array<number | bigint | string | boolean | null>>,
  rowCount: number,
): Record<string, BlockColumn> {
  const columns: Record<string, BlockColumn> = {};

  for (const name of columnOrder) {
    const dtype = schema[name] ?? DType.String;
    const values = rows[name] ?? [];
    let hasNulls = false;
    let nullBitmap: Uint8Array | undefined;

    if (dtype === DType.String) {
      const data = new Array<string>(rowCount);
      for (let i = 0; i < rowCount; i++) {
        const value = values[i] ?? null;
        if (value === null) {
          hasNulls = true;
          if (!nullBitmap) nullBitmap = new Uint8Array(Math.ceil(rowCount / 8));
          const byteIndex = i >> 3;
          const bitIndex = i & 7;
          nullBitmap[byteIndex] = (nullBitmap[byteIndex] ?? 0) | (1 << bitIndex);
          data[i] = '';
        } else {
          data[i] = String(value);
        }
      }
      columns[name] = { dtype, data, hasNulls, nullBitmap };
      continue;
    }

    const typed =
      dtype === DType.Int32
        ? new Int32Array(rowCount)
        : dtype === DType.Float64
          ? new Float64Array(rowCount)
          : dtype === DType.Bool
            ? new Uint8Array(rowCount)
            : new BigInt64Array(rowCount);

    for (let i = 0; i < rowCount; i++) {
      const value = values[i] ?? null;
      if (value === null) {
        hasNulls = true;
        if (!nullBitmap) nullBitmap = new Uint8Array(Math.ceil(rowCount / 8));
        const byteIndex = i >> 3;
        const bitIndex = i & 7;
        nullBitmap[byteIndex] = (nullBitmap[byteIndex] ?? 0) | (1 << bitIndex);
        if (typed instanceof BigInt64Array) {
          typed[i] = 0n;
        } else {
          typed[i] = 0;
        }
      } else if (typed instanceof BigInt64Array) {
        typed[i] = typeof value === 'bigint' ? value : BigInt(value as number);
      } else {
        typed[i] = typeof value === 'boolean' ? (value ? 1 : 0) : Number(value);
      }
    }

    columns[name] = { dtype, data: typed, hasNulls, nullBitmap };
  }

  return columns;
}

function materializeRunFromBlocks(blocks: BinaryBlock[]): RunData {
  const columns: Record<string, Array<number | bigint | string | boolean | null>> = {};
  let rowCount = 0;

  for (const block of blocks) {
    for (const [name, col] of Object.entries(block.columns)) {
      if (!columns[name]) columns[name] = [];
      const target = columns[name]!;

      for (let i = 0; i < block.rowCount; i++) {
        if (col.hasNulls && col.nullBitmap) {
          const byteIndex = i >> 3;
          const bitIndex = i & 7;
          const isNull = ((col.nullBitmap[byteIndex] ?? 0) & (1 << bitIndex)) !== 0;
          if (isNull) {
            target.push(null);
            continue;
          }
        }

        if (Array.isArray(col.data)) {
          target.push(col.data[i] ?? '');
        } else if (col.data instanceof BigInt64Array) {
          target.push(col.data[i] ?? 0n);
        } else {
          target.push(col.data[i] ?? 0);
        }
      }
    }
    rowCount += block.rowCount;
  }

  return { columns, rowCount };
}

function buildDataFrameFromColumns(
  schema: Record<string, DType>,
  columnOrder: string[],
  columns: Record<string, Array<number | bigint | string | boolean | null>>,
  rowCount: number,
): Result<DataFrame, Error> {
  const df = createDataFrame();

  for (const name of columnOrder) {
    const dtype = schema[name] ?? DType.String;
    const addResult = addColumn(df, name, dtype, rowCount);
    if (!addResult.ok) return err(new Error(addResult.error));

    const colResult = getColumn(df, name);
    if (!colResult.ok) return err(new Error(colResult.error));

    const column = colResult.data;
    const values = columns[name] ?? [];
    let hasNulls = false;

    for (let i = 0; i < rowCount; i++) {
      if (values[i] === null) {
        hasNulls = true;
        break;
      }
    }

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
}

export async function externalMergeSortBatchesToDataFrame(
  iterator: ColumnarBatchIterator,
  sortColumns: string[],
  directions: SortDirection[] = [],
  options: ExternalSortOptions = {},
): Promise<Result<DataFrame, Error>> {
  try {
    const runBytes = options.runBytes ?? 4 * 1024 * 1024;
    const tempBase = options.tempDir ?? path.join(process.cwd(), '.molniya_tmp');
    fs.mkdirSync(tempBase, { recursive: true });

    let schema = iterator.schema;
    let columnOrder = iterator.columnOrder;
    let runRows: Record<string, Array<number | bigint | string | boolean | null>> = {};
    let runRowCount = 0;
    let estimatedBytes = 0;
    const runFiles: string[] = [];

    const flushRun = async (): Promise<void> => {
      if (runRowCount === 0) return;
      const indices = new Array(runRowCount).fill(0).map((_, i) => i);
      indices.sort((aIdx, bIdx) => {
        for (let i = 0; i < sortColumns.length; i++) {
          const col = sortColumns[i];
          if (!col) continue;
          const dir = directions[i] ?? 'asc';
          const aVal = runRows[col]?.[aIdx] ?? null;
          const bVal = runRows[col]?.[bIdx] ?? null;
          const cmp = compareValues(aVal, bVal);
          if (cmp !== 0) return dir === 'asc' ? cmp : -cmp;
        }
        return 0;
      });

      const sortedRows: Record<string, Array<number | bigint | string | boolean | null>> = {};
      for (const name of columnOrder) {
        sortedRows[name] = indices.map((idx) => runRows[name]?.[idx] ?? null);
      }

      const blockColumns = buildBlockColumns(schema, columnOrder, sortedRows, runRowCount);
      const block: BinaryBlock = {
        blockId: 0,
        rowCount: runRowCount,
        columns: blockColumns,
      };

      const runPath = path.join(tempBase, `molniya-sort-${Date.now()}-${runFiles.length}.mbin`);
      const writeResult = await writeBinaryBlocks(runPath, [block]);
      if (!writeResult.ok) {
        throw writeResult.error;
      }
      runFiles.push(runPath);

      runRows = {};
      runRowCount = 0;
      estimatedBytes = 0;
    };

    for await (const batch of iterator) {
      schema = iterator.schema;
      if (columnOrder.length === 0) {
        columnOrder =
          iterator.columnOrder.length > 0 ? iterator.columnOrder : Object.keys(batch.columns);
      }

      for (const name of columnOrder) {
        if (!runRows[name]) runRows[name] = [];
      }

      const columnRefs = columnOrder.map((name) => {
        const col = batch.columns[name];
        if (!col) throw new Error(`Sort column '${name}' missing from batch`);
        return col;
      });

      for (let row = 0; row < batch.rowCount; row++) {
        for (let colIndex = 0; colIndex < columnRefs.length; colIndex++) {
          const col = columnRefs[colIndex];
          const name = columnOrder[colIndex];
          if (!col || !name) continue;
          const value =
            col.hasNulls && col.nullBitmap && isBatchNull(col.nullBitmap, row)
              ? null
              : getBatchValue(col, row);
          runRows[name]!.push(value);
          estimatedBytes += estimateValueBytes(value, col.dtype);
        }
        runRowCount += 1;
        if (estimatedBytes >= runBytes) {
          await flushRun();
        }
      }
    }

    await flushRun();

    if (runFiles.length === 0) {
      return ok(createDataFrame());
    }

    const runs: RunData[] = [];
    for (const runFile of runFiles) {
      const readResult = await readBinaryBlocks(runFile);
      if (!readResult.ok) {
        throw readResult.error;
      }
      const runData = materializeRunFromBlocks(readResult.data.blocks);
      runs.push(runData);
    }

    const runPositions = runs.map(() => 0);
    const outputRows: Record<string, Array<number | bigint | string | boolean | null>> = {};
    for (const name of columnOrder) {
      outputRows[name] = [];
    }

    let remaining = runs.reduce((sum, run) => sum + run.rowCount, 0);

    while (remaining > 0) {
      let bestRun = -1;
      for (let i = 0; i < runs.length; i++) {
        if ((runPositions[i] ?? 0) >= runs[i]!.rowCount) continue;
        if (bestRun === -1) {
          bestRun = i;
          continue;
        }
        const cmp = compareRows(
          runs,
          i,
          runPositions[i]!,
          bestRun,
          runPositions[bestRun]!,
          sortColumns,
          directions,
        );
        if (cmp < 0) {
          bestRun = i;
        }
      }

      if (bestRun === -1) break;

      const rowIdx = runPositions[bestRun]!;
      for (const name of columnOrder) {
        outputRows[name]!.push(runs[bestRun]!.columns[name]?.[rowIdx] ?? null);
      }
      runPositions[bestRun]! += 1;
      remaining -= 1;
    }

    for (const runFile of runFiles) {
      try {
        fs.unlinkSync(runFile);
      } catch {
        // ignore cleanup failures
      }
    }

    return buildDataFrameFromColumns(
      schema,
      columnOrder,
      outputRows,
      outputRows[columnOrder[0] ?? '']?.length ?? 0,
    );
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}
