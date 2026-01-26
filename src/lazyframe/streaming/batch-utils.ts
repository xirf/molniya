import type { ColumnarBatchColumn } from '../../io/columnar-batch';
import { DType, getTypedArrayConstructor } from '../../types/dtypes';
import type { FilterOperator } from '../../types/operators';

export interface BatchFilter {
  column: string;
  operator: FilterOperator;
  value: number | bigint | string | boolean | Array<number | bigint | string | boolean>;
}

export function isBatchNull(bitmap: Uint8Array, index: number): boolean {
  const byteIndex = index >> 3;
  const bitIndex = index & 7;
  return ((bitmap[byteIndex] ?? 0) & (1 << bitIndex)) !== 0;
}

export function setBatchNull(bitmap: Uint8Array, index: number): void {
  const byteIndex = index >> 3;
  const bitIndex = index & 7;
  bitmap[byteIndex] = (bitmap[byteIndex] ?? 0) | (1 << bitIndex);
}

export function buildSchemaFromColumns(
  schema: Record<string, DType>,
  columnOrder: string[],
): Record<string, DType> {
  const next: Record<string, DType> = {};
  for (const name of columnOrder) {
    const dtype = schema[name];
    if (dtype) next[name] = dtype;
  }
  return next;
}

export function getBatchValue(
  column: ColumnarBatchColumn,
  rowIndex: number,
): number | bigint | string | boolean | null {
  switch (column.dtype) {
    case DType.String:
      return (column.data as string[])[rowIndex] ?? '';
    case DType.Bool:
      return (column.data as Uint8Array)[rowIndex] ?? 0;
    case DType.Int32:
      return (column.data as Int32Array)[rowIndex] ?? 0;
    case DType.Float64:
      return (column.data as Float64Array)[rowIndex] ?? 0;
    case DType.Date:
    case DType.DateTime:
      return (column.data as BigInt64Array)[rowIndex] ?? 0n;
    default:
      return null;
  }
}

export function matchesFilter(
  rowValue: number | bigint | string | boolean | null,
  operator: FilterOperator,
  compareValue: number | bigint | string | boolean | Array<number | bigint | string | boolean>,
): boolean {
  if (operator === 'in' || operator === 'not-in') {
    if (!Array.isArray(compareValue)) return false;
    const contains = compareValue.includes(rowValue as number | bigint | string | boolean);
    return operator === 'in' ? contains : !contains;
  }

  if (Array.isArray(compareValue)) return false;

  switch (operator) {
    case '==':
      return rowValue === compareValue;
    case '!=':
      return rowValue !== compareValue;
    case '>':
      if (typeof rowValue === 'number' && typeof compareValue === 'number') {
        return rowValue > compareValue;
      }
      if (typeof rowValue === 'bigint' && typeof compareValue === 'bigint') {
        return rowValue > compareValue;
      }
      return false;
    case '<':
      if (typeof rowValue === 'number' && typeof compareValue === 'number') {
        return rowValue < compareValue;
      }
      if (typeof rowValue === 'bigint' && typeof compareValue === 'bigint') {
        return rowValue < compareValue;
      }
      return false;
    case '>=':
      if (typeof rowValue === 'number' && typeof compareValue === 'number') {
        return rowValue >= compareValue;
      }
      if (typeof rowValue === 'bigint' && typeof compareValue === 'bigint') {
        return rowValue >= compareValue;
      }
      return false;
    case '<=':
      if (typeof rowValue === 'number' && typeof compareValue === 'number') {
        return rowValue <= compareValue;
      }
      if (typeof rowValue === 'bigint' && typeof compareValue === 'bigint') {
        return rowValue <= compareValue;
      }
      return false;
  }
}

export function computeBatchByteSize(columns: Record<string, ColumnarBatchColumn>): number {
  let total = 0;
  const encoder = new TextEncoder();

  for (const column of Object.values(columns)) {
    if (Array.isArray(column.data)) {
      for (const str of column.data) {
        const bytes = encoder.encode(str ?? '').length;
        total += 4 + bytes;
      }
    } else {
      total += column.data.byteLength;
    }

    if (column.hasNulls && column.nullBitmap) {
      total += column.nullBitmap.byteLength;
    }
  }

  return total;
}

export function buildFilteredColumn(
  column: ColumnarBatchColumn,
  indices: number[],
): ColumnarBatchColumn {
  const rowCount = indices.length;
  let hasNulls = false;
  let nullBitmap: Uint8Array | undefined;

  if (column.dtype === DType.String) {
    const data = new Array<string>(rowCount);
    if (column.hasNulls) {
      nullBitmap = new Uint8Array(Math.ceil(rowCount / 8));
    }
    for (let i = 0; i < rowCount; i++) {
      const srcIndex = indices[i];
      if (srcIndex === undefined) continue;
      const value = (column.data as string[])[srcIndex] ?? '';
      data[i] = value;
      if (column.hasNulls && column.nullBitmap && isBatchNull(column.nullBitmap, srcIndex)) {
        hasNulls = true;
        if (nullBitmap) setBatchNull(nullBitmap, i);
      }
    }
    return {
      dtype: column.dtype,
      data,
      hasNulls,
      nullBitmap: hasNulls ? nullBitmap : undefined,
    };
  }

  const ctor = getTypedArrayConstructor(column.dtype);
  const data = new ctor(rowCount) as Int32Array | Float64Array | Uint8Array | BigInt64Array;
  if (column.hasNulls) {
    nullBitmap = new Uint8Array(Math.ceil(rowCount / 8));
  }

  for (let i = 0; i < rowCount; i++) {
    const srcIndex = indices[i];
    if (srcIndex === undefined) continue;

    if (column.hasNulls && column.nullBitmap && isBatchNull(column.nullBitmap, srcIndex)) {
      hasNulls = true;
      if (nullBitmap) setBatchNull(nullBitmap, i);
      data[i] = column.dtype === DType.Date || column.dtype === DType.DateTime ? 0n : 0;
      continue;
    }

    if (column.dtype === DType.Date || column.dtype === DType.DateTime) {
      (data as BigInt64Array)[i] = (column.data as BigInt64Array)[srcIndex] ?? 0n;
    } else {
      (data as Int32Array | Float64Array | Uint8Array)[i] =
        (column.data as Int32Array | Float64Array | Uint8Array)[srcIndex] ?? 0;
    }
  }

  return {
    dtype: column.dtype,
    data,
    hasNulls,
    nullBitmap: hasNulls ? nullBitmap : undefined,
  };
}
