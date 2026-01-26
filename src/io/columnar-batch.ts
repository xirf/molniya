import { DType, getDTypeSize } from '../types/dtypes';

export const DEFAULT_BATCH_BYTES = 512 * 1024; // 512KB

export type ColumnarData = Int32Array | Float64Array | Uint8Array | BigInt64Array | string[];

export interface ColumnarBatchColumn {
  dtype: DType;
  data: ColumnarData;
  hasNulls: boolean;
  nullBitmap?: Uint8Array;
}

export interface ColumnarBatch {
  rowCount: number;
  byteSize: number;
  columns: Record<string, ColumnarBatchColumn>;
}

export interface ColumnarBatchIterator extends AsyncIterable<ColumnarBatch> {
  schema: Record<string, DType>;
  columnOrder: string[];
  batchSizeBytes: number;
}

interface ColumnBuffer {
  dtype: DType;
  values?: string[];
  typed?: Int32Array | Float64Array | Uint8Array | BigInt64Array;
  hasNulls: boolean;
  nullBitmap?: Uint8Array;
  stringBytes: number;
  bytesPerElement: number;
}

function setNullBit(bitmap: Uint8Array, index: number): void {
  const byteIndex = Math.floor(index / 8);
  const bitIndex = index % 8;
  bitmap[byteIndex] = (bitmap[byteIndex] ?? 0) | (1 << bitIndex);
}

function ensureNullBitmap(bitmap: Uint8Array | undefined, length: number): Uint8Array {
  const requiredBytes = Math.ceil(length / 8);
  if (!bitmap || bitmap.length < requiredBytes) {
    const next = new Uint8Array(requiredBytes);
    if (bitmap) next.set(bitmap);
    return next;
  }
  return bitmap;
}

function measureStringBytes(value: string, encoder: TextEncoder): number {
  return encoder.encode(value).length;
}

export class ColumnarBatchBuilder {
  private readonly schema: Record<string, DType>;
  private readonly columnOrder: string[];
  private readonly targetBytes: number;
  private readonly encoder: TextEncoder;
  private buffers: Map<string, ColumnBuffer>;
  private readonly capacity: number;
  private readonly fixedBytesPerRow: number;
  private currentColumnIndex = 0;
  private rowCount = 0;
  private estimatedBytes = 0;

  constructor(
    schema: Record<string, DType>,
    columnOrder: string[] = Object.keys(schema),
    targetBytes: number = DEFAULT_BATCH_BYTES,
  ) {
    this.schema = schema;
    this.columnOrder = columnOrder;
    this.targetBytes = targetBytes;
    this.encoder = new TextEncoder();
    this.fixedBytesPerRow = this.columnOrder.reduce((sum, name) => {
      const dtype = this.schema[name];
      if (!dtype || dtype === DType.String) return sum;
      return sum + getDTypeSize(dtype);
    }, 0);
    this.capacity =
      this.fixedBytesPerRow > 0
        ? Math.max(1, Math.floor(targetBytes / this.fixedBytesPerRow))
        : 1024;
    this.buffers = this.createBuffers();
  }

  getRowCount(): number {
    return this.rowCount;
  }

  getEstimatedBytes(): number {
    return this.estimatedBytes;
  }

  appendValue(value: number | bigint | string | boolean | null, byteLength?: number): void {
    const colName = this.columnOrder[this.currentColumnIndex];
    if (!colName) {
      throw new Error('Column index out of bounds');
    }

    const buffer = this.buffers.get(colName);
    if (!buffer) {
      throw new Error(`Missing column buffer: ${colName}`);
    }

    if (value === null) {
      buffer.hasNulls = true;
      buffer.nullBitmap = ensureNullBitmap(buffer.nullBitmap, this.rowCount + 1);
      setNullBit(buffer.nullBitmap, this.rowCount);

      // Store default zero value for fixed-size types
      switch (buffer.dtype) {
        case DType.Int32:
        case DType.Float64:
        case DType.Bool:
          this.ensureTypedCapacity(buffer, this.rowCount + 1);
          if (!buffer.typed) throw new Error('Missing typed buffer');
          buffer.typed[this.rowCount] = 0;
          this.estimatedBytes += getDTypeSize(buffer.dtype);
          break;
        case DType.Date:
        case DType.DateTime:
          this.ensureTypedCapacity(buffer, this.rowCount + 1);
          if (!buffer.typed) throw new Error('Missing typed buffer');
          (buffer.typed as BigInt64Array)[this.rowCount] = 0n;
          this.estimatedBytes += getDTypeSize(buffer.dtype);
          break;
        case DType.String:
          if (!buffer.values) buffer.values = [];
          buffer.values[this.rowCount] = '';
          buffer.stringBytes += 0;
          this.estimatedBytes += 4; // length prefix only
          break;
      }
    } else {
      switch (buffer.dtype) {
        case DType.Int32:
        case DType.Float64: {
          const num = typeof value === 'number' ? value : Number(value);
          this.ensureTypedCapacity(buffer, this.rowCount + 1);
          if (!buffer.typed) throw new Error('Missing typed buffer');
          buffer.typed[this.rowCount] = num;
          this.estimatedBytes += getDTypeSize(buffer.dtype);
          break;
        }
        case DType.Bool: {
          const boolVal = typeof value === 'boolean' ? (value ? 1 : 0) : Number(value);
          this.ensureTypedCapacity(buffer, this.rowCount + 1);
          if (!buffer.typed) throw new Error('Missing typed buffer');
          buffer.typed[this.rowCount] = boolVal;
          this.estimatedBytes += getDTypeSize(buffer.dtype);
          break;
        }
        case DType.Date:
        case DType.DateTime: {
          const bigintVal = typeof value === 'bigint' ? value : BigInt(value as number);
          this.ensureTypedCapacity(buffer, this.rowCount + 1);
          if (!buffer.typed) throw new Error('Missing typed buffer');
          (buffer.typed as BigInt64Array)[this.rowCount] = bigintVal;
          this.estimatedBytes += getDTypeSize(buffer.dtype);
          break;
        }
        case DType.String: {
          const str = String(value);
          if (!buffer.values) buffer.values = [];
          buffer.values[this.rowCount] = str;
          const bytes = byteLength ?? measureStringBytes(str, this.encoder);
          buffer.stringBytes += bytes;
          this.estimatedBytes += 4 + bytes; // length prefix + bytes
          break;
        }
      }
    }

    this.currentColumnIndex += 1;
  }

  endRow(): ColumnarBatch | null {
    if (this.currentColumnIndex !== this.columnOrder.length) {
      throw new Error('Row has missing columns');
    }

    this.rowCount += 1;
    this.currentColumnIndex = 0;

    if (this.rowCount >= this.capacity || this.estimatedBytes >= this.targetBytes) {
      return this.buildBatch();
    }

    return null;
  }

  flush(): ColumnarBatch | null {
    if (this.rowCount === 0) return null;
    return this.buildBatch();
  }

  private createBuffers(): Map<string, ColumnBuffer> {
    const buffers = new Map<string, ColumnBuffer>();
    for (const name of this.columnOrder) {
      const dtype = this.schema[name];
      if (!dtype) {
        throw new Error(`Missing dtype for column: ${name}`);
      }

      const bytesPerElement = dtype === DType.String ? 0 : getDTypeSize(dtype);
      let typed: ColumnBuffer['typed'];
      let values: string[] | undefined;

      if (dtype === DType.String) {
        values = [];
      } else if (dtype === DType.Int32) {
        typed = new Int32Array(this.capacity);
      } else if (dtype === DType.Float64) {
        typed = new Float64Array(this.capacity);
      } else if (dtype === DType.Bool) {
        typed = new Uint8Array(this.capacity);
      } else if (dtype === DType.Date || dtype === DType.DateTime) {
        typed = new BigInt64Array(this.capacity);
      }

      buffers.set(name, {
        dtype,
        values,
        typed,
        hasNulls: false,
        stringBytes: 0,
        bytesPerElement,
      });
    }
    return buffers;
  }

  private ensureTypedCapacity(buffer: ColumnBuffer, requiredLength: number): void {
    if (!buffer.typed) return;
    if (buffer.typed.length >= requiredLength) return;

    const nextLength = Math.max(requiredLength, buffer.typed.length * 2);
    if (buffer.dtype === DType.Int32) {
      const next = new Int32Array(nextLength);
      next.set(buffer.typed as Int32Array);
      buffer.typed = next;
    } else if (buffer.dtype === DType.Float64) {
      const next = new Float64Array(nextLength);
      next.set(buffer.typed as Float64Array);
      buffer.typed = next;
    } else if (buffer.dtype === DType.Bool) {
      const next = new Uint8Array(nextLength);
      next.set(buffer.typed as Uint8Array);
      buffer.typed = next;
    } else if (buffer.dtype === DType.Date || buffer.dtype === DType.DateTime) {
      const next = new BigInt64Array(nextLength);
      next.set(buffer.typed as BigInt64Array);
      buffer.typed = next;
    }
  }

  private buildBatch(): ColumnarBatch {
    const columns: Record<string, ColumnarBatchColumn> = {};
    let byteSize = 0;

    for (const name of this.columnOrder) {
      const buffer = this.buffers.get(name);
      if (!buffer) continue;

      const { dtype, hasNulls, nullBitmap } = buffer;
      let data: ColumnarData;

      switch (dtype) {
        case DType.Int32:
          data = (buffer.typed as Int32Array).subarray(0, this.rowCount);
          byteSize += this.rowCount * getDTypeSize(dtype);
          break;
        case DType.Float64:
          data = (buffer.typed as Float64Array).subarray(0, this.rowCount);
          byteSize += this.rowCount * getDTypeSize(dtype);
          break;
        case DType.Bool:
          data = (buffer.typed as Uint8Array).subarray(0, this.rowCount);
          byteSize += this.rowCount * getDTypeSize(dtype);
          break;
        case DType.Date:
        case DType.DateTime:
          data = (buffer.typed as BigInt64Array).subarray(0, this.rowCount);
          byteSize += this.rowCount * getDTypeSize(dtype);
          break;
        case DType.String:
          data = buffer.values ?? [];
          byteSize += buffer.stringBytes + data.length * 4;
          break;
      }

      if (hasNulls && nullBitmap) {
        byteSize += nullBitmap.byteLength;
      }

      columns[name] = {
        dtype,
        data,
        hasNulls,
        nullBitmap,
      };
    }

    const batch: ColumnarBatch = {
      rowCount: this.rowCount,
      byteSize,
      columns,
    };

    this.reset();
    return batch;
  }

  private reset(): void {
    this.buffers = this.createBuffers();
    this.rowCount = 0;
    this.estimatedBytes = 0;
    this.currentColumnIndex = 0;
  }
}
