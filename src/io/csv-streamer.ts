import * as fs from 'node:fs';
import type { Schema } from '../core/schema';
import { validateSchema } from '../core/schema';
import type { MemoryBudget } from '../memory/budget';
import { isNearLimit, trackAllocation, trackDeallocation } from '../memory/budget';
import { DType } from '../types/dtypes';
import type { FilterOperator } from '../types/operators';
import type { Result } from '../types/result';
import { err, ok } from '../types/result';
import { type BinaryBlock, BinaryBlockWriter } from './binary-format';
import {
  type ColumnarBatch,
  ColumnarBatchBuilder,
  type ColumnarBatchIterator,
  DEFAULT_BATCH_BYTES,
} from './columnar-batch';
import {
  CHAR_1,
  CHAR_T,
  CHAR_t,
  COMMA,
  CR,
  LF,
  evaluatePredicate,
  isNullField,
  parseBoolStrict,
  parseFloatFromBytes,
  parseFloatFromBytesStrict,
  parseHeaderLine,
  parseIntFromBytes,
} from './csv-streamer-utils';

export interface CsvStreamOptions {
  /** Schema definition (required) */
  schema: Schema;
  /** Field delimiter (default: ",") */
  delimiter?: string;
  /** Whether first row is header (default: true) */
  hasHeader?: boolean;
  /** Custom null value representations (default: ["NA", "null", "-", ""]) */
  nullValues?: string[];
  /** Target batch size in bytes (default: 512KB) */
  batchSizeBytes?: number;
  /** Drop rows that don't conform to schema (default: false) */
  dropInvalidRows?: boolean;
  /** Optional cache path for .mbin streaming writes */
  cachePath?: string;
  /** Delete cache file if stream is terminated early (default: true) */
  deleteCacheOnAbort?: boolean;
  /** Delete cache file after successful completion (default: false) */
  deleteCacheOnComplete?: boolean;
  /** Optional predicate pushdown filters */
  predicates?: CsvStreamPredicate[];
  /** Optional column pruning (only yield these columns) */
  requiredColumns?: Set<string>;
  /** Optional memory budget for backpressure */
  memoryBudget?: MemoryBudget;
}

export interface CsvStreamPredicate {
  columnName: string;
  operator: FilterOperator;
  value: number | bigint | string | boolean | Array<number | bigint | string | boolean>;
}

async function* readChunks(path: string): AsyncGenerator<Uint8Array> {
  if (typeof (globalThis as { Bun?: unknown }).Bun !== 'undefined') {
    const file = (
      globalThis as { Bun: { file: (p: string) => { stream: () => ReadableStream<Uint8Array> } } }
    ).Bun.file(path);
    const stream = file.stream();
    const reader = stream.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value) yield value;
      }
    } finally {
      reader.releaseLock();
    }
  } else {
    const stream = fs.createReadStream(path);
    for await (const chunk of stream) {
      yield chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk as Buffer);
    }
  }
}

function concatBytes(a: Uint8Array, b: Uint8Array): Uint8Array {
  if (a.length === 0) return b;
  if (b.length === 0) return a;
  const combined = new Uint8Array(a.length + b.length);
  combined.set(a, 0);
  combined.set(b, a.length);
  return combined;
}

export async function streamCsvBatches(
  path: string,
  options: CsvStreamOptions,
): Promise<Result<ColumnarBatchIterator, Error>> {
  try {
    const schemaResult = validateSchema(options.schema);
    if (!schemaResult.ok) {
      return err(new Error(schemaResult.error));
    }

    const delimiter = options.delimiter?.charCodeAt(0) ?? COMMA;
    const hasHeader = options.hasHeader ?? true;
    const nullValues = options.nullValues ?? ['NA', 'null', '-', ''];
    const batchSizeBytes = options.batchSizeBytes ?? DEFAULT_BATCH_BYTES;
    const dropInvalidRows = options.dropInvalidRows ?? false;
    const cachePath = options.cachePath;
    const deleteCacheOnAbort = options.deleteCacheOnAbort ?? true;
    const deleteCacheOnComplete = options.deleteCacheOnComplete ?? false;
    const predicates = options.predicates ?? [];
    const requiredColumns = options.requiredColumns;
    const memoryBudget = options.memoryBudget;

    const encoder = new TextEncoder();
    const decoder = new TextDecoder();
    const nullValueBytes = nullValues.map((v) => encoder.encode(v));

    let columnOrder = hasHeader ? [] : Object.keys(options.schema);
    let outputColumns = columnOrder;
    let outputIndex = new Map<string, number>();
    let predicateIndex = new Map<number, CsvStreamPredicate[]>();

    const iterator: ColumnarBatchIterator = {
      schema: options.schema,
      columnOrder: outputColumns,
      batchSizeBytes,
      async *[Symbol.asyncIterator](): AsyncGenerator<ColumnarBatch> {
        let remainder = new Uint8Array(0);
        let headerParsed = !hasHeader;
        let builder = new ColumnarBatchBuilder(options.schema, outputColumns, batchSizeBytes);
        let cacheWriter: BinaryBlockWriter | null = cachePath
          ? new BinaryBlockWriter(cachePath)
          : null;
        let cacheBlockId = 0;
        let completed = false;

        const writeCacheBlock = (batch: ColumnarBatch): void => {
          if (!cacheWriter) return;
          const block: BinaryBlock = {
            blockId: cacheBlockId++,
            rowCount: batch.rowCount,
            columns: batch.columns,
          };
          const writeResult = cacheWriter.writeBlock(block);
          if (!writeResult.ok) {
            throw writeResult.error instanceof Error
              ? writeResult.error
              : new Error(String(writeResult.error));
          }
        };

        const rebuildOutputMappings = (): void => {
          const requiredSet = requiredColumns ?? new Set(columnOrder);
          outputColumns = columnOrder.filter((name) => requiredSet.has(name));
          iterator.columnOrder = outputColumns;
          outputIndex = new Map(outputColumns.map((name, idx) => [name, idx]));

          predicateIndex = new Map();
          for (const pred of predicates) {
            const idx = columnOrder.indexOf(pred.columnName);
            if (idx === -1) {
              throw new Error(`Predicate column '${pred.columnName}' not found in CSV`);
            }
            const list = predicateIndex.get(idx) ?? [];
            list.push(pred);
            predicateIndex.set(idx, list);
          }

          builder = new ColumnarBatchBuilder(options.schema, outputColumns, batchSizeBytes);
        };

        if (!hasHeader) {
          rebuildOutputMappings();
        }

        const waitForBudget = async (): Promise<void> => {
          if (!memoryBudget) return;
          while (isNearLimit(memoryBudget)) {
            await new Promise((resolve) => setTimeout(resolve, 0));
          }
        };

        const processLine = (
          bytes: Uint8Array,
          start: number,
          end: number,
        ): ColumnarBatch | null => {
          if (start >= end) return null;

          if (!headerParsed) {
            columnOrder = parseHeaderLine(bytes, start, end, delimiter);

            const schemaKeys = Object.keys(options.schema);
            if (schemaKeys.length !== columnOrder.length) {
              throw new Error(
                `Schema column count (${schemaKeys.length}) doesn't match CSV columns (${columnOrder.length})`,
              );
            }

            for (const header of columnOrder) {
              if (!schemaKeys.includes(header)) {
                throw new Error(`Schema missing column from CSV header: ${header}`);
              }
            }

            rebuildOutputMappings();
            headerParsed = true;
            return null;
          }

          let fieldStart = start;
          let colIdx = 0;

          const rowValues: Array<{
            value: number | bigint | string | boolean | null;
            bytes?: number;
          }> = new Array(outputColumns.length);
          let rowValid = true;
          let rowPass = true;

          for (let i = start; i <= end && colIdx < columnOrder.length; i++) {
            const isEndOfLine = i === end;
            const isDelimiter = bytes[i] === delimiter;

            if (isDelimiter || isEndOfLine) {
              let fieldEnd = i;
              if (fieldEnd > fieldStart && bytes[fieldEnd - 1] === CR) fieldEnd--;

              const colName = columnOrder[colIdx];
              const dtype = colName ? options.schema[colName] : undefined;
              if (!dtype) {
                throw new Error(`Missing dtype for column: ${colName}`);
              }

              const isNull = isNullField(bytes, fieldStart, fieldEnd, nullValueBytes);
              let parsedValue: number | bigint | string | boolean | null = null;
              let parsedValid = true;
              let parsedBytes: number | undefined;

              if (!isNull) {
                switch (dtype) {
                  case DType.Float64: {
                    if (dropInvalidRows) {
                      const parsed = parseFloatFromBytesStrict(bytes, fieldStart, fieldEnd);
                      parsedValue = parsed.value;
                      parsedValid = parsed.valid;
                    } else {
                      parsedValue = parseFloatFromBytes(bytes, fieldStart, fieldEnd);
                    }
                    break;
                  }
                  case DType.Int32: {
                    const value = parseIntFromBytes(bytes, fieldStart, fieldEnd);
                    parsedValue = value === null ? null : value;
                    parsedValid = value !== null;
                    break;
                  }
                  case DType.Bool: {
                    if (dropInvalidRows) {
                      const parsed = parseBoolStrict(bytes, fieldStart, fieldEnd);
                      parsedValue = parsed.value;
                      parsedValid = parsed.valid;
                    } else {
                      const byte = fieldStart < fieldEnd ? bytes[fieldStart] : undefined;
                      const boolVal = byte === CHAR_t || byte === CHAR_T || byte === CHAR_1;
                      parsedValue = boolVal;
                    }
                    break;
                  }
                  case DType.String: {
                    const str = decoder.decode(bytes.subarray(fieldStart, fieldEnd));
                    parsedValue = str;
                    parsedBytes = fieldEnd - fieldStart;
                    break;
                  }
                  case DType.Date:
                  case DType.DateTime: {
                    if (dropInvalidRows) {
                      const parsed = parseFloatFromBytesStrict(bytes, fieldStart, fieldEnd);
                      parsedValue = BigInt(Math.floor(parsed.value));
                      parsedValid = parsed.valid;
                    } else {
                      const timestamp = BigInt(
                        Math.floor(parseFloatFromBytes(bytes, fieldStart, fieldEnd)),
                      );
                      parsedValue = timestamp;
                    }
                    break;
                  }
                }
              }

              if (dropInvalidRows && !parsedValid) {
                rowValid = false;
              }

              const outputIdx = outputIndex.get(colName!);
              if (outputIdx !== undefined) {
                rowValues[outputIdx] = { value: isNull ? null : parsedValue, bytes: parsedBytes };
              }

              const predicateList = predicateIndex.get(colIdx);
              if (predicateList && predicateList.length > 0) {
                if (isNull || parsedValue === null || !parsedValid) {
                  rowPass = false;
                } else {
                  for (const pred of predicateList) {
                    if (!evaluatePredicate(pred.operator, parsedValue, pred.value)) {
                      rowPass = false;
                      break;
                    }
                  }
                }
              }

              fieldStart = i + 1;
              colIdx++;
            }
          }

          if (colIdx < columnOrder.length && dropInvalidRows) {
            rowValid = false;
          }

          if (!rowPass || (dropInvalidRows && !rowValid)) {
            return null;
          }

          for (let i = 0; i < rowValues.length; i++) {
            const entry = rowValues[i];
            if (!entry) {
              if (dropInvalidRows) {
                return null;
              }
              builder.appendValue(null);
              continue;
            }
            builder.appendValue(entry.value, entry.bytes);
          }

          return builder.endRow();
        };

        let finalizeError: Error | null = null;

        try {
          for await (const chunk of readChunks(path)) {
            const data = concatBytes(remainder, chunk);
            let lineStart = 0;

            for (let i = 0; i < data.length; i++) {
              if (data[i] === LF) {
                const batch = processLine(data, lineStart, i);
                if (batch) {
                  writeCacheBlock(batch);
                  if (memoryBudget) {
                    trackAllocation(memoryBudget, batch.byteSize);
                  }
                  yield batch;
                  if (memoryBudget) {
                    trackDeallocation(memoryBudget, batch.byteSize);
                  }
                }
                lineStart = i + 1;
              }
            }

            remainder = lineStart < data.length ? data.slice(lineStart) : new Uint8Array(0);
            await waitForBudget();
          }

          if (remainder.length > 0) {
            const batch = processLine(remainder, 0, remainder.length);
            if (batch) {
              writeCacheBlock(batch);
              if (memoryBudget) {
                trackAllocation(memoryBudget, batch.byteSize);
              }
              yield batch;
              if (memoryBudget) {
                trackDeallocation(memoryBudget, batch.byteSize);
              }
            }
          }

          const finalBatch = builder.flush();
          if (finalBatch) {
            writeCacheBlock(finalBatch);
            if (memoryBudget) {
              trackAllocation(memoryBudget, finalBatch.byteSize);
            }
            yield finalBatch;
            if (memoryBudget) {
              trackDeallocation(memoryBudget, finalBatch.byteSize);
            }
          }

          completed = true;
        } finally {
          if (cacheWriter) {
            if (completed) {
              const finalizeResult = cacheWriter.finalize();
              if (!finalizeResult.ok) {
                cacheWriter.abort(true);
                finalizeError =
                  finalizeResult.error instanceof Error
                    ? finalizeResult.error
                    : new Error(String(finalizeResult.error));
              }
              if (deleteCacheOnComplete && cachePath) {
                try {
                  fs.unlinkSync(cachePath);
                } catch {
                  // ignore
                }
              }
            } else {
              cacheWriter.abort(deleteCacheOnAbort);
            }
            cacheWriter = null;
          }
        }

        if (finalizeError) {
          throw finalizeError;
        }
      },
    };

    return ok(iterator);
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}
