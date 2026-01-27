import { setColumnValue } from '../core/column';
import type { Schema } from '../core/schema';
import { validateSchema } from '../core/schema';
import type { DataFrame } from '../dataframe/dataframe';
import { addColumn, createDataFrame } from '../dataframe/dataframe';
import { internString } from '../memory/dictionary';
import { getDTypeSize } from '../types/dtypes';
import { DType } from '../types/dtypes';
import type { Result } from '../types/result';
import { err, ok } from '../types/result';

/**
 * CSV scanning options (streaming with chunking)
 */
export interface CsvScanOptions {
  /** Schema definition (required) */
  schema: Schema;
  /** Field delimiter (default: ",") */
  delimiter?: string;
  /** Whether first row is header (default: true) */
  hasHeader?: boolean;
  /** Custom null value representations (default: ["NA", "null", "-", ""]) */
  nullValues?: string[];
  /** Chunk size for processing (default: 50000 rows) */
  chunkSize?: number;
}

// Byte constants for zero-copy parsing
const COMMA = 44;
const CR = 13;
const LF = 10;
const MINUS = 45;
const PLUS = 43;
const DOT = 46;
const ZERO = 48;
const NINE = 57;
const CHAR_t = 116;
const CHAR_T = 84;
const CHAR_1 = 49;

/**
 * Parse float directly from bytes (zero-copy, no object creation)
 */
function parseFloatFromBytes(bytes: Uint8Array, start: number, end: number): number {
  if (start >= end) return 0;

  let i = start;
  let negative = false;

  if (bytes[i] === MINUS) {
    negative = true;
    i++;
  } else if (bytes[i] === PLUS) {
    i++;
  }

  let intPart = 0;
  while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
    intPart = intPart * 10 + (bytes[i]! - ZERO);
    i++;
  }

  let result = intPart;

  if (i < end && bytes[i] === DOT) {
    i++;
    let fracPart = 0;
    let fracDigits = 0;
    while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
      fracPart = fracPart * 10 + (bytes[i]! - ZERO);
      fracDigits++;
      i++;
    }
    if (fracDigits > 0) {
      result += fracPart / 10 ** fracDigits;
    }
  }

  return negative ? -result : result;
}

/**
 * Parse int directly from bytes (zero-copy, no object creation)
 * Returns null if the value is not a valid integer
 */
function parseIntFromBytes(bytes: Uint8Array, start: number, end: number): number | null {
  if (start >= end) return null;

  let i = start;
  let negative = false;

  if (bytes[i] === MINUS) {
    negative = true;
    i++;
  } else if (bytes[i] === PLUS) {
    i++;
  }

  const digitStart = i;
  let result = 0;
  while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
    result = result * 10 + (bytes[i]! - ZERO);
    i++;
  }

  // If we didn't parse any digits or didn't consume all characters, it's invalid
  if (i === digitStart || i < end) return null;
  return negative ? -result : result;
}

/**
 * Check if field bytes represent a null value
 */
function isNullField(
  bytes: Uint8Array,
  start: number,
  end: number,
  nullValueBytes: Uint8Array[],
): boolean {
  const len = end - start;

  // Check each null value pattern
  for (const nullBytes of nullValueBytes) {
    if (nullBytes.length !== len) continue;

    let match = true;
    for (let i = 0; i < len; i++) {
      if (bytes[start + i] !== nullBytes[i]) {
        match = false;
        break;
      }
    }
    if (match) return true;
  }

  return false;
}

/**
 * Find line starts in byte buffer
 */
function findLineStarts(bytes: Uint8Array): number[] {
  const starts: number[] = [0];
  for (let i = 0; i < bytes.length; i++) {
    if (bytes[i] === LF) {
      starts.push(i + 1);
    }
  }
  return starts;
}

/**
 * Parse header line from bytes
 */
function parseHeaderLine(
  bytes: Uint8Array,
  start: number,
  end: number,
  delimiter: number,
): string[] {
  const headers: string[] = [];
  let fieldStart = start;
  const decoder = new TextDecoder();

  for (let i = start; i <= end; i++) {
    if (i === end || bytes[i] === delimiter) {
      let fieldEnd = i;
      // When we hit end of line, include the character at 'end'
      if (i === end && i < bytes.length) fieldEnd = i + 1;
      if (fieldEnd > fieldStart && bytes[fieldEnd - 1] === CR) fieldEnd--;
      const header = decoder.decode(bytes.subarray(fieldStart, fieldEnd)).trim();
      headers.push(header);
      fieldStart = i + 1;
    }
  }

  return headers;
}

/**
 * Internal function that processes CSV data (used by both file and string inputs)
 * Works directly on Uint8Array for zero-copy, high-performance parsing
 */
async function scanCsvInternal(
  data: string | Buffer,
  options: CsvScanOptions,
): Promise<Result<DataFrame, Error>> {
  // Validate schema
  const schemaResult = validateSchema(options.schema);
  if (!schemaResult.ok) {
    return err(new Error(schemaResult.error));
  }

  const delimiter = options.delimiter?.charCodeAt(0) ?? COMMA;
  const hasHeader = options.hasHeader ?? true;
  const chunkSize = options.chunkSize ?? 50000;
  const nullValues = options.nullValues ?? ['NA', 'null', '-', ''];

  // Pre-encode null values as bytes for efficient comparison
  const encoder = new TextEncoder();
  const nullValueBytes = nullValues.map((v) => encoder.encode(v));

  // Convert to Uint8Array for byte-level operations
  const bytes = typeof data === 'string' ? new TextEncoder().encode(data) : new Uint8Array(data);

  if (bytes.length === 0) {
    return err(new Error('Empty CSV data'));
  }

  // Find all line starts
  const lineStarts = findLineStarts(bytes);

  if (lineStarts.length < 1) {
    return err(new Error('No lines found in CSV'));
  }

  // Parse header
  let headerNames: string[];
  let dataStartLine = 0;

  if (hasHeader) {
    const firstLineEnd = lineStarts[1] ?? bytes.length;
    const firstLineStart = lineStarts[0] ?? 0;
    headerNames = parseHeaderLine(bytes, firstLineStart, firstLineEnd - 1, delimiter);
    dataStartLine = 1;
  } else {
    headerNames = Object.keys(options.schema);
  }

  // Validate schema matches header
  const schemaKeys = Object.keys(options.schema);
  if (schemaKeys.length !== headerNames.length) {
    return err(
      new Error(
        `Schema column count (${schemaKeys.length}) doesn't match CSV columns (${headerNames.length})`,
      ),
    );
  }

  if (hasHeader) {
    for (const header of headerNames) {
      if (!schemaKeys.includes(header)) {
        return err(new Error(`Schema missing column from CSV header: ${header}`));
      }
    }
  }

  // Calculate total data rows
  // If file doesn't end with newline, we have one more line than lineStarts indicates
  let totalRows = lineStarts.length - dataStartLine;

  // Check if last line is empty (trim trailing empty lines)
  while (totalRows > 0) {
    const lineIdx = dataStartLine + totalRows - 1;
    const start = lineStarts[lineIdx];
    const end = lineIdx + 1 < lineStarts.length ? lineStarts[lineIdx + 1] : bytes.length;
    if (start === undefined || end === undefined) break;

    // Check if line has content (more than just CR/LF)
    let hasContent = false;
    for (let i = start; i < end; i++) {
      if (bytes[i] !== CR && bytes[i] !== LF) {
        hasContent = true;
        break;
      }
    }
    if (hasContent) break;
    totalRows--;
  }

  // Create empty DataFrame
  const df = createDataFrame();

  if (totalRows === 0) {
    return ok(df);
  }

  // Create columns with total capacity
  for (const colName of headerNames) {
    const dtype = options.schema[colName];
    if (!dtype) return err(new Error(`Missing dtype for column: ${colName}`));
    const addResult = addColumn(df, colName, dtype, totalRows);
    if (!addResult.ok) {
      return err(new Error(addResult.error));
    }
  }

  const decoder = new TextDecoder();
  let currentRow = 0;

  // Process data in chunks for memory efficiency
  for (
    let chunkStart = dataStartLine;
    chunkStart < dataStartLine + totalRows;
    chunkStart += chunkSize
  ) {
    const chunkEnd = Math.min(chunkStart + chunkSize, dataStartLine + totalRows);

    // HOT LOOP: Process each line - zero object creation
    for (let lineIdx = chunkStart; lineIdx < chunkEnd; lineIdx++) {
      const lineStart = lineStarts[lineIdx];
      const lineEnd = lineStarts[lineIdx + 1] ?? bytes.length;
      if (lineStart === undefined) continue;

      let fieldStart = lineStart;
      let colIdx = 0;

      // HOT LOOP: Parse fields - zero object creation
      for (let i = lineStart; i <= lineEnd && colIdx < headerNames.length; i++) {
        const isEndOfLine = i === lineEnd || bytes[i] === LF;
        const isDelimiter = bytes[i] === delimiter;

        if (isDelimiter || isEndOfLine) {
          let fieldEnd: number = i;
          if (fieldEnd > fieldStart && bytes[fieldEnd - 1] === CR) fieldEnd--;

          const colName = headerNames[colIdx];
          const dtype = colName ? options.schema[colName] : undefined;
          const column = colName && dtype ? df.columns.get(colName) : undefined;

          if (colName && dtype && !column) {
            return err(new Error(`Column not found: ${colName}`));
          }

          // Check for null
          const isNull = isNullField(bytes, fieldStart, fieldEnd, nullValueBytes);

          // Write value directly to column buffer using DataView
          const bytesPerElement = dtype ? getDTypeSize(dtype) : 0;
          const byteOffset = currentRow * bytesPerElement;

          if (column && dtype && !isNull) {
            switch (dtype) {
              case DType.Float64:
                column.view.setFloat64(
                  byteOffset,
                  parseFloatFromBytes(bytes, fieldStart, fieldEnd),
                  true,
                );
                break;

              case DType.Int32: {
                const parsed = parseIntFromBytes(bytes, fieldStart, fieldEnd);
                if (parsed === null) {
                  const badValue = decoder.decode(bytes.subarray(fieldStart, fieldEnd));
                  return err(
                    new Error(
                      `Invalid integer value at row ${currentRow + 1}, column '${colName}': ${badValue}`,
                    ),
                  );
                }
                column.view.setInt32(byteOffset, parsed, true);
                break;
              }

              case DType.Bool: {
                const byte = bytes[fieldStart];
                column.view.setUint8(
                  byteOffset,
                  byte === CHAR_t || byte === CHAR_T || byte === CHAR_1 ? 1 : 0,
                );
                break;
              }

              case DType.String: {
                const str = decoder.decode(bytes.subarray(fieldStart, fieldEnd));
                const dictId = df.dictionary ? internString(df.dictionary, str) : 0;
                column.view.setInt32(byteOffset, dictId, true);
                break;
              }

              case DType.DateTime:
              case DType.Date: {
                const timestamp = BigInt(
                  Math.floor(parseFloatFromBytes(bytes, fieldStart, fieldEnd)),
                );
                column.view.setBigInt64(byteOffset, timestamp, true);
                break;
              }
            }
          }
          // else: already zero-initialized

          fieldStart = i + 1;
          colIdx++;
        }
      }

      currentRow++;
    }
  }

  return ok(df);
}

/**
 * Scan CSV from a file path into a DataFrame (streaming with chunking)
 * Handles file reading internally using Bun's efficient file I/O
 *
 * @param path - Path to CSV file
 * @param options - CSV scanning options
 * @example
 * ```ts
 * const result = await scanCsv('data.csv', {
 *   schema: { id: DType.Int32, name: DType.String }
 * });
 * if (result.ok) {
 *   console.log(result.value);
 * }
 * ```
 */
export async function scanCsv<S extends Record<string, DType>>(
  path: string,
  options: CsvScanOptions & { schema: S },
): Promise<Result<DataFrame<InferSchemaType<S>>, Error>> {
  try {
    // Use Bun's file API for efficient file reading
    const file = Bun.file(path);
    const text = await file.text();
    return scanCsvInternal(text, options) as unknown as Promise<
      Result<DataFrame<InferSchemaType<S>>, Error>
    >;
  } catch (error) {
    return err(
      new Error(
        `Failed to read file '${path}': ${error instanceof Error ? error.message : String(error)}`,
      ),
    );
  }
}

/**
 * Scan CSV from a string into a DataFrame (streaming with chunking)
 * Use this when you already have CSV data in memory
 *
 * @param data - CSV string or Buffer data
 * @param options - CSV scanning options
 * @example
 * ```ts
 * const csvData = "id,name\n1,Alice\n2,Bob";
 * const result = await scanCsvFromString(csvData, {
 *   schema: { id: DType.Int32, name: DType.String }
 * });
 * ```
 */
export async function scanCsvFromString<S extends Record<string, DType>>(
  data: string | Buffer,
  options: CsvScanOptions & { schema: S },
): Promise<Result<DataFrame<InferSchemaType<S>>, Error>> {
  return scanCsvInternal(data, options) as unknown as Promise<
    Result<DataFrame<InferSchemaType<S>>, Error>
  >;
}

// Helper type for type inference
import type { InferSchemaType } from '../lazyframe/types';
