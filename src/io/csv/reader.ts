import { DataFrame } from '../../core/dataframe';
import { Series } from '../../core/series';
import type { DTypeKind, Schema } from '../../core/types';
import { inferColumnType } from './inference';
import { type CsvOptions, DEFAULT_CSV_OPTIONS } from './options';
import {
  type CsvReadResult,
  type ParseFailures,
  createParseFailures,
  recordFailure,
} from './parse-result';

/**
 * Ultra-fast CSV reader using optimized byte-level parsing.
 *
 * Performance profile (387MB, 7.38M rows):
 * - Target: ~1.5s on Bun
 * - Approach: Direct Uint8Array processing, minimal allocations
 * 
 * Returns a CsvReadResult with the DataFrame and any parse errors.
 */
export async function readCsv<S extends Schema = Schema>(
  path: string,
  options?: CsvOptions & { schema?: S },
): Promise<CsvReadResult<S>> {
  const opts = { ...DEFAULT_CSV_OPTIONS, ...options };
  const providedSchema = options?.schema;
  const trackErrors = opts.trackErrors ?? true;

  // Read file - keep both Buffer (for SIMD indexOf) and Uint8Array (for parsing)
  const file = Bun.file(path);
  const arrayBuffer = await file.arrayBuffer();
  const buffer = Buffer.from(arrayBuffer); // For SIMD indexOf
  const bytes = new Uint8Array(arrayBuffer); // For byte-level parsing
  const len = buffer.length;

  const COMMA = opts.delimiter;
  const LF = 10;
  const CR = 13;
  const decoder = new TextDecoder('utf-8');

  // SIMD-accelerated line finding using Buffer.indexOf
  const lineStarts: number[] = [0];
  let pos = 0;
  while (pos < len) {
    const idx = buffer.indexOf(LF, pos);
    if (idx === -1) break;
    lineStarts.push(idx + 1);
    pos = idx + 1;
  }

  // Get headers
  const firstLineEnd = lineStarts[1]! - 1;
  const headerEnd = bytes[firstLineEnd - 1] === CR ? firstLineEnd - 1 : firstLineEnd;
  const headerLine = decoder.decode(bytes.subarray(0, headerEnd));
  const delimiter = String.fromCharCode(COMMA);
  const headers = opts.hasHeader
    ? headerLine.split(delimiter)
    : headerLine.split(delimiter).map((_, i) => `column_${i}`);

  const numCols = headers.length;
  const startLineIdx = opts.hasHeader ? 1 : 0;

  // Find actual data lines
  let numDataLines = lineStarts.length - 1;
  while (numDataLines > startLineIdx) {
    const start = lineStarts[numDataLines - 1]!;
    const end = lineStarts[numDataLines] ?? len;
    if (end - start > 1) break;
    numDataLines--;
  }

  const rowCount = Math.min(numDataLines - startLineIdx, opts.maxRows);
  if (rowCount === 0) {
    return {
      df: DataFrame.empty({} as S),
      hasErrors: false,
    };
  }

  // Sample for type inference
  const sampleSize = Math.min(opts.sampleRows, rowCount);
  const samples: string[][] = [];
  for (let i = 0; i < sampleSize; i++) {
    const lineIdx = startLineIdx + i;
    const start = lineStarts[lineIdx]!;
    let end = lineStarts[lineIdx + 1]! - 1;
    if (bytes[end - 1] === CR) end--;
    samples.push(decoder.decode(bytes.subarray(start, end)).split(delimiter));
  }

  // Infer schema
  const schema: Schema = providedSchema ?? inferSchemaFast(headers, samples);

  // Pre-allocate storage
  const storage: (Float64Array | Int32Array | Uint8Array | string[])[] = [];
  const colTypes: DTypeKind[] = [];
  
  // Error tracking
  const parseErrors = new Map<string, ParseFailures>();

  for (let col = 0; col < numCols; col++) {
    const dtype = schema[headers[col]!];
    colTypes[col] = dtype?.kind ?? 'string';

    switch (dtype?.kind) {
      case 'float64':
        storage[col] = new Float64Array(rowCount);
        break;
      case 'int32':
        storage[col] = new Int32Array(rowCount);
        break;
      case 'bool':
        storage[col] = new Uint8Array(rowCount);
        break;
      default:
        storage[col] = new Array<string>(rowCount);
        break;
    }
    
    if (trackErrors) {
      parseErrors.set(headers[col]!, createParseFailures(rowCount));
    }
  }

  // Parse rows directly into columns - optimized byte-level parsing
  for (let rowIdx = 0; rowIdx < rowCount; rowIdx++) {
    const lineIdx = startLineIdx + rowIdx;
    const lineStart = lineStarts[lineIdx]!;
    let lineEnd = (lineStarts[lineIdx + 1] ?? len) - 1;
    if (bytes[lineEnd - 1] === CR) lineEnd--;

    let fieldStart = lineStart;

    for (let col = 0; col < numCols; col++) {
      // Find field end (handle quoted fields)
      let fieldEnd = fieldStart;
      if (bytes[fieldStart] === 34) { // Quote character
        fieldStart++; // Skip opening quote
        fieldEnd = fieldStart;
        while (fieldEnd < lineEnd && bytes[fieldEnd] !== 34) fieldEnd++;
        const valueEnd = fieldEnd;
        fieldEnd++; // Skip closing quote
        if (fieldEnd < lineEnd && bytes[fieldEnd] === COMMA) fieldEnd++; // Skip comma
        
        // For quoted fields, decode and store
        const dtype = colTypes[col]!;
        const store = storage[col]!;
        const fieldStr = decoder.decode(bytes.subarray(fieldStart, valueEnd));
        
        switch (dtype) {
          case 'float64':
            (store as Float64Array)[rowIdx] = parseFloat(fieldStr) || 0;
            break;
          case 'int32':
            (store as Int32Array)[rowIdx] = parseInt(fieldStr, 10) || 0;
            break;
          case 'bool':
            (store as Uint8Array)[rowIdx] = fieldStr === 'true' || fieldStr === '1' ? 1 : 0;
            break;
          default:
            (store as string[])[rowIdx] = fieldStr;
        }
        fieldStart = fieldEnd;
      } else {
        // Fast path: unquoted field
        while (fieldEnd < lineEnd && bytes[fieldEnd] !== COMMA) fieldEnd++;

        const dtype = colTypes[col]!;
        const store = storage[col]!;

        switch (dtype) {
          case 'float64':
            (store as Float64Array)[rowIdx] = parseFloatFast(bytes, fieldStart, fieldEnd);
            break;
          case 'int32':
            (store as Int32Array)[rowIdx] = parseIntFast(bytes, fieldStart, fieldEnd);
            break;
          case 'bool':
            (store as Uint8Array)[rowIdx] =
              bytes[fieldStart] === 116 || bytes[fieldStart] === 84 || bytes[fieldStart] === 49
                ? 1
                : 0;
            break;
          default:
            (store as string[])[rowIdx] = decoder.decode(bytes.subarray(fieldStart, fieldEnd));
        }

        fieldStart = fieldEnd + 1;
      }
    }
  }

  // Build Series
  const columns = new Map<keyof S, Series<DTypeKind>>();
  for (let col = 0; col < numCols; col++) {
    const header = headers[col]! as keyof S;
    const dtype = colTypes[col]!;
    const store = storage[col]!;

    switch (dtype) {
      case 'float64':
        columns.set(header, Series.float64(store as Float64Array));
        break;
      case 'int32':
        columns.set(header, Series.int32(store as Int32Array));
        break;
      case 'bool':
        columns.set(
          header,
          Series._fromStorage({ kind: 'bool', nullable: false }, store as Uint8Array),
        );
        break;
      default:
        columns.set(header, Series.string(store as string[]));
        break;
    }
  }

  const df = DataFrame._fromColumns(schema as S, columns, headers as (keyof S)[], rowCount);

  // Filter out columns with no errors
  const filteredErrors = new Map<keyof S, ParseFailures>();
  let hasErrors = false;

  if (trackErrors) {
    for (const [header, tracker] of parseErrors) {
      if (tracker.failureCount > 0) {
        filteredErrors.set(header as keyof S, tracker);
        hasErrors = true;
      }
    }
  }

  return {
    df,
    parseErrors: hasErrors ? filteredErrors : undefined,
    hasErrors,
  };
}

// Optimized numeric parsers operating directly on bytes
function parseFloatFast(bytes: Uint8Array, start: number, end: number): number {
  if (start >= end) return 0;

  let i = start;
  let negative = false;
  if (bytes[i] === 45) { // '-'
    negative = true;
    i++;
  } else if (bytes[i] === 43) { // '+'
    i++;
  }

  let intPart = 0;
  while (i < end && bytes[i]! >= 48 && bytes[i]! <= 57) {
    intPart = intPart * 10 + (bytes[i]! - 48);
    i++;
  }

  let result = intPart;
  if (i < end && bytes[i] === 46) { // '.'
    i++;
    let fracPart = 0;
    let fracDigits = 0;
    while (i < end && bytes[i]! >= 48 && bytes[i]! <= 57) {
      fracPart = fracPart * 10 + (bytes[i]! - 48);
      fracDigits++;
      i++;
    }
    if (fracDigits > 0) {
      result += fracPart / 10 ** fracDigits;
    }
  }

  return negative ? -result : result;
}

function parseIntFast(bytes: Uint8Array, start: number, end: number): number {
  if (start >= end) return 0;

  let i = start;
  let negative = false;
  if (bytes[i] === 45) {
    negative = true;
    i++;
  } else if (bytes[i] === 43) {
    i++;
  }

  let result = 0;
  while (i < end && bytes[i]! >= 48 && bytes[i]! <= 57) {
    result = result * 10 + (bytes[i]! - 48);
    i++;
  }

  return negative ? -result : result;
}

function inferSchemaFast(headers: string[], samples: string[][]): Schema {
  const schema: Schema = {};
  for (let col = 0; col < headers.length; col++) {
    const columnSamples: string[] = [];
    for (const row of samples) {
      if (row[col] !== undefined) columnSamples.push(row[col]!);
    }
    schema[headers[col]!] = inferColumnType(columnSamples);
  }
  return schema;
}
