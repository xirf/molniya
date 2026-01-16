import { DataFrame } from '../../core/dataframe';
import { Series } from '../../core/series';
import type { DTypeKind, Schema } from '../../core/types';
import { inferColumnType } from './inference';
import { BYTES, type CsvOptions, DEFAULT_CSV_OPTIONS } from './options';
import { type CsvReadResult, type ParseFailures, createParseFailures } from './parse-result';
import { CsvChunkParser } from './parser';

/**
 * CSV reader using CsvChunkParser for RFC 4180 compliance.
 *
 * Correctly handles:
 * - Quoted fields containing commas
 * - Escaped quotes ("")
 * - Newlines within quoted fields
 *
 * Performance profile (387MB, 7.38M rows):
 * - Target: ~2s on Bun
 * - Uses streaming chunk parser for correctness
 */
export async function readCsv<S extends Schema = Schema>(
  path: string,
  options?: CsvOptions & { schema?: S },
): Promise<CsvReadResult<S>> {
  const opts = { ...DEFAULT_CSV_OPTIONS, ...options };
  const providedSchema = options?.schema;
  const trackErrors = opts.trackErrors ?? true;

  // Read file as bytes
  const file = Bun.file(path);
  const arrayBuffer = await file.arrayBuffer();
  const bytes = new Uint8Array(arrayBuffer);

  // Use chunk parser for RFC 4180 compliance (handles quoted newlines)
  const parser = new CsvChunkParser(opts.delimiter, opts.quote);
  parser.processChunk(bytes);
  parser.finish();

  const allRows = parser.consumeRows();
  if (allRows.length === 0) {
    return {
      df: DataFrame.empty({} as S),
      hasErrors: false,
    };
  }

  // Extract headers
  const headers = opts.hasHeader ? allRows[0]! : allRows[0]!.map((_, i) => `column_${i}`);
  const numCols = headers.length;
  const startRowIdx = opts.hasHeader ? 1 : 0;

  // Apply maxRows limit
  const dataRows = allRows.slice(startRowIdx, startRowIdx + opts.maxRows);
  const rowCount = dataRows.length;

  if (rowCount === 0) {
    return {
      df: DataFrame.empty({} as S),
      hasErrors: false,
    };
  }

  // Check abort signal
  if (opts.signal?.aborted) {
    throw new DOMException('Operation was aborted', 'AbortError');
  }

  // Sample for type inference
  const sampleSize = Math.min(opts.sampleRows, rowCount);
  const samples = dataRows.slice(0, sampleSize);

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

  // Parse rows into typed columns
  for (let rowIdx = 0; rowIdx < rowCount; rowIdx++) {
    // Periodically check abort signal
    if (opts.signal?.aborted && rowIdx % 10000 === 0) {
      throw new DOMException('Operation was aborted', 'AbortError');
    }

    const row = dataRows[rowIdx]!;

    for (let col = 0; col < numCols; col++) {
      const fieldStr = row[col] ?? '';
      const dtype = colTypes[col]!;
      const store = storage[col]!;

      switch (dtype) {
        case 'float64': {
          const val = Number.parseFloat(fieldStr);
          (store as Float64Array)[rowIdx] = Number.isNaN(val) ? 0 : val;
          break;
        }
        case 'int32': {
          const val = Number.parseInt(fieldStr, 10);
          (store as Int32Array)[rowIdx] = Number.isNaN(val) ? 0 : val;
          break;
        }
        case 'bool':
          (store as Uint8Array)[rowIdx] = fieldStr === 'true' || fieldStr === '1' ? 1 : 0;
          break;
        default:
          (store as string[])[rowIdx] = fieldStr;
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
