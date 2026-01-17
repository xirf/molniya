/**
 * Node.js-compatible CSV reader not optimized.
 * Uses Node's fs module instead of Bun APIs.
 */

import * as fs from 'node:fs';
import { DataFrame } from '../../core/dataframe';
import { Series } from '../../core/series';
import type { DType, DTypeKind, Schema } from '../../core/types';
import { inferColumnType, parseValue } from './inference';
import { type CsvOptions, DEFAULT_CSV_OPTIONS } from './options';
import {
  type CsvReadResult,
  type ParseFailures,
  createParseFailures,
  recordFailure,
} from './parse-result';

/**
 * Reads a CSV file into a DataFrame using Node.js fs module.
 * Compatible with both Node.js and Bun runtimes.
 *
 * Returns a CsvReadResult with the DataFrame and any parse errors.
 */
export async function readCsvNode<S extends Schema = Schema>(
  path: string,
  options?: CsvOptions & { schema?: S },
): Promise<CsvReadResult<S>> {
  const opts = { ...DEFAULT_CSV_OPTIONS, ...options };
  const providedSchema = options?.schema;
  const trackErrors = opts.trackErrors ?? true;

  // Read file using Node.js fs
  const content = fs.readFileSync(path, 'utf-8');
  const lines = content.split('\n').filter((line) => line.trim());

  const delimiter = opts.delimiter;

  // Parse header
  let headers: string[];
  let startRow: number;

  if (opts.hasHeader) {
    headers = lines[0]!.split(delimiter).map((h) => h.trim());
    startRow = 1;
  } else {
    const firstRow = lines[0]!.split(delimiter);
    headers = firstRow.map((_, i) => `column_${i}`);
    startRow = 0;
  }

  const numCols = headers.length;

  // Sample for type inference
  const sampleEnd = Math.min(startRow + (opts.sampleRows ?? 100), lines.length);
  const sampleLines = lines.slice(startRow, sampleEnd);
  const samples: string[][] = sampleLines.map((line) => line.split(delimiter));

  // Infer schema
  const schema: Schema = providedSchema ?? inferSchemaFromSamples(headers, samples);

  // Parse data
  const maxRows = opts.maxRows ?? Number.POSITIVE_INFINITY;
  const endRow = Math.min(startRow + maxRows, lines.length);
  const dataLines = lines.slice(startRow, endRow);
  const rowCount = dataLines.length;

  // Pre-allocate storage
  const storage: Map<string, number[] | string[] | (boolean | null)[]> = new Map();

  // Error tracking
  const parseErrors = new Map<string, ParseFailures>();

  for (const header of headers) {
    const dtype = schema[header];
    if (!dtype) continue;

    storage.set(header, new Array(rowCount));

    if (trackErrors) {
      parseErrors.set(header, createParseFailures(rowCount));
    }
  }

  // Parse rows
  for (let rowIdx = 0; rowIdx < rowCount; rowIdx++) {
    const fields = dataLines[rowIdx]!.split(delimiter);

    for (let colIdx = 0; colIdx < numCols; colIdx++) {
      const header = headers[colIdx]!;
      const dtype = schema[header];
      if (!dtype) continue;

      const rawValue = fields[colIdx]?.trim() ?? '';
      const store = storage.get(header)!;
      const result = parseValue(rawValue, dtype);

      store[rowIdx] = result.value as (typeof store)[number];

      // Track parse failures
      if (trackErrors && !result.success && result.original !== undefined) {
        const tracker = parseErrors.get(header)!;
        recordFailure(tracker, rowIdx, result.original);
      }
    }
  }

  // Build Series
  const columns = new Map<keyof S, Series<DTypeKind>>();

  for (const header of headers) {
    const dtype = schema[header];
    if (!dtype) continue;

    const store = storage.get(header)!;

    switch (dtype.kind) {
      case 'float64':
        columns.set(header as keyof S, Series.float64(new Float64Array(store as number[])));
        break;
      case 'int32':
        columns.set(header as keyof S, Series.int32(new Int32Array(store as number[])));
        break;
      case 'bool':
        columns.set(header as keyof S, Series.bool(store as boolean[]));
        break;
      default:
        columns.set(header as keyof S, Series.string(store as string[]));
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

function inferSchemaFromSamples(headers: string[], samples: string[][]): Schema {
  const schema: Schema = {};

  for (let col = 0; col < headers.length; col++) {
    const columnSamples: string[] = [];
    for (const row of samples) {
      if (row[col] !== undefined) {
        columnSamples.push(row[col]!.trim());
      }
    }
    schema[headers[col]!] = inferColumnType(columnSamples);
  }

  return schema;
}
