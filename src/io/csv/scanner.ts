import { LazyFrame, type LazyFrameConfig } from '../../core/lazyframe';
import type { Schema } from '../../core/types';
import { inferColumnType } from './inference';
import { type CsvOptions, resolveOptions } from './options';

/**
 * Scan a CSV file for lazy loading.
 *
 * Unlike readCsv which loads everything into memory, scanCsv creates
 * a LazyFrame that loads data on-demand. Ideal for large files that
 * exceed available RAM.
 *
 * @param path - Path to CSV file
 * @param options - CSV parsing options
 * @returns LazyFrame instance
 *
 * @example
 * ```ts
 * // For large files (10GB+)
 * const lazy = await scanCsv('./huge_dataset.csv');
 *
 * // Only loads first 10 rows
 * const first10 = await lazy.head(10);
 * first10.print();
 *
 * // Stream filter - processes in chunks
 * const filtered = await lazy.filter(row => row.price > 100);
 *
 * // Convert to full DataFrame (loads everything)
 * const df = await lazy.collect();
 * ```
 */
export async function scanCsv<S extends Schema = Schema>(
  path: string,
  options?: CsvOptions & { schema?: S; lazyConfig?: LazyFrameConfig },
): Promise<LazyFrame<S>> {
  const opts = resolveOptions(options);
  const providedSchema = options?.schema;
  const lazyConfig = options?.lazyConfig ?? {};

  // Read file for initial scanning
  const file = Bun.file(path);
  const arrayBuffer = await file.arrayBuffer();
  const buffer = Buffer.from(arrayBuffer);
  const bytes = new Uint8Array(arrayBuffer);
  const len = buffer.length;

  const COMMA = opts.delimiter;
  const LF = 10;
  const CR = 13;
  const decoder = new TextDecoder('utf-8');
  const delimiter = String.fromCharCode(COMMA);

  // Find first line for headers
  let firstLineEnd = buffer.indexOf(LF);
  if (firstLineEnd === -1) firstLineEnd = len;
  const headerEnd = bytes[firstLineEnd - 1] === CR ? firstLineEnd - 1 : firstLineEnd;
  const headerLine = decoder.decode(bytes.subarray(0, headerEnd));

  const headers = opts.hasHeader
    ? parseLineWithQuotes(headerLine, delimiter)
    : parseLineWithQuotes(headerLine, delimiter).map((_, i) => `column_${i}`);

  // Sample rows for type inference if schema not provided
  let schema: Schema;

  if (providedSchema) {
    schema = providedSchema;
  } else {
    // Sample first N rows for inference
    const samples: string[][] = [];
    const sampleSize = Math.min(opts.sampleRows, 1000);

    let pos = opts.hasHeader ? firstLineEnd + 1 : 0;
    let sampledCount = 0;

    while (pos < len && sampledCount < sampleSize) {
      const lineEnd = buffer.indexOf(LF, pos);
      const end = lineEnd === -1 ? len : lineEnd;
      let lineEndClean = end;
      if (bytes[lineEndClean - 1] === CR) lineEndClean--;

      if (lineEndClean > pos) {
        const line = decoder.decode(bytes.subarray(pos, lineEndClean));
        samples.push(parseLineWithQuotes(line, delimiter));
        sampledCount++;
      }

      pos = end + 1;
    }

    schema = inferSchemaFromSamples(headers, samples);
  }

  // Create LazyFrame
  return LazyFrame._create(
    path,
    schema as S,
    headers as (keyof S)[],
    lazyConfig,
    opts.hasHeader,
    delimiter,
  );
}

/**
 * Parse a CSV line handling quoted fields.
 */
function parseLineWithQuotes(line: string, delimiter: string): string[] {
  const fields: string[] = [];
  let current = '';
  let inQuotes = false;
  let i = 0;

  while (i < line.length) {
    const char = line[i]!;

    if (inQuotes) {
      if (char === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i += 2;
        } else {
          inQuotes = false;
          i++;
        }
      } else {
        current += char;
        i++;
      }
    } else {
      if (char === '"') {
        inQuotes = true;
        i++;
      } else if (char === delimiter) {
        fields.push(current);
        current = '';
        i++;
      } else {
        current += char;
        i++;
      }
    }
  }

  fields.push(current);
  return fields;
}

/**
 * Infer schema from sample rows.
 */
function inferSchemaFromSamples(headers: string[], samples: string[][]): Schema {
  const schema: Schema = {};

  for (let col = 0; col < headers.length; col++) {
    const columnSamples: string[] = [];
    for (const row of samples) {
      if (row[col] !== undefined) {
        columnSamples.push(row[col]!);
      }
    }
    schema[headers[col]!] = inferColumnType(columnSamples);
  }

  return schema;
}
