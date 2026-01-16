import type { DataFrame } from '../../core/dataframe';
import type { Schema } from '../../core/types';

/**
 * Options for CSV writing.
 */
export interface CsvWriteOptions {
  /** Column delimiter (default: ',') */
  delimiter?: string;
  /** Whether to include header row (default: true) */
  includeHeader?: boolean;
  /** Line ending (default: '\n') */
  lineEnding?: string;
}

const DEFAULT_WRITE_OPTIONS: Required<CsvWriteOptions> = {
  delimiter: ',',
  includeHeader: true,
  lineEnding: '\n',
};

/**
 * Convert DataFrame to CSV string.
 *
 * @example
 * ```ts
 * const df = DataFrame.fromColumns({ a: [1, 2], b: ['x', 'y'] });
 * const csv = toCsv(df);
 * // "a,b\n1,x\n2,y\n"
 * ```
 */
export function toCsv<S extends Schema>(df: DataFrame<S>, options?: CsvWriteOptions): string {
  const opts = { ...DEFAULT_WRITE_OPTIONS, ...options };
  const { delimiter, includeHeader, lineEnding } = opts;

  const columns = df.columns();
  const lines: string[] = [];

  // Header row
  if (includeHeader) {
    lines.push(columns.map((col) => escapeField(String(col), delimiter)).join(delimiter));
  }

  // Data rows
  for (let rowIdx = 0; rowIdx < df.shape[0]; rowIdx++) {
    const row: string[] = [];
    for (const colName of columns) {
      const series = df._columns.get(colName)!;
      const value = series.at(rowIdx);
      row.push(formatValue(value, delimiter));
    }
    lines.push(row.join(delimiter));
  }

  return lines.join(lineEnding) + lineEnding;
}

/**
 * Write DataFrame to CSV file.
 *
 * @example
 * ```ts
 * await writeCsv(df, './output.csv');
 * ```
 */
export async function writeCsv<S extends Schema>(
  df: DataFrame<S>,
  path: string,
  options?: CsvWriteOptions,
): Promise<void> {
  const csv = toCsv(df, options);
  await Bun.write(path, csv);
}

/**
 * Escape a field value for CSV output.
 * Quotes the field if it contains delimiter, quote, or newline.
 */
function escapeField(value: string, delimiter: string): string {
  if (value.includes(delimiter) || value.includes('"') || value.includes('\n')) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

/**
 * Format a value for CSV output.
 */
function formatValue(value: unknown, delimiter: string): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'number') {
    if (Number.isNaN(value)) return '';
    return String(value);
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  return escapeField(String(value), delimiter);
}
