/**
 * CSV parsing options.
 */
export interface CsvOptions {
  /** Column delimiter byte (default: 44 for comma) */
  delimiter?: number;

  /** Quote character byte (default: 34 for double quote) */
  quote?: number;

  /** Whether first row is header (default: true) */
  hasHeader?: boolean;

  /** Whether to auto-detect column types (default: true) */
  inferTypes?: boolean;

  /** Number of rows to sample for type inference (default: 100) */
  sampleRows?: number;

  /** Maximum rows to read (default: Infinity) */
  maxRows?: number;

  /** Whether to track parse errors (default: true) */
  trackErrors?: boolean;
}

/** Default CSV options */
export const DEFAULT_CSV_OPTIONS: Required<CsvOptions> = {
  delimiter: 44, // comma
  quote: 34, // double quote
  hasHeader: true,
  inferTypes: true,
  sampleRows: 100,
  maxRows: Number.POSITIVE_INFINITY,
  trackErrors: true,
};

/**
 * Byte constants for parsing.
 */
export const BYTES = {
  COMMA: 44,
  QUOTE: 34,
  CR: 13,
  LF: 10,
  TAB: 9,
} as const;
