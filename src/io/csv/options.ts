/**
 * CSV parsing options.
 */
export interface CsvOptions {
  /** Column delimiter (default: ",") */
  delimiter?: string;

  /** Quote character (default: '"') */
  quote?: string;

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

  /** AbortSignal to cancel long-running reads */
  signal?: AbortSignal;
}

/** Default CSV options */
export const DEFAULT_CSV_OPTIONS = {
  delimiter: ',',
  quote: '"',
  hasHeader: true,
  inferTypes: true,
  sampleRows: 100,
  maxRows: Number.POSITIVE_INFINITY,
  trackErrors: true,
  signal: undefined as AbortSignal | undefined,
} as const;

/** Resolved CSV options with byte codes for internal use */
export interface ResolvedCsvOptions {
  delimiter: number;
  quote: number;
  hasHeader: boolean;
  inferTypes: boolean;
  sampleRows: number;
  maxRows: number;
  trackErrors: boolean;
  signal: AbortSignal | undefined;
}

/** Convert user-facing options to internal byte-based options */
export function resolveOptions(options?: CsvOptions): ResolvedCsvOptions {
  const opts = { ...DEFAULT_CSV_OPTIONS, ...options };
  return {
    delimiter: opts.delimiter.charCodeAt(0),
    quote: opts.quote.charCodeAt(0),
    hasHeader: opts.hasHeader,
    inferTypes: opts.inferTypes,
    sampleRows: opts.sampleRows,
    maxRows: opts.maxRows,
    trackErrors: opts.trackErrors,
    signal: opts.signal,
  };
}

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
