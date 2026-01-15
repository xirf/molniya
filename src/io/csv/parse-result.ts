import type { DataFrame } from '../../core/dataframe';
import type { Schema } from '../../core/types';

/**
 * Tracks parse failures for a single column.
 */
export interface ParseFailures {
  /** Map of rowIndex → original string value that failed to parse */
  failures: Map<number, string>;
  /** Number of failed parses */
  failureCount: number;
  /** Total number of rows */
  totalRows: number;
  /** Success rate: (totalRows - failureCount) / totalRows */
  successRate: number;
}

/**
 * Result of reading a CSV file with error tracking.
 */
export interface CsvReadResult<S extends Schema> {
  /** The parsed DataFrame */
  df: DataFrame<S>;
  /** Parse errors by column name. Only present if errors occurred. */
  parseErrors?: Map<keyof S, ParseFailures>;
  /** Whether any parse errors occurred */
  hasErrors: boolean;
}

/**
 * Creates a new ParseFailures tracker.
 */
export function createParseFailures(totalRows: number): ParseFailures {
  return {
    failures: new Map(),
    failureCount: 0,
    totalRows,
    successRate: 1.0,
  };
}

/**
 * Records a parse failure.
 */
export function recordFailure(
  tracker: ParseFailures,
  rowIndex: number,
  originalValue: string,
): void {
  tracker.failures.set(rowIndex, originalValue);
  tracker.failureCount++;
  tracker.successRate = (tracker.totalRows - tracker.failureCount) / tracker.totalRows;
}
