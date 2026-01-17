import type { DType, DTypeKind } from '../../core/types';
import { m } from '../../core/types';

/**
 * Result of parsing a single value.
 */
export interface ParseResult {
  /** The parsed value */
  value: unknown;
  /** Whether parsing succeeded */
  success: boolean;
  /** Original string if parsing failed */
  original?: string;
}

/**
 * Infers the most appropriate dtype for a column of string values.
 * Samples values to determine if they're numeric, boolean, or string.
 *
 * NOTE: Always uses float64 for numeric columns to avoid truncation.
 * int32 can be explicitly specified via schema if needed.
 */
export function inferColumnType(samples: string[]): DType<DTypeKind> {
  if (samples.length === 0) {
    return m.string();
  }

  let allNumeric = true;
  let allIntegers = true;
  let allBools = true;
  let hasNonEmpty = false;

  for (const value of samples) {
    const trimmed = value.trim();

    // Skip empty values in inference
    if (trimmed === '' || trimmed.toLowerCase() === 'null' || trimmed.toLowerCase() === 'na') {
      continue;
    }

    hasNonEmpty = true;

    // Check boolean
    const lower = trimmed.toLowerCase();
    if (lower !== 'true' && lower !== 'false' && lower !== '0' && lower !== '1') {
      allBools = false;
    }

    // Check numeric
    const num = Number(trimmed);
    if (Number.isNaN(num)) {
      allNumeric = false;
    } else if (!Number.isInteger(num)) {
      allIntegers = false;
    }
  }

  // If all values were empty/null, default to string
  if (!hasNonEmpty) {
    return m.string();
  }

  if (allBools) {
    return m.bool();
  }

  // Always use float64 for numeric columns to avoid data loss
  // Users can explicitly specify int32 via schema if needed
  if (allNumeric) {
    return m.float64();
  }

  return m.string();
}

/**
 * Parses a string value according to its dtype.
 * Returns a ParseResult with success status and original value on failure.
 */
export function parseValue(value: string, dtype: DType<DTypeKind>): ParseResult {
  const trimmed = value.trim();

  // Handle null/empty - these are not failures, just missing values
  if (trimmed === '' || trimmed.toLowerCase() === 'null' || trimmed.toLowerCase() === 'na') {
    switch (dtype.kind) {
      case 'float64':
        return { value: Number.NaN, success: true };
      case 'int32':
        return { value: Number.NaN, success: true }; // Use NaN for missing in int32 too
      case 'bool':
        return { value: null, success: true }; // null for missing bool
      default:
        return { value: '', success: true };
    }
  }

  switch (dtype.kind) {
    case 'float64':
    case 'int32': {
      const num = Number(trimmed);
      if (Number.isNaN(num)) {
        // Parse failure - not a valid number
        return { value: Number.NaN, success: false, original: trimmed };
      }
      if (dtype.kind === 'int32') {
        return { value: Math.trunc(num), success: true };
      }
      return { value: num, success: true };
    }
    case 'bool': {
      const lower = trimmed.toLowerCase();
      if (lower === 'true' || lower === '1') {
        return { value: true, success: true };
      }
      if (lower === 'false' || lower === '0') {
        return { value: false, success: true };
      }
      // Not a valid boolean
      return { value: false, success: false, original: trimmed };
    }
    default:
      // String type - always succeeds
      return { value: trimmed, success: true };
  }
}

/**
 * Parses a value, returning just the value (legacy compatibility).
 * Use parseValue() for full error tracking.
 */
export function parseValueSimple(value: string, dtype: DType<DTypeKind>): unknown {
  return parseValue(value, dtype).value;
}
