import type { Series } from '../series';
import type { DType, DTypeKind, Schema } from '../types';

/**
 * Internal DataFrame context for display methods.
 * Provides access to DataFrame internals without circular imports.
 */
export interface DisplayContext<S extends Schema> {
  readonly schema: S;
  readonly shape: readonly [rows: number, cols: number];
  _columns: Map<keyof S, Series<DTypeKind>>;
  _columnOrder: (keyof S)[];
}

/**
 * Formats a value for display.
 */
export function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'null';
  }
  if (typeof value === 'number') {
    if (Number.isNaN(value)) return 'NaN';
    return Number.isInteger(value) ? String(value) : value.toFixed(4);
  }
  return String(value);
}

/**
 * Centers text within a given width.
 */
export function padCenter(str: string, width: number): string {
  const totalPad = width - str.length;
  const leftPad = Math.floor(totalPad / 2);
  const rightPad = totalPad - leftPad;
  return ' '.repeat(leftPad) + str + ' '.repeat(rightPad);
}

/**
 * Formats a single row as a table row string.
 */
export function formatRow<S extends Schema>(
  ctx: DisplayContext<S>,
  rowIndex: number,
  widths: number[],
): string {
  const cells: string[] = [];
  for (let j = 0; j < ctx._columnOrder.length; j++) {
    const series = ctx._columns.get(ctx._columnOrder[j]!)!;
    const value = formatValue(series.at(rowIndex));
    const truncated = value.length > widths[j]! ? `${value.slice(0, widths[j]! - 1)}…` : value;
    cells.push(` ${truncated.padStart(widths[j]!, ' ')} `);
  }
  return `│${cells.join('│')}│`;
}

/**
 * Formats a DataFrame as an ASCII table string.
 */
export function formatDataFrame<S extends Schema>(ctx: DisplayContext<S>): string {
  const maxRows = 10;
  const maxColWidth = 20;

  // Calculate column widths
  const headers = ctx._columnOrder.map(String);
  const widths = headers.map((h) => Math.min(h.length, maxColWidth));

  // Sample data for width calculation
  const sampleRows = Math.min(ctx.shape[0], maxRows);
  for (let i = 0; i < sampleRows; i++) {
    for (let j = 0; j < headers.length; j++) {
      const series = ctx._columns.get(ctx._columnOrder[j]!)!;
      const value = formatValue(series.at(i));
      widths[j] = Math.min(maxColWidth, Math.max(widths[j]!, value.length));
    }
  }

  // Build table
  const lines: string[] = [];

  // Top border
  lines.push(`┌${widths.map((w) => '─'.repeat(w! + 2)).join('┬')}┐`);

  // Header
  lines.push(`│${headers.map((h, i) => ` ${padCenter(h, widths[i]!)} `).join('│')}│`);

  // Header separator
  lines.push(`├${widths.map((w) => '─'.repeat(w! + 2)).join('┼')}┤`);

  // Rows
  if (ctx.shape[0] <= maxRows) {
    for (let i = 0; i < ctx.shape[0]; i++) {
      lines.push(formatRow(ctx, i, widths));
    }
  } else {
    // First 5
    for (let i = 0; i < 5; i++) {
      lines.push(formatRow(ctx, i, widths));
    }
    // Ellipsis row
    lines.push(`│${widths.map((w) => ` ${'...'.padStart(w!, ' ')} `).join('│')}│`);
    // Last 5
    for (let i = ctx.shape[0] - 5; i < ctx.shape[0]; i++) {
      lines.push(formatRow(ctx, i, widths));
    }
  }

  // Bottom border
  lines.push(`└${widths.map((w) => '─'.repeat(w! + 2)).join('┴')}┘`);

  // Shape info
  lines.push(`[${ctx.shape[0]} rows × ${ctx.shape[1]} columns]`);

  return lines.join('\n');
}
