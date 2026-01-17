import { BYTES } from './options';

/**
 * Unified CSV parser that automatically selects the optimal algorithm.
 *
 * Strategies:
 * - No quotes detected: Uses SIMD line-finding + split (fast for clean data)
 * - Quotes detected: Uses hybrid line-by-line parsing (handles RFC 4180)
 *
 * @example
 * ```typescript
 * const parser = new CsvParser();
 * const rows = parser.parse(buffer);
 * ```
 */
export class CsvParser {
  private readonly delimiter: number;
  private readonly quote: number;
  private readonly decoder = new TextDecoder('utf-8');

  constructor(delimiter: number = BYTES.COMMA, quote: number = BYTES.QUOTE) {
    this.delimiter = delimiter;
    this.quote = quote;
  }

  /**
   * Parse CSV buffer into rows.
   * Automatically detects whether to use fast or hybrid parsing.
   */
  parse(buffer: Buffer): string[][] {
    const hasQuotes = buffer.indexOf(this.quote) !== -1;
    return hasQuotes ? this.parseHybrid(buffer) : this.parseFast(buffer);
  }

  /**
   * Parse with header extraction.
   */
  parseWithHeader(buffer: Buffer, hasHeader: boolean): { headers: string[]; rows: string[][] } {
    const allRows = this.parse(buffer);

    if (allRows.length === 0) {
      return { headers: [], rows: [] };
    }

    if (hasHeader) {
      const headers = allRows[0]!;
      const rows = allRows.slice(1);
      return { headers, rows };
    }

    const headers = allRows[0]!.map((_, i) => `column_${i}`);
    return { headers, rows: allRows };
  }

  /**
   * Fast path for CSVs without quotes.
   * Uses SIMD-accelerated Buffer.indexOf + String.split.
   */
  private parseFast(buffer: Buffer): string[][] {
    const rows: string[][] = [];
    const len = buffer.length;
    const delimiterChar = String.fromCharCode(this.delimiter);

    let lineStart = 0;

    while (lineStart < len) {
      let lineEnd = buffer.indexOf(BYTES.LF, lineStart);
      if (lineEnd === -1) lineEnd = len;

      let lineEndClean = lineEnd;
      if (lineEndClean > lineStart && buffer[lineEndClean - 1] === BYTES.CR) {
        lineEndClean--;
      }

      if (lineEndClean > lineStart) {
        const line = this.decoder.decode(buffer.subarray(lineStart, lineEndClean));
        rows.push(line.split(delimiterChar));
      }

      lineStart = lineEnd + 1;
    }

    return rows;
  }

  /**
   * Hybrid path for CSVs with quotes.
   * Uses SIMD for line-finding, then per-line quote detection.
   */
  private parseHybrid(buffer: Buffer): string[][] {
    const rows: string[][] = [];
    const len = buffer.length;
    const delimiterChar = String.fromCharCode(this.delimiter);

    let lineStart = 0;

    while (lineStart < len) {
      let lineEnd = buffer.indexOf(BYTES.LF, lineStart);
      if (lineEnd === -1) lineEnd = len;

      // Check if this line contains quotes
      const quotePos = buffer.indexOf(this.quote, lineStart);
      const hasQuote = quotePos !== -1 && quotePos < lineEnd;

      if (hasQuote) {
        const result = this.parseQuotedLine(buffer, lineStart, len);
        if (result.row.length > 0) {
          rows.push(result.row);
        }
        lineStart = result.nextStart;
      } else {
        // Fast path for this line
        let lineEndClean = lineEnd;
        if (lineEndClean > lineStart && buffer[lineEndClean - 1] === BYTES.CR) {
          lineEndClean--;
        }

        if (lineEndClean > lineStart) {
          const line = this.decoder.decode(buffer.subarray(lineStart, lineEndClean));
          rows.push(line.split(delimiterChar));
        }

        lineStart = lineEnd + 1;
      }
    }

    return rows;
  }

  /**
   * Parse a line that contains quotes (may span multiple lines).
   */
  private parseQuotedLine(
    buffer: Buffer,
    start: number,
    bufferLen: number,
  ): { row: string[]; nextStart: number } {
    const fields: string[] = [];
    let pos = start;
    let inQuotes = false;
    let fieldBuffer = '';

    while (pos < bufferLen) {
      const byte = buffer[pos]!;

      if (inQuotes) {
        if (byte === this.quote) {
          if (pos + 1 < bufferLen && buffer[pos + 1] === this.quote) {
            fieldBuffer += '"';
            pos += 2;
          } else {
            inQuotes = false;
            pos++;
          }
        } else {
          fieldBuffer += String.fromCharCode(byte);
          pos++;
        }
      } else {
        if (byte === this.quote) {
          inQuotes = true;
          pos++;
        } else if (byte === this.delimiter) {
          fields.push(fieldBuffer);
          fieldBuffer = '';
          pos++;
        } else if (byte === BYTES.LF) {
          fields.push(fieldBuffer);
          return { row: fields, nextStart: pos + 1 };
        } else if (byte === BYTES.CR) {
          pos++;
        } else {
          fieldBuffer += String.fromCharCode(byte);
          pos++;
        }
      }
    }

    if (fieldBuffer.length > 0 || fields.length > 0) {
      fields.push(fieldBuffer);
    }

    return { row: fields, nextStart: bufferLen };
  }
}

/**
 * Check if buffer contains any quote characters.
 * Uses Buffer.indexOf for SIMD-accelerated search.
 */
export function hasQuotedFields(buffer: Buffer, quote: number = BYTES.QUOTE): boolean {
  return buffer.indexOf(quote) !== -1;
}
