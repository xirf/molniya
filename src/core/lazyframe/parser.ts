import type { DType, DTypeKind, Schema } from '../types';
import type { Vector } from './chunk-cache';

// ASCII Character Codes
const CODE_NEWLINE = 10;
const CODE_RETURN = 13;
const CODE_QUOTE = 34;
const CODE_DOT = 46;
const CODE_MINUS = 45;
const CODE_PLUS = 43;
const CODE_E_LOWER = 101;
const CODE_E_UPPER = 69;
const CODE_0 = 48;
const CODE_9 = 57;

/**
 * Parse a chunk of CSV text into columns (String-based).
 */
export function parseChunk(
  text: string,
  expectedRows: number,
  columnOrder: string[],
  schema: Schema,
  delimiter: string,
): Record<string, unknown[]> {
  const colOrderLen = columnOrder.length;
  const columns: Record<string, unknown[]> = {};
  for (const col of columnOrder) {
    columns[col] = new Array(expectedRows);
  }

  let pos = 0;
  const len = text.length;
  let rowIndex = 0;

  while (pos < len && rowIndex < expectedRows) {
    const nextNewline = text.indexOf('\n', pos);
    const lineEnd = nextNewline === -1 ? len : nextNewline;
    let lineEndClean = lineEnd;
    if (text[lineEndClean - 1] === '\r') lineEndClean--;

    const line = text.substring(pos, lineEndClean);
    const fields = parseLine(line, delimiter);

    for (let col = 0; col < colOrderLen; col++) {
      const colName = columnOrder[col]!;
      const dtype = schema[colName];
      const fieldValue = fields[col] ?? '';
      columns[colName]![rowIndex] = parseValue(fieldValue, dtype);
    }

    rowIndex++;
    pos = lineEnd + 1;
  }

  // Trim arrays if fewer rows found (e.g. end of file)
  if (rowIndex < expectedRows) {
    for (const col of columnOrder) {
      columns[col] = columns[col]!.slice(0, rowIndex);
    }
  }

  return columns;
}

export function parseLine(line: string, delimiter: string): string[] {
  const fields: string[] = [];
  const len = line.length;
  let i = 0;

  while (i < len) {
    if (line[i] === '"') {
      let field = '';
      i++;
      let start = i;
      while (i < len) {
        const nextQuote = line.indexOf('"', i);
        if (nextQuote === -1) {
          field += line.substring(start);
          i = len;
          break;
        }
        if (line[nextQuote + 1] === '"') {
          field += line.substring(start, nextQuote + 1);
          i = nextQuote + 2;
          start = i;
        } else {
          field += line.substring(start, nextQuote);
          i = nextQuote + 1;
          if (line[i] === delimiter) i++;
          break;
        }
      }
      fields.push(field);
    } else {
      const nextDelimiter = line.indexOf(delimiter, i);
      if (nextDelimiter === -1) {
        fields.push(line.substring(i));
        i = len;
      } else {
        fields.push(line.substring(i, nextDelimiter));
        i = nextDelimiter + 1;
      }
    }
  }
  if (line[len - 1] === delimiter) {
    fields.push('');
  }
  return fields;
}

export function parseValue(value: string, dtype: DType<DTypeKind> | undefined): unknown {
  if (!dtype) return value;
  switch (dtype.kind) {
    case 'float64':
      return Number.parseFloat(value) || 0;
    case 'int32':
      return Number.parseInt(value, 10) || 0;
    case 'bool':
      return value === 'true' || value === 'True' || value === '1';
    default:
      return value;
  }
}

/**
 * Parse a chunk of CSV bytes into columns (Byte-level optimization).
 * Returns Vector array (Struct of Arrays layout).
 */
export function parseChunkBytes(
  bytes: Uint8Array,
  expectedRows: number,
  columnOrder: string[],
  schema: Schema,
  delimiterCode: number,
  skipRows = 0,
): Vector[] {
  const colOrderLen = columnOrder.length;
  let pos = 0;
  const len = bytes.length;

  // Skip leading rows if requested
  if (skipRows > 0) {
    let skipped = 0;
    while (pos < len && skipped < skipRows) {
      const idx = bytes.indexOf(10, pos); // Find LF
      if (idx === -1) {
        pos = len; // Consumed all
        break;
      }
      pos = idx + 1;
      skipped++;
    }

    if (pos >= len) {
      // If we mapped to end, return empty vectors
      const emptyCols: Vector[] = new Array(colOrderLen);
      for (let i = 0; i < colOrderLen; i++) {
        const col = columnOrder[i]!;
        const kind = schema[col]!.kind;
        if (kind === 'string') {
          emptyCols[i] = {
            kind: 'string',
            data: new Uint8Array(0),
            offsets: new Uint32Array(0),
            lengths: new Uint32Array(0),
            needsUnescape: new Uint8Array(0),
          };
        } else if (kind === 'int32') {
          emptyCols[i] = { kind: 'int32', data: new Int32Array(0) };
        } else if (kind === 'bool') {
          emptyCols[i] = { kind: 'bool', data: new Uint8Array(0) };
        } else {
          emptyCols[i] = { kind: 'float64', data: new Float64Array(0) };
        }
      }
      return emptyCols;
    }
  }

  // Initialize Vectors
  const columns: Vector[] = new Array(colOrderLen);

  for (let i = 0; i < colOrderLen; i++) {
    const colName = columnOrder[i]!;
    const dtype = schema[colName];

    if (!dtype) {
      // Unknown type ? Default to string-like behavior (offsets)
      columns[i] = {
        kind: 'string',
        data: bytes,
        offsets: new Uint32Array(expectedRows),
        lengths: new Uint32Array(expectedRows),
        needsUnescape: new Uint8Array(expectedRows),
      };
      continue;
    }

    switch (dtype.kind) {
      case 'float64':
        columns[i] = { kind: 'float64', data: new Float64Array(expectedRows) };
        break;
      case 'int32':
        columns[i] = { kind: 'int32', data: new Int32Array(expectedRows) };
        break;
      case 'bool':
        columns[i] = { kind: 'bool', data: new Uint8Array(expectedRows) };
        break;
      default:
        // String or unknown
        columns[i] = {
          kind: 'string',
          data: bytes,
          offsets: new Uint32Array(expectedRows),
          lengths: new Uint32Array(expectedRows),
          needsUnescape: new Uint8Array(expectedRows),
        };
    }
  }

  let rowIndex = 0;

  while (pos < len && rowIndex < expectedRows) {
    // 1. Find end of line
    let lineEnd = pos;
    while (lineEnd < len && bytes[lineEnd] !== CODE_NEWLINE) {
      lineEnd++;
    }

    // Handle CRLF
    let lineEndClean = lineEnd;
    if (lineEndClean > pos && bytes[lineEndClean - 1] === CODE_RETURN) {
      lineEndClean--;
    }

    // 2. Parse Line
    let currentPos = pos;
    let colIndex = 0;

    while (currentPos <= lineEndClean && colIndex < colOrderLen) {
      let fieldStart = currentPos;
      let fieldEnd = currentPos;

      // Check for quotes
      let hasEscapedQuote = 0;
      if (bytes[currentPos] === CODE_QUOTE) {
        fieldStart++; // skip opening quote
        currentPos++;

        while (currentPos < lineEndClean) {
          if (bytes[currentPos] === CODE_QUOTE) {
            if (currentPos + 1 < lineEndClean && bytes[currentPos + 1] === CODE_QUOTE) {
              // Escaped quote "" -> skip both
              currentPos += 2;
              hasEscapedQuote = 1;
            } else {
              // End of quoted field
              fieldEnd = currentPos;
              currentPos++; // skip closing quote
              // consume delimiter if present
              if (currentPos < lineEndClean && bytes[currentPos] === delimiterCode) {
                currentPos++;
              }
              break;
            }
          } else {
            currentPos++;
          }
        }
      } else {
        // Simple field
        while (currentPos < lineEndClean && bytes[currentPos] !== delimiterCode) {
          currentPos++;
        }
        fieldEnd = currentPos;
        if (currentPos < lineEndClean && bytes[currentPos] === delimiterCode) {
          currentPos++;
        }
      }

      // 3. Store Value
      const vec = columns[colIndex]!;

      switch (vec.kind) {
        case 'float64':
          vec.data[rowIndex] = batof(bytes, fieldStart, fieldEnd);
          break;
        case 'int32':
          vec.data[rowIndex] = batoi(bytes, fieldStart, fieldEnd);
          break;
        case 'bool': {
          const first = bytes[fieldStart];
          vec.data[rowIndex] = first === 116 || first === 84 || first === 49 ? 1 : 0;
          break;
        }
        case 'string':
          vec.offsets[rowIndex] = fieldStart;
          vec.lengths[rowIndex] = fieldEnd - fieldStart;
          if (hasEscapedQuote) vec.needsUnescape[rowIndex] = 1;
          break;
      }

      colIndex++;
    }

    // Fill remaining columns with defaults
    while (colIndex < colOrderLen) {
      colIndex++;
    }

    rowIndex++;
    pos = lineEnd + 1;
  }

  return columns;
}

/**
 * Byte-Array to Integer (batoi)
 */
export function batoi(bytes: Uint8Array, start: number, end: number): number {
  let val = 0;
  let sign = 1;
  let i = start;

  if (i >= end) return 0;

  if (bytes[i] === CODE_MINUS) {
    sign = -1;
    i++;
  } else if (bytes[i] === CODE_PLUS) {
    i++;
  }

  for (; i < end; i++) {
    const code = bytes[i]!;
    if (code >= CODE_0 && code <= CODE_9) {
      val = val * 10 + (code - CODE_0);
    } else {
      // Non-digit encountered (maybe space or garbage), stop or ignore?
      // For strict parsing we might stop.
      break;
    }
  }

  return val * sign;
}

/**
 * Byte-Array to Float (batof)
 */
export function batof(bytes: Uint8Array, start: number, end: number): number {
  if (start >= end) return 0;

  let val = 0.0;
  let sign = 1.0;
  let i = start;

  // Sign
  if (bytes[i] === CODE_MINUS) {
    sign = -1.0;
    i++;
  } else if (bytes[i] === CODE_PLUS) {
    i++;
  }

  // Integer part
  for (; i < end; i++) {
    const code = bytes[i]!;
    if (code >= CODE_0 && code <= CODE_9) {
      val = val * 10 + (code - CODE_0);
    } else if (code === CODE_DOT) {
      i++;
      break;
    } else if (code === CODE_E_LOWER || code === CODE_E_UPPER) {
      return parseScientific(val * sign, bytes, i + 1, end);
    } else {
      break; // invalid char
    }
  }

  // Fractional part
  if (i < end) {
    let fraction = 0.1;
    for (; i < end; i++) {
      const code = bytes[i]!;
      if (code >= CODE_0 && code <= CODE_9) {
        val += (code - CODE_0) * fraction;
        fraction *= 0.1;
      } else if (code === CODE_E_LOWER || code === CODE_E_UPPER) {
        return parseScientific(val * sign, bytes, i + 1, end);
      } else {
        break;
      }
    }
  }

  return val * sign;
}

function parseScientific(base: number, bytes: Uint8Array, start: number, end: number): number {
  const exponent = batoi(bytes, start, end);
  return base * 10 ** exponent;
}
