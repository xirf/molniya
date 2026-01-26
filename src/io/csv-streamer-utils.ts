import type { FilterOperator } from '../types/operators';

// Byte constants
export const COMMA = 44;
export const CR = 13;
export const LF = 10;
export const MINUS = 45;
export const PLUS = 43;
export const DOT = 46;
export const ZERO = 48;
export const NINE = 57;
export const CHAR_t = 116;
export const CHAR_T = 84;
export const CHAR_1 = 49;

export function parseFloatFromBytes(bytes: Uint8Array, start: number, end: number): number {
  if (start >= end) return 0;

  let i = start;
  let negative = false;

  if (bytes[i] === MINUS) {
    negative = true;
    i++;
  } else if (bytes[i] === PLUS) {
    i++;
  }

  let intPart = 0;
  while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
    intPart = intPart * 10 + (bytes[i]! - ZERO);
    i++;
  }

  let result = intPart;

  if (i < end && bytes[i] === DOT) {
    i++;
    let fracPart = 0;
    let fracDigits = 0;
    while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
      fracPart = fracPart * 10 + (bytes[i]! - ZERO);
      fracDigits++;
      i++;
    }
    if (fracDigits > 0) {
      result += fracPart / 10 ** fracDigits;
    }
  }

  return negative ? -result : result;
}

export function parseFloatFromBytesStrict(
  bytes: Uint8Array,
  start: number,
  end: number,
): { value: number; valid: boolean } {
  if (start >= end) return { value: 0, valid: false };

  let i = start;
  let negative = false;

  if (bytes[i] === MINUS) {
    negative = true;
    i++;
  } else if (bytes[i] === PLUS) {
    i++;
  }

  let intPart = 0;
  let intDigits = 0;
  while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
    intPart = intPart * 10 + (bytes[i]! - ZERO);
    intDigits++;
    i++;
  }

  let result = intPart;
  let fracDigits = 0;

  if (i < end && bytes[i] === DOT) {
    i++;
    let fracPart = 0;
    while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
      fracPart = fracPart * 10 + (bytes[i]! - ZERO);
      fracDigits++;
      i++;
    }
    if (fracDigits > 0) {
      result += fracPart / 10 ** fracDigits;
    }
  }

  const valid = i === end && intDigits + fracDigits > 0;
  return { value: negative ? -result : result, valid };
}

export function parseIntFromBytes(bytes: Uint8Array, start: number, end: number): number | null {
  if (start >= end) return null;

  let i = start;
  let negative = false;

  if (bytes[i] === MINUS) {
    negative = true;
    i++;
  } else if (bytes[i] === PLUS) {
    i++;
  }

  const digitStart = i;
  let result = 0;
  while (i < end && bytes[i]! >= ZERO && bytes[i]! <= NINE) {
    result = result * 10 + (bytes[i]! - ZERO);
    i++;
  }

  if (i === digitStart || i < end) return null;
  return negative ? -result : result;
}

export function parseBoolStrict(
  bytes: Uint8Array,
  start: number,
  end: number,
): { value: boolean; valid: boolean } {
  const length = end - start;
  if (length === 1) {
    const byte = bytes[start];
    if (byte === CHAR_t || byte === CHAR_T || byte === CHAR_1) return { value: true, valid: true };
    if (byte === 48 || byte === 102 || byte === 70) return { value: false, valid: true }; // 0, f, F
    return { value: false, valid: false };
  }

  if (length === 4) {
    if (
      bytes[start] === 116 &&
      bytes[start + 1] === 114 &&
      bytes[start + 2] === 117 &&
      bytes[start + 3] === 101
    ) {
      return { value: true, valid: true };
    }
  }

  if (length === 5) {
    if (
      bytes[start] === 102 &&
      bytes[start + 1] === 97 &&
      bytes[start + 2] === 108 &&
      bytes[start + 3] === 115 &&
      bytes[start + 4] === 101
    ) {
      return { value: false, valid: true };
    }
  }

  return { value: false, valid: false };
}

export function isNullField(
  bytes: Uint8Array,
  start: number,
  end: number,
  nullValueBytes: Uint8Array[],
): boolean {
  const len = end - start;

  for (const nullBytes of nullValueBytes) {
    if (nullBytes.length !== len) continue;

    let match = true;
    for (let i = 0; i < len; i++) {
      if (bytes[start + i] !== nullBytes[i]) {
        match = false;
        break;
      }
    }
    if (match) return true;
  }

  return false;
}

export function parseHeaderLine(
  bytes: Uint8Array,
  start: number,
  end: number,
  delimiter: number,
): string[] {
  const headers: string[] = [];
  let fieldStart = start;
  const decoder = new TextDecoder();

  for (let i = start; i <= end; i++) {
    if (i === end || bytes[i] === delimiter) {
      let fieldEnd = i;
      if (i === end && i < bytes.length) fieldEnd = i + 1;
      if (fieldEnd > fieldStart && bytes[fieldEnd - 1] === CR) fieldEnd--;
      const header = decoder.decode(bytes.subarray(fieldStart, fieldEnd)).trim();
      headers.push(header);
      fieldStart = i + 1;
    }
  }

  return headers;
}

export function evaluatePredicate(
  operator: FilterOperator,
  left: number | bigint | string | boolean,
  right: number | bigint | string | boolean | Array<number | bigint | string | boolean>,
): boolean {
  switch (operator) {
    case '==':
      return left === right;
    case '!=':
      return left !== right;
    case '>':
      return (left as number) > (right as number);
    case '<':
      return (left as number) < (right as number);
    case '>=':
      return (left as number) >= (right as number);
    case '<=':
      return (left as number) <= (right as number);
    case 'in':
      return Array.isArray(right) && right.includes(left as never);
    case 'not-in':
      return Array.isArray(right) && !right.includes(left as never);
    default:
      return false;
  }
}
