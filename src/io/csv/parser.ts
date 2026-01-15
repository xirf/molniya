import { BYTES } from './options';

/**
 * Parser states for the CSV state machine.
 */
enum State {
  /** Normal field parsing */
  Field = 0,
  /** Inside a quoted field */
  QuotedField = 1,
  /** Just saw a quote inside quoted field (could be escape or end) */
  AfterQuote = 2,
}

/**
 * Low-level byte-oriented CSV parser.
 *
 * Processes raw byte chunks without converting entire file to strings.
 * Uses a state machine for correct RFC 4180 parsing.
 */
export class CsvChunkParser {
  private state: State = State.Field;
  private readonly delimiter: number;
  private readonly quote: number;

  /** Buffer for current field (reused) */
  private fieldBuffer: Uint8Array;
  private fieldLength = 0;

  /** Current row being built */
  private currentRow: string[] = [];

  /** Completed rows waiting to be consumed */
  private pendingRows: string[][] = [];

  /** Text decoder for field conversion */
  private readonly decoder = new TextDecoder('utf-8');

  constructor(delimiter: number = BYTES.COMMA, quote: number = BYTES.QUOTE, bufferSize = 4096) {
    this.delimiter = delimiter;
    this.quote = quote;
    this.fieldBuffer = new Uint8Array(bufferSize);
  }

  /**
   * Processes a chunk of bytes.
   * Call consumeRows() after to get completed rows.
   */
  processChunk(chunk: Uint8Array): void {
    for (let i = 0; i < chunk.length; i++) {
      const byte = chunk[i]!;
      this.processByte(byte);
    }
  }

  /**
   * Signals end of input.
   * Flushes any remaining data as final row.
   */
  finish(): void {
    // Emit final field if any content
    if (this.fieldLength > 0 || this.currentRow.length > 0) {
      this.emitField();
      this.emitRow();
    }
  }

  /**
   * Consumes and returns all completed rows.
   * Clears the internal pending queue.
   */
  consumeRows(): string[][] {
    const rows = this.pendingRows;
    this.pendingRows = [];
    return rows;
  }

  /**
   * Returns count of pending rows without consuming.
   */
  pendingCount(): number {
    return this.pendingRows.length;
  }

  // ─────────────────────────────────────────────────────────────
  // Internal State Machine
  // ─────────────────────────────────────────────────────────────

  private processByte(byte: number): void {
    switch (this.state) {
      case State.Field:
        this.handleFieldState(byte);
        break;
      case State.QuotedField:
        this.handleQuotedFieldState(byte);
        break;
      case State.AfterQuote:
        this.handleAfterQuoteState(byte);
        break;
    }
  }

  private handleFieldState(byte: number): void {
    if (byte === this.delimiter) {
      this.emitField();
    } else if (byte === BYTES.LF) {
      this.emitField();
      this.emitRow();
    } else if (byte === BYTES.CR) {
      // Ignore CR, wait for LF
    } else if (byte === this.quote && this.fieldLength === 0) {
      // Quote at start of field - enter quoted mode
      this.state = State.QuotedField;
    } else {
      this.appendByte(byte);
    }
  }

  private handleQuotedFieldState(byte: number): void {
    if (byte === this.quote) {
      // Could be escape or end of quoted field
      this.state = State.AfterQuote;
    } else {
      this.appendByte(byte);
    }
  }

  private handleAfterQuoteState(byte: number): void {
    if (byte === this.quote) {
      // Escaped quote - append single quote
      this.appendByte(this.quote);
      this.state = State.QuotedField;
    } else if (byte === this.delimiter) {
      // End of quoted field, followed by delimiter
      this.emitField();
      this.state = State.Field;
    } else if (byte === BYTES.LF) {
      // End of quoted field, end of row
      this.emitField();
      this.emitRow();
      this.state = State.Field;
    } else if (byte === BYTES.CR) {
      // Ignore CR
      this.state = State.Field;
    } else {
      // Malformed CSV - continue as unquoted
      this.appendByte(byte);
      this.state = State.Field;
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Buffer Management
  // ─────────────────────────────────────────────────────────────

  private appendByte(byte: number): void {
    // Grow buffer if needed
    if (this.fieldLength >= this.fieldBuffer.length) {
      const newBuffer = new Uint8Array(this.fieldBuffer.length * 2);
      newBuffer.set(this.fieldBuffer);
      this.fieldBuffer = newBuffer;
    }
    this.fieldBuffer[this.fieldLength++] = byte;
  }

  private emitField(): void {
    // Decode field bytes to string
    const field = this.decoder.decode(this.fieldBuffer.subarray(0, this.fieldLength));
    this.currentRow.push(field);
    this.fieldLength = 0;
  }

  private emitRow(): void {
    if (this.currentRow.length > 0) {
      this.pendingRows.push(this.currentRow);
      this.currentRow = [];
    }
  }
}
