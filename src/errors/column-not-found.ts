import { MornyeError } from './base';

/**
 * Error thrown when accessing a non-existent column.
 */
export class ColumnNotFoundError extends MornyeError {
  readonly column: string;
  readonly available: string[];

  constructor(column: string, available: string[]) {
    const hint = available.length > 0
      ? `available columns are: ${available.map(c => `'${c}'`).join(', ')}`
      : 'DataFrame has no columns';
    
    super(`column not found`, hint);
    this.name = 'ColumnNotFoundError';
    this.column = column;
    this.available = available;
  }

  protected override _getExpression(): string {
    return `df.col('${this.column}')`;
  }

  protected override _getDetail(): string {
    return `column '${this.column}' does not exist in DataFrame`;
  }
}
