import { MornyeError } from './base';

/**
 * Error thrown when accessing an invalid index.
 */
export class IndexOutOfBoundsError extends MornyeError {
  readonly index: number;
  readonly min: number;
  readonly max: number;

  constructor(index: number, min: number, max: number) {
    const hint = max >= min
      ? `valid range is ${min} to ${max}`
      : 'collection is empty';
    
    super(`index out of bounds`, hint);
    this.name = 'IndexOutOfBoundsError';
    this.index = index;
    this.min = min;
    this.max = max;
  }

  protected override _getExpression(): string {
    return `index[${this.index}]`;
  }

  protected override _getDetail(): string {
    return `index ${this.index} is outside the valid range`;
  }
}
