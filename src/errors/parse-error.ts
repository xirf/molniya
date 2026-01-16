import { MornyeError } from './base';

/**
 * Error thrown when parsing fails.
 */
export class ParseError extends MornyeError {
  readonly what: string;
  readonly value: string;

  constructor(what: string, value: string, hint?: string) {
    super('parse error', hint);
    this.name = 'ParseError';
    this.what = what;
    this.value = value;
  }

  protected override _getExpression(): string {
    return `parse ${this.what}`;
  }

  protected override _getDetail(): string {
    return `failed to parse ${this.what} from '${this.value}'`;
  }
}
