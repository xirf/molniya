import { MornyeError } from './base';

/**
 * Error thrown when schema validation fails.
 */
export class SchemaError extends MornyeError {
  private _detail: string;

  constructor(detail: string, hint?: string) {
    super('schema error', hint);
    this.name = 'SchemaError';
    this._detail = detail;
  }

  protected override _getExpression(): string {
    return 'schema definition';
  }

  protected override _getDetail(): string {
    return this._detail;
  }
}
