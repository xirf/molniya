import { MolniyaError } from './base';

/**
 * Error thrown when an operation cannot be performed.
 */
export class InvalidOperationError extends MolniyaError {
  readonly operation: string;
  readonly reason: string;

  constructor(operation: string, reason: string, hint?: string) {
    super('invalid operation', hint);
    this.name = 'InvalidOperationError';
    this.operation = operation;
    this.reason = reason;
  }

  protected override _getExpression(): string {
    return `${this.operation}(...)`;
  }

  protected override _getDetail(): string {
    return `'${this.operation}' ${this.reason}`;
  }
}
