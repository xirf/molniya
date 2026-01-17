import { MolniyaError } from './base';

/**
 * Error thrown when an operation is called on an incompatible type.
 */
export class TypeMismatchError extends MolniyaError {
  readonly operation: string;
  readonly actualType: string;
  readonly expectedTypes: string[];

  constructor(operation: string, actualType: string, expectedTypes: string[]) {
    const expected = expectedTypes.join(' or ');
    const hint = `'${operation}' requires ${expected} Series`;

    super('type mismatch', hint);
    this.name = 'TypeMismatchError';
    this.operation = operation;
    this.actualType = actualType;
    this.expectedTypes = expectedTypes;
  }

  protected override _getExpression(): string {
    return `series.${this.operation}()`;
  }

  protected override _getDetail(): string {
    return `cannot call '${this.operation}()' on ${this.actualType} Series`;
  }
}
