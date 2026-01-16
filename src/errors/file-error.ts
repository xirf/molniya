import { MornyeError } from './base';

/**
 * Error thrown when file operations fail.
 */
export class FileError extends MornyeError {
  readonly path: string;
  readonly reason: string;

  constructor(path: string, reason: string, hint?: string) {
    super('file error', hint);
    this.name = 'FileError';
    this.path = path;
    this.reason = reason;
  }

  protected override _getExpression(): string {
    return `readCsv('${this.path}')`;
  }

  protected override _getDetail(): string {
    return `cannot read '${this.path}': ${this.reason}`;
  }
}
