/**
 * Base error class for all Mornye errors.
 * Provides formatted error output with location tracking and hints.
 */
export class MornyeError extends Error {
  readonly hint?: string;
  readonly location?: { file: string; line: number; column: number };

  constructor(message: string, hint?: string) {
    super(message);
    this.name = 'MornyeError';
    this.hint = hint;
    this.location = this._extractLocation();
  }

  private _extractLocation(): { file: string; line: number; column: number } | undefined {
    const stack = this.stack;
    if (!stack) return undefined;

    const lines = stack.split('\n');
    for (const line of lines) {
      if (line.includes('node_modules') || line.includes('MornyeError')) continue;

      const match =
        line.match(/at .+? \((.+?):(\d+):(\d+)\)/) || line.match(/at (.+?):(\d+):(\d+)/);

      if (match) {
        return {
          file: match[1]!,
          line: Number.parseInt(match[2]!, 10),
          column: Number.parseInt(match[3]!, 10),
        };
      }
    }
    return undefined;
  }

  format(): string {
    const lines: string[] = [];

    const loc = this.location
      ? ` at ${this.location.file.split('/').slice(-1)[0]}:${this.location.line}:${this.location.column}`
      : '';

    lines.push(`error: ${this.message}${loc}`);
    lines.push(`  --> ${this._getExpression()}`);
    lines.push('   |');
    lines.push(`   └── ${this._getDetail()}`);

    if (this.hint) {
      lines.push('');
      lines.push(`help: ${this.hint}`);
    }

    return lines.join('\n');
  }

  protected _getExpression(): string {
    return '(expression)';
  }

  protected _getDetail(): string {
    return this.message;
  }
}
