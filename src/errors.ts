export type ErrorCode =
  | "SCHEMA"
  | "TYPE"
  | "RANGE"
  | "DISPOSED"
  | "MEMORY"
  | "CSV"
  | "IO"
  | "CANCELLED"
  | "PLAN";

export class EngineError extends Error {
  constructor(
    readonly code: ErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "EngineError";
  }
}

export function requireThat(
  condition: unknown,
  code: ErrorCode,
  message: string,
): asserts condition {
  if (!condition) throw new EngineError(code, message);
}

export function integer(value: number, name: string, minimum = 0): void {
  requireThat(
    Number.isSafeInteger(value) && value >= minimum,
    "RANGE",
    `${name} must be a safe integer >= ${minimum}`,
  );
}
