/**
 * Go-style error handling for zero-allocation error paths.
 *
 * Instead of throwing exceptions (which allocates Error objects and unwinds stack),
 * we return [value, errorCode] tuples. On the happy path, errorCode is None.
 * On error, value is undefined and errorCode indicates what went wrong.
 */

/** Error codes as const enum - compiles to plain numbers, zero allocation */
export enum ErrorCode {
	None = 0,

	// Buffer errors (1-99)
	BufferFull = 1,
	BufferEmpty = 2,
	BufferOverflow = 3,
	InvalidOffset = 4,
	InvalidCapacity = 5,
	BufferUnderflow = 6,

	// Schema errors (100-199)
	SchemaMismatch = 100,
	UnknownColumn = 101,
	TypeMismatch = 102,
	DuplicateColumn = 103,
	EmptySchema = 104,
	InvalidColumnName = 105,

	// Parse errors (200-299)
	MalformedData = 200,
	InvalidUtf8 = 201,
	UnexpectedEof = 202,
	InvalidNumber = 203,
	InvalidBoolean = 204,
	InvalidDate = 205,
	UnterminatedQuote = 206,
	TooManyColumns = 207,
	TooFewColumns = 208,

	// I/O errors (300-399)
	FileNotFound = 300,
	ReadError = 301,
	WriteError = 302,
	PermissionDenied = 303,

	// Pipeline errors (400-499)
	InvalidPipeline = 400,
	ExecutionFailed = 401,
	InvalidExpression = 402,
	InvalidAggregation = 403,
	EmptyInput = 404,

	// Expression errors (500-599)
	DivisionByZero = 500,
	ColumnNotFound = 501,
	TypeIncompatible = 502,
	InvalidOperand = 503,

	// Cast errors (510-519)
	CastNotSupported = 510,
	CastOverflow = 511,
	InvalidFillValue = 512,

	// Internal/state errors (520-529)
	InvalidState = 520,
	InvalidArgument = 521,
}

/** Human-readable error messages for debugging */
export const ERROR_MESSAGES: Record<ErrorCode, string> = {
	[ErrorCode.None]: "No error",
	[ErrorCode.BufferFull]: "Buffer is full",
	[ErrorCode.BufferEmpty]: "Buffer is empty",
	[ErrorCode.BufferOverflow]: "Buffer overflow",
	[ErrorCode.InvalidOffset]: "Invalid offset",
	[ErrorCode.InvalidCapacity]: "Invalid buffer capacity",
	[ErrorCode.BufferUnderflow]: "Buffer underflow",
	[ErrorCode.SchemaMismatch]: "Schema mismatch",
	[ErrorCode.UnknownColumn]: "Unknown column",
	[ErrorCode.TypeMismatch]: "Type mismatch",
	[ErrorCode.DuplicateColumn]: "Duplicate column name",
	[ErrorCode.EmptySchema]: "Schema cannot be empty",
	[ErrorCode.InvalidColumnName]: "Invalid column name",
	[ErrorCode.MalformedData]: "Malformed data",
	[ErrorCode.InvalidUtf8]: "Invalid UTF-8 encoding",
	[ErrorCode.UnexpectedEof]: "Unexpected end of file",
	[ErrorCode.InvalidNumber]: "Invalid number format",
	[ErrorCode.InvalidBoolean]: "Invalid boolean value",
	[ErrorCode.InvalidDate]: "Invalid date format",
	[ErrorCode.UnterminatedQuote]: "Unterminated quoted field",
	[ErrorCode.TooManyColumns]: "Too many columns in row",
	[ErrorCode.TooFewColumns]: "Too few columns in row",
	[ErrorCode.FileNotFound]: "File not found",
	[ErrorCode.ReadError]: "Read error",
	[ErrorCode.WriteError]: "Write error",
	[ErrorCode.PermissionDenied]: "Permission denied",
	[ErrorCode.InvalidPipeline]: "Invalid pipeline configuration",
	[ErrorCode.ExecutionFailed]: "Pipeline execution failed",
	[ErrorCode.InvalidExpression]: "Invalid expression",
	[ErrorCode.InvalidAggregation]: "Invalid aggregation",
	[ErrorCode.EmptyInput]: "Input is empty",
	[ErrorCode.DivisionByZero]: "Division by zero",
	[ErrorCode.ColumnNotFound]: "Column not found",
	[ErrorCode.TypeIncompatible]: "Incompatible types for operation",
	[ErrorCode.InvalidOperand]: "Invalid operand",
	[ErrorCode.CastNotSupported]: "Cast not supported for this type combination",
	[ErrorCode.CastOverflow]: "Value overflow during cast",
	[ErrorCode.InvalidFillValue]: "Invalid fill value for column type",
	[ErrorCode.InvalidState]: "Invalid state",
	[ErrorCode.InvalidArgument]: "Invalid argument",
};

/**
 * Result type for operations that can fail.
 * Discriminated union: check error first, then access value.
 *
 * Usage:
 *   const result = someOperation();
 *   if (result.error !== ErrorCode.None) {
 *     console.error(getErrorMessage(result.error));
 *     return;
 *   }
 *   // result.value is now safely accessible
 *   console.log(result.value);
 */
export type Result<T> =
	| { readonly value: T; readonly error: ErrorCode.None }
	| {
			readonly value: undefined;
			readonly error: Exclude<ErrorCode, ErrorCode.None>;
	  };

/** Create a successful result */
export function ok<T>(value: T): Result<T> {
	return { value, error: ErrorCode.None };
}

/** Create an error result */
export function err<T>(error: Exclude<ErrorCode, ErrorCode.None>): Result<T> {
	return { value: undefined, error };
}

/** Check if a result is successful */
export function isOk<T>(
	result: Result<T>,
): result is { value: T; error: ErrorCode.None } {
	return result.error === ErrorCode.None;
}

/** Check if a result is an error */
export function isErr<T>(
	result: Result<T>,
): result is { value: undefined; error: Exclude<ErrorCode, ErrorCode.None> } {
	return result.error !== ErrorCode.None;
}

/** Get human-readable error message */
export function getErrorMessage(code: ErrorCode): string {
	return ERROR_MESSAGES[code] ?? `Unknown error (${code})`;
}

/**
 * Unwrap a result, throwing if it's an error.
 * Use sparingly - only in tests or at application boundaries.
 */
export function unwrap<T>(result: Result<T>): T {
	if (result.error !== ErrorCode.None) {
		throw new Error(getErrorMessage(result.error));
	}
	return result.value;
}

/**
 * Unwrap a result or return a default value.
 */
export function unwrapOr<T>(result: Result<T>, defaultValue: T): T {
	if (result.error !== ErrorCode.None) {
		return defaultValue;
	}
	return result.value;
}

/**
 * Map over a successful result.
 */
export function mapResult<T, U>(
	result: Result<T>,
	fn: (value: T) => U,
): Result<U> {
	if (result.error !== ErrorCode.None) {
		return { value: undefined, error: result.error };
	}
	return ok(fn(result.value));
}

/**
 * Chain results (flatMap).
 */
export function andThen<T, U>(
	result: Result<T>,
	fn: (value: T) => Result<U>,
): Result<U> {
	if (result.error !== ErrorCode.None) {
		return { value: undefined, error: result.error };
	}
	return fn(result.value);
}
