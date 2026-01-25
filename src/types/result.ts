/**
 * Result type for panic-free error handling
 * Represents either a successful value (ok) or an error
 */
export type Result<T, E = Error> = { ok: true; data: T } | { ok: false; error: E };

/**
 * Creates a successful Result
 * @param data - The success value
 * @returns Result with ok: true
 */
export function ok<T>(data: T): Result<T, never> {
  return { ok: true, data };
}

/**
 * Creates an error Result
 * @param error - The error value (string, Error, or custom type)
 * @returns Result with ok: false
 */
export function err<E>(error: E): Result<never, E> {
  return { ok: false, error };
}

/**
 * Unwraps a Result, returning the data if successful or throwing the error
 * @param result - The Result to unwrap
 * @returns The success value
 * @throws The error if the Result is not ok
 */
export function unwrap<T, E = Error>(result: Result<T, E>): T {
  if (!result.ok) {
    throw result.error;
  }
  return result.data;
}

/**
 * Unwraps the error from a Result that is known to be an error
 * This helper works around TypeScript's type narrowing limitations
 * @param result - A Result that is known to have ok: false
 * @returns The error value
 */
export function unwrapErr<T, E>(result: Result<T, E>): E {
  if (result.ok) {
    throw new Error('Called unwrapErr on a successful Result');
  }
  // Type assertion is safe here because we know result.ok is false
  return (result as { ok: false; error: E }).error;
}
