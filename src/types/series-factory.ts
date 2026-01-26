import type { DType } from './dtypes';
import type { Series } from './series';

/**
 * Create a Series from an array of values
 * @param data - Array of values
 * @param dtype - Data type
 * @param name - Optional series name
 * @returns Series instance
 */
export function createSeries<T>(data: T[], dtype: DType, name?: string): Series<T> {
  // This will be implemented when we integrate with Column creation
  // For now, this is a placeholder
  throw new Error('createSeries not yet implemented - use DataFrame.get() instead');
}
