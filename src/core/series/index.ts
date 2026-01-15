/**
 * Series module.
 * Provides typed 1D array with Pandas-like operations.
 */

export { Series } from './series';
export { StringAccessor } from './string-accessor';
export type { ISeries, SeriesView } from './interface';
export { createStorage, createStorageFrom, NULL_SENTINELS } from './storage';
