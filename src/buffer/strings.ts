/**
 * Enhanced string handling for Molniya DataFrame.
 * 
 * This module provides adaptive string storage that automatically chooses
 * between dictionary encoding (for low cardinality) and StringView encoding
 * (for high cardinality) based on data characteristics.
 * 
 * Key features:
 * - Automatic cardinality detection with sampling
 * - User-controllable encoding via DType hints
 * - Shared dictionaries for efficient joins
 * - UTF-8 aware operations without string materialization
 * - Zero V8 heap pressure for high-cardinality strings
 */

// Core string storage implementations
export { StringView, SharedStringBuffer, createStringView, createSharedStringBuffer } from "./string-view.ts";
export { AdaptiveStringColumn, StringEncoding, createAdaptiveStringColumn } from "./adaptive-string.ts";
export { EnhancedColumnBuffer, createEnhancedColumnBuffer } from "./column-buffer-ext.ts";

// Resource management
export {
	StringResourceHandle,
	getStringResourceManager,
	getSharedDictionary,
	getSharedStringBuffer,
	releaseSharedDictionary,
	releaseSharedStringBuffer,
	type ResourceId
} from "./string-resources.ts";

// UTF-8 operations
export {
	UTF8Utils,
	StringViewOps,
	AdaptiveStringOps,
	shouldUseByteOps
} from "./utf8-ops.ts";

// Re-export enhanced types
export { DTypeKind } from "../types/dtypes.ts";
export type { DType } from "../types/dtypes.ts";

/**
 * Example usage:
 * 
 * ```typescript
 * import { createEnhancedColumnBuffer, DType } from "./strings";
 * 
 * // Auto-adaptive string column
 * const autoColumn = createEnhancedColumnBuffer(
 *   DTypeKind.String, 1000, true
 * );
 * 
 * // Force dictionary encoding
 * const dictColumn = createEnhancedColumnBuffer(
 *   DTypeKind.StringDict, 1000, true,
 *   { sharedDictionaryId: "cities", resourceTags: ["location"] }
 * );
 * 
 * // Force view encoding for high-cardinality data
 * const viewColumn = createEnhancedColumnBuffer(
 *   DTypeKind.StringView, 1000, true,
 *   { sharedBufferId: "text-data" }
 * );
 * 
 * // Add some data
 * autoColumn.append("New York");
 * autoColumn.append("Los Angeles");
 * 
 * // Check what encoding was chosen
 * console.log(autoColumn.getStringEncoding()); // "view" or "dictionary"
 * 
 * // Get recommendations
 * const stats = autoColumn.getStringStats();
 * if (autoColumn.shouldUseDictionary()) {
 *   console.log("Dictionary encoding recommended");
 *   autoColumn.convertToDictionary(); // Expensive O(N) operation
 * }
 * ```
 */