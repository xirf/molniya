/**
 * Public exports for the buffer module.
 */

export {
	Chunk,
	createChunkFromArrays,
	createEmptyChunk,
} from "./chunk.ts";

export {
	ColumnBuffer,
	columnBufferFromArray,
	createColumnBuffer,
	type TypedArray,
	type TypedArrayFor,
} from "./column-buffer.ts";
export {
	createDictionary,
	type DictIndex,
	Dictionary,
	NULL_INDEX,
} from "./dictionary.ts";

export {
	SelectionBufferPool,
	selectionPool,
} from "./selection-pool.ts";

// Enhanced string handling
export {
	StringView,
	SharedStringBuffer,
	createStringView,
} from "./string-view.ts";

export {
	EnhancedChunk,
	EnhancedChunkBuilder,
} from "./chunk-ext.ts";

export {
	ColumnStorageAdapter,
	fromColumnBuffer,
	fromStringView,
	type ColumnStorage,
} from "./column-storage.ts";

export {
	AdaptiveStringColumn,
	type StringEncoding,
	type CardinalityStats,
} from "./adaptive-string.ts";

export {
	EnhancedColumnBuffer,
} from "./column-buffer-ext.ts";

export {
	getStringResourceManager,
	type StringResourceHandle,
} from "./string-resources.ts";

export {
	UTF8Utils,
	StringViewOps,
	AdaptiveStringOps,
} from "./utf8-ops.ts";
