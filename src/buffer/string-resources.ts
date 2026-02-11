/**
 * Shared resource manager for string columns.
 * 
 * Manages shared dictionaries and string buffers across columns to:
 * 1. Enable efficient joins (shared dictionary = integer comparison)
 * 2. Reduce memory usage for similar string patterns
 * 3. Provide resource pooling and lifecycle management
 */

import { createDictionary, type Dictionary } from "./dictionary.ts";
import { SharedStringBuffer } from "./string-view.ts";

/** Resource pool configuration */
interface ResourcePoolConfig {
	maxSharedDictionaries: number;
	maxSharedBuffers: number;
	dictionaryGCThreshold: number; // Size threshold for GC
	bufferGCThreshold: number;     // Size threshold for GC
}

const DEFAULT_CONFIG: ResourcePoolConfig = {
	maxSharedDictionaries: 10,
	maxSharedBuffers: 5,
	dictionaryGCThreshold: 10 * 1024 * 1024, // 10MB
	bufferGCThreshold: 50 * 1024 * 1024,     // 50MB
};

/** Resource identifier for tracking shared resources */
export type ResourceId = string;

/** Shared dictionary with reference counting */
interface SharedDictionary {
	id: ResourceId;
	dictionary: Dictionary;
	refCount: number;
	tags: Set<string>; // Optional tags for categorization
}

/** Shared string buffer with reference counting */
interface SharedBuffer {
	id: ResourceId;
	buffer: SharedStringBuffer;
	refCount: number;
	tags: Set<string>; // Optional tags for categorization
}

/**
 * Global resource manager for shared string resources.
 */
class StringResourceManager {
	private dictionaries: Map<ResourceId, SharedDictionary>;
	private buffers: Map<ResourceId, SharedBuffer>;
	private config: ResourcePoolConfig;

	constructor(config: ResourcePoolConfig = DEFAULT_CONFIG) {
		this.dictionaries = new Map();
		this.buffers = new Map();
		this.config = config;
	}

	/**
	 * Create or get a shared dictionary.
	 * If id already exists, returns existing dictionary and increments ref count.
	 */
	getOrCreateDictionary(id: ResourceId, tags: string[] = []): Dictionary {
		let shared = this.dictionaries.get(id);
		
		if (!shared) {
			// Create new shared dictionary
			shared = {
				id,
				dictionary: createDictionary(),
				refCount: 0,
				tags: new Set(tags)
			};
			this.dictionaries.set(id, shared);
		}
		
		shared.refCount++;
		return shared.dictionary;
	}

	/**
	 * Release reference to a shared dictionary.
	 * Automatically cleans up when ref count reaches zero.
	 */
	releaseDictionary(id: ResourceId): void {
		const shared = this.dictionaries.get(id);
		if (!shared) return;
		
		shared.refCount--;
		
		if (shared.refCount <= 0) {
			this.dictionaries.delete(id);
		}
	}

	/**
	 * Create or get a shared string buffer.
	 */
	getOrCreateBuffer(id: ResourceId, initialSize?: number, tags: string[] = []): SharedStringBuffer {
		let shared = this.buffers.get(id);
		
		if (!shared) {
			// Create new shared buffer
			shared = {
				id,
				buffer: new SharedStringBuffer(initialSize),
				refCount: 0,
				tags: new Set(tags)
			};
			this.buffers.set(id, shared);
		}
		
		shared.refCount++;
		return shared.buffer;
	}

	/**
	 * Release reference to a shared buffer.
	 */
	releaseBuffer(id: ResourceId): void {
		const shared = this.buffers.get(id);
		if (!shared) return;
		
		shared.refCount--;
		
		if (shared.refCount <= 0) {
			this.buffers.delete(id);
		}
	}

	/**
	 * Find dictionaries by tag.
	 * Useful for finding related dictionaries (e.g., "city", "location").
	 */
	findDictionariesByTag(tag: string): ResourceId[] {
		const results: ResourceId[] = [];
		for (const [id, shared] of this.dictionaries) {
			if (shared.tags.has(tag)) {
				results.push(id);
			}
		}
		return results;
	}

	/**
	 * Find buffers by tag.
	 */
	findBuffersByTag(tag: string): ResourceId[] {
		const results: ResourceId[] = [];
		for (const [id, shared] of this.buffers) {
			if (shared.tags.has(tag)) {
				results.push(id);
			}
		}
		return results;
	}

	/**
	 * Get statistics about shared resources.
	 */
	getStats(): {
		dictionaries: {
			count: number;
			totalSize: number;
			avgSize: number;
			maxSize: number;
		};
		buffers: {
			count: number;
			totalSize: number;
			avgSize: number;
			maxSize: number;
		};
	} {
		let dictTotalSize = 0;
		let dictMaxSize = 0;
		
		for (const shared of this.dictionaries.values()) {
			const size = shared.dictionary.dataSize;
			dictTotalSize += size;
			dictMaxSize = Math.max(dictMaxSize, size);
		}
		
		let bufferTotalSize = 0;
		let bufferMaxSize = 0;
		
		for (const shared of this.buffers.values()) {
			const size = shared.buffer.size;
			bufferTotalSize += size;
			bufferMaxSize = Math.max(bufferMaxSize, size);
		}

		return {
			dictionaries: {
				count: this.dictionaries.size,
				totalSize: dictTotalSize,
				avgSize: this.dictionaries.size > 0 ? dictTotalSize / this.dictionaries.size : 0,
				maxSize: dictMaxSize
			},
			buffers: {
				count: this.buffers.size,
				totalSize: bufferTotalSize,
				avgSize: this.buffers.size > 0 ? bufferTotalSize / this.buffers.size : 0,
				maxSize: bufferMaxSize
			}
		};
	}

	/**
	 * Perform garbage collection on oversized resources.
	 */
	gc(): void {
		// GC oversized dictionaries with zero refs
		for (const [id, shared] of this.dictionaries) {
			if (shared.refCount === 0 && shared.dictionary.dataSize > this.config.dictionaryGCThreshold) {
				this.dictionaries.delete(id);
			}
		}

		// GC oversized buffers with zero refs  
		for (const [id, shared] of this.buffers) {
			if (shared.refCount === 0 && shared.buffer.size > this.config.bufferGCThreshold) {
				this.buffers.delete(id);
			}
		}

		// Also compact existing buffers
		for (const shared of this.buffers.values()) {
			shared.buffer.compact();
		}
	}

	/**
	 * Clear all resources (for testing/cleanup).
	 */
	clear(): void {
		this.dictionaries.clear();
		this.buffers.clear();
	}

	/**
	 * List all resource IDs.
	 */
	listResources(): {
		dictionaries: ResourceId[];
		buffers: ResourceId[];
	} {
		return {
			dictionaries: Array.from(this.dictionaries.keys()),
			buffers: Array.from(this.buffers.keys())
		};
	}
}

/** Global singleton resource manager */
const globalResourceManager = new StringResourceManager();

/**
 * Get the global string resource manager.
 */
export function getStringResourceManager(): StringResourceManager {
	return globalResourceManager;
}

/**
 * Convenience function to get or create a shared dictionary.
 * 
 * Example usage:
 * ```typescript
 * // Share dictionary between origin and destination city columns
 * const cityDict = getSharedDictionary("cities", ["location", "geographic"]);
 * ```
 */
export function getSharedDictionary(id: ResourceId, tags?: string[]): Dictionary {
	return globalResourceManager.getOrCreateDictionary(id, tags);
}

/**
 * Convenience function to get or create a shared string buffer.
 */
export function getSharedStringBuffer(id: ResourceId, initialSize?: number, tags?: string[]): SharedStringBuffer {
	return globalResourceManager.getOrCreateBuffer(id, initialSize, tags);
}

/**
 * Release a shared dictionary reference.
 */
export function releaseSharedDictionary(id: ResourceId): void {
	globalResourceManager.releaseDictionary(id);
}

/**
 * Release a shared buffer reference.
 */
export function releaseSharedStringBuffer(id: ResourceId): void {
	globalResourceManager.releaseBuffer(id);
}

/**
 * Resource handle that automatically manages reference counting.
 * Use this for RAII-style resource management.
 */
export class StringResourceHandle {
	private dictionaryIds: Set<ResourceId> = new Set();
	private bufferIds: Set<ResourceId> = new Set();

	/**
	 * Get or create a shared dictionary and track it for cleanup.
	 */
	getDictionary(id: ResourceId, tags?: string[]): Dictionary {
		this.dictionaryIds.add(id);
		return getSharedDictionary(id, tags);
	}

	/**
	 * Get or create a shared buffer and track it for cleanup.
	 */
	getBuffer(id: ResourceId, initialSize?: number, tags?: string[]): SharedStringBuffer {
		this.bufferIds.add(id);
		return getSharedStringBuffer(id, initialSize, tags);
	}

	/**
	 * Release all tracked resources.
	 * Call this when the handle is no longer needed.
	 */
	dispose(): void {
		for (const id of this.dictionaryIds) {
			releaseSharedDictionary(id);
		}
		for (const id of this.bufferIds) {
			releaseSharedStringBuffer(id);
		}
		
		this.dictionaryIds.clear();
		this.bufferIds.clear();
	}
}