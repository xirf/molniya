/**
 * Dictionary table for string interning.
 *
 * Strings are stored once and referenced by uint32 index.
 * This avoids string object creation during processing.
 *
 * Features:
 * - FNV-1a hashing for fast deduplication
 * - Byte-level comparison (no string conversion for equality)
 * - O(1) lookup by index, O(1) amortized insert
 */

/** FNV-1a hash constants */
const FNV_OFFSET_BASIS = 2166136261;
const FNV_PRIME = 16777619;

/** Initial capacity for the dictionary */
const INITIAL_CAPACITY = 1024;

/** Load factor threshold for rehashing */
const LOAD_FACTOR = 0.75;

/** Represents the index of a string in the dictionary */
export type DictIndex = number;

/** Special index for null/missing string */
export const NULL_INDEX: DictIndex = 0xffffffff;

/**
 * Dictionary table for string interning.
 *
 * Stores unique strings and returns indices for deduplication.
 * All string operations use UTF-8 bytes to avoid object allocation.
 */
export class Dictionary {
	/** String data storage (concatenated UTF-8 bytes) */
	private data: Uint8Array;
	/** Byte offset where next string will be written */
	private dataOffset: number;

	/** Offset and length for each string: [offset0, len0, offset1, len1, ...] */
	private offsets: Uint32Array;
	/** Number of strings stored */
	private count: number;

	/** Hash table: hash -> first index with that hash (for chaining) */
	private hashTable: Int32Array;
	/** Chain links: index -> next index with same hash (-1 = end) */
	private chains: Int32Array;
	/** Cached hash values per entry, avoids recomputation during rehash */
	private hashes: Uint32Array;
	/** Current hash table size (power of 2) */
	private hashTableSize: number;

	/** Text encoder/decoder for string conversion (only used at boundaries) */
	private readonly encoder: TextEncoder;
	private readonly decoder: TextDecoder;

	/** Lazy string cache: decoded strings indexed by DictIndex */
	private stringCache: string[] = [];

	constructor(initialCapacity: number = INITIAL_CAPACITY) {
		// Ensure power of 2 for hash table
		this.hashTableSize = nextPowerOf2(initialCapacity);

		this.data = new Uint8Array(initialCapacity * 32); // Assume avg 32 bytes per string
		this.dataOffset = 0;

		this.offsets = new Uint32Array(initialCapacity * 2); // [offset, len] pairs
		this.count = 0;

		this.hashTable = new Int32Array(this.hashTableSize).fill(-1);
		this.chains = new Int32Array(initialCapacity).fill(-1);
		this.hashes = new Uint32Array(initialCapacity);

		this.encoder = new TextEncoder();
		this.decoder = new TextDecoder("utf-8", { fatal: true });
	}

	/** Number of unique strings in the dictionary */
	get size(): number {
		return this.count;
	}

	/** Total bytes used for string data */
	get dataSize(): number {
		return this.dataOffset;
	}

	/**
	 * Intern a string from UTF-8 bytes.
	 * Returns the index if string exists, or adds it and returns new index.
	 */
	intern(bytes: Uint8Array): DictIndex {
		if (bytes.length === 0) {
			// Empty string gets index 0 if not already present
			return this.internEmpty();
		}

		const hash = this.hash(bytes);
		const slot = hash & (this.hashTableSize - 1);

		// Search chain for existing entry
		let idx = this.hashTable[slot] ?? -1;
		while (idx !== -1) {
			if (this.bytesEqual(idx, bytes)) {
				return idx;
			}
			idx = this.chains[idx] ?? -1;
		}

		// Not found, add new entry
		return this.addEntry(bytes, hash, slot);
	}

	/**
	 * Intern a string (convenience method).
	 * Encodes to UTF-8 first - use intern(bytes) in hot paths.
	 */
	internString(str: string): DictIndex {
		if (str.length === 0) {
			return this.internEmpty();
		}
		const bytes = this.encoder.encode(str);
		return this.intern(bytes);
	}

	/**
	 * Get string by index (cached).
	 * First call per index decodes from UTF-8; subsequent calls return cached string.
	 */
	getString(index: DictIndex): string | undefined {
		if (index >= this.count) {
			return undefined;
		}
		// Check cache (undefined means not yet cached; sparse array access returns undefined)
		const cached = this.stringCache[index];
		if (cached !== undefined) return cached;

		const bytes = this.getBytes(index);
		if (bytes === undefined) {
			return undefined;
		}
		const str = this.decoder.decode(bytes);
		this.stringCache[index] = str;
		return str;
	}

	/**
	 * Get raw bytes by index (zero-copy view).
	 */
	getBytes(index: DictIndex): Uint8Array | undefined {
		if (index >= this.count) {
			return undefined;
		}
		const offsetIdx = index * 2;
		const offset = this.offsets[offsetIdx] ?? 0;
		const length = this.offsets[offsetIdx + 1] ?? 0;
		return this.data.subarray(offset, offset + length);
	}

	/**
	 * Compare two dictionary entries by index.
	 * Returns negative if a < b, positive if a > b, 0 if equal.
	 */
	compare(a: DictIndex, b: DictIndex): number {
		if (a === b) return 0;

		const aBytes = this.getBytes(a);
		const bBytes = this.getBytes(b);

		if (aBytes === undefined) return bBytes === undefined ? 0 : -1;
		if (bBytes === undefined) return 1;

		const minLen = Math.min(aBytes.length, bBytes.length);
		for (let i = 0; i < minLen; i++) {
			const diff = (aBytes[i] ?? 0) - (bBytes[i] ?? 0);
			if (diff !== 0) return diff;
		}
		return aBytes.length - bBytes.length;
	}

	/**
	 * Check if bytes at index equal the given bytes.
	 */
	bytesEqual(index: DictIndex, bytes: Uint8Array): boolean {
		const offsetIdx = index * 2;
		const offset = this.offsets[offsetIdx] ?? 0;
		const length = this.offsets[offsetIdx + 1] ?? 0;

		if (length !== bytes.length) return false;

		for (let i = 0; i < length; i++) {
			if (this.data[offset + i] !== bytes[i]) return false;
		}
		return true;
	}

	/** Handle empty string specially */
	private internEmpty(): DictIndex {
		// Check if empty string is already at index 0
		if (this.count > 0) {
			const len = this.offsets[1] ?? 0;
			if (len === 0) {
				return 0;
			}
		}

		// Need to add empty string
		if (this.count === 0) {
			this.offsets[0] = 0;
			this.offsets[1] = 0;
			this.count = 1;
			return 0;
		}

		// Empty string not at index 0, add normally
		const emptyBytes = new Uint8Array(0);
		const hash = this.hash(emptyBytes);
		const slot = hash & (this.hashTableSize - 1);
		return this.addEntry(emptyBytes, hash, slot);
	}

	/** Add a new entry to the dictionary */
	private addEntry(bytes: Uint8Array, hash: number, slot: number): DictIndex {
		// Check if we need to grow
		if (this.count >= this.chains.length) {
			this.grow();
			// Recalculate slot after grow
			slot = hash & (this.hashTableSize - 1);
		}

		// Check if we need more data space
		if (this.dataOffset + bytes.length > this.data.length) {
			this.growData(bytes.length);
		}

		const index = this.count;

		// Store offset and length
		const offsetIdx = index * 2;
		if (offsetIdx + 1 >= this.offsets.length) {
			const newOffsets = new Uint32Array(this.offsets.length * 2);
			newOffsets.set(this.offsets);
			this.offsets = newOffsets;
		}
		this.offsets[offsetIdx] = this.dataOffset;
		this.offsets[offsetIdx + 1] = bytes.length;

		// Copy string data
		this.data.set(bytes, this.dataOffset);
		this.dataOffset += bytes.length;

		// Store hash for fast rehash later
		if (index >= this.hashes.length) {
			const newHashes = new Uint32Array(this.hashes.length * 2);
			newHashes.set(this.hashes);
			this.hashes = newHashes;
		}
		this.hashes[index] = hash;

		// Add to hash chain
		this.chains[index] = this.hashTable[slot] ?? -1;
		this.hashTable[slot] = index;

		this.count++;

		// Check load factor
		if (this.count > this.hashTableSize * LOAD_FACTOR) {
			this.rehash();
		}

		return index;
	}

	/** FNV-1a hash of bytes */
	private hash(bytes: Uint8Array): number {
		let hash = FNV_OFFSET_BASIS;
		for (let i = 0; i < bytes.length; i++) {
			hash ^= bytes[i] ?? 0;
			hash = Math.imul(hash, FNV_PRIME);
		}
		return hash >>> 0; // Ensure unsigned
	}

	/** Grow the chain array */
	private grow(): void {
		const newChains = new Int32Array(this.chains.length * 2).fill(-1);
		newChains.set(this.chains);
		this.chains = newChains;
	}

	/** Grow the data array */
	private growData(needed: number): void {
		const newSize = Math.max(this.data.length * 2, this.dataOffset + needed);
		const newData = new Uint8Array(newSize);
		newData.set(this.data);
		this.data = newData;
	}

	/** Rehash when load factor exceeded */
	private rehash(): void {
		this.hashTableSize *= 2;
		this.hashTable = new Int32Array(this.hashTableSize).fill(-1);
		this.chains.fill(-1);

		// Re-insert all entries using cached hashes (no recomputation needed)
		const mask = this.hashTableSize - 1;
		for (let i = 0; i < this.count; i++) {
			const slot = (this.hashes[i]!) & mask;
			this.chains[i] = this.hashTable[slot] ?? -1;
			this.hashTable[slot] = i;
		}
	}
}

/** Find next power of 2 >= n */
function nextPowerOf2(n: number): number {
	if (n <= 0) return 1;
	n--;
	n |= n >> 1;
	n |= n >> 2;
	n |= n >> 4;
	n |= n >> 8;
	n |= n >> 16;
	return n + 1;
}

/**
 * Create a new empty dictionary.
 */
export function createDictionary(initialCapacity?: number): Dictionary {
	return new Dictionary(initialCapacity);
}
