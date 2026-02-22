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

/** Default max entries before LRU eviction (0 = unlimited to prevent data corruption during query execution) */
const DEFAULT_MAX_ENTRIES = 0;

/** Represents the index of a string in the dictionary */
export type DictIndex = number;

/** Special index for null/missing string */
export const NULL_INDEX: DictIndex = 0xffffffff;

/** Options for configuring dictionary behavior */
export interface DictionaryOptions {
	/** Initial hash table capacity (default: 1024) */
	initialCapacity?: number;
	/** Maximum entries before LRU eviction (default: 1_000_000, 0 = unlimited) */
	maxEntries?: number;
}

/**
 * Dictionary table for string interning.
 *
 * Stores unique strings and returns indices for deduplication.
 * All string operations use UTF-8 bytes to avoid object allocation.
 *
 * When maxEntries is set, uses LRU eviction to cap memory usage.
 * Evicted indices are reused for new entries.
 */
export class Dictionary {
	/** String data storage (concatenated UTF-8 bytes) */
	private data: Uint8Array;
	/** Byte offset where next string will be written */
	private dataOffset: number;

	/** Offset and length for each string: [offset0, len0, offset1, len1, ...] */
	private offsets: Uint32Array;
	/** Number of strings stored (including evicted slots that are reused) */
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

	/** Maximum entries before eviction (0 = unlimited) */
	private readonly maxEntries: number;

	/** Number of live (non-evicted) entries */
	private liveCount: number;

	/** Whether each index slot is live (1) or evicted/free (0) */
	private alive: Uint8Array;

	/**
	 * Two-clock eviction state.
	 * recentBit[i] = 1 if entry i was accessed since the last eviction sweep.
	 * On eviction: entries with recentBit=1 get a "second chance" (bit cleared);
	 * entries with recentBit=0 are evicted.
	 */
	private recentBit: Uint8Array;
	/** Clock hand: next index to check during eviction */
	private clockHand: number;

	/** Free indices from evicted entries, available for reuse */
	private freeIndices: number[];

	constructor(options?: DictionaryOptions | number) {
		const initialCapacity =
			typeof options === "number"
				? options
				: options?.initialCapacity ?? INITIAL_CAPACITY;
		this.maxEntries =
			typeof options === "number"
				? DEFAULT_MAX_ENTRIES
				: options?.maxEntries ?? DEFAULT_MAX_ENTRIES;

		// Ensure power of 2 for hash table
		this.hashTableSize = nextPowerOf2(initialCapacity);

		this.data = new Uint8Array(initialCapacity * 32);
		this.dataOffset = 0;

		this.offsets = new Uint32Array(initialCapacity * 2);
		this.count = 0;
		this.liveCount = 0;

		this.hashTable = new Int32Array(this.hashTableSize).fill(-1);
		this.chains = new Int32Array(initialCapacity).fill(-1);
		this.hashes = new Uint32Array(initialCapacity);
		this.alive = new Uint8Array(initialCapacity);
		this.recentBit = new Uint8Array(initialCapacity);
		this.clockHand = 0;
		this.freeIndices = [];

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
	 * When maxEntries is reached, evicts entries using the two-clock algorithm.
	 */
	intern(bytes: Uint8Array): DictIndex {
		if (bytes.length === 0) {
			return this.internEmpty();
		}

		const hash = this.hash(bytes);
		const slot = hash & (this.hashTableSize - 1);

		// Search chain for existing entry (only live entries match)
		let idx = this.hashTable[slot] ?? -1;
		while (idx !== -1) {
			if (this.alive[idx] && this.bytesEqual(idx, bytes)) {
				// Mark as recently used
				this.recentBit[idx] = 1;
				return idx;
			}
			idx = this.chains[idx] ?? -1;
		}

		// Evict if at capacity
		if (this.maxEntries > 0 && this.liveCount >= this.maxEntries) {
			this.evictClock();
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
	 * Returns undefined for evicted or out-of-range indices.
	 */
	getString(index: DictIndex): string | undefined {
		if (index >= this.count || !this.alive[index]) {
			return undefined;
		}
		this.recentBit[index] = 1;

		const cached = this.stringCache[index];
		if (cached !== undefined) return cached;

		const bytes = this.getBytes(index);
		if (bytes === undefined) return undefined;
		const str = this.decoder.decode(bytes);
		this.stringCache[index] = str;
		return str;
	}

	/**
	 * Get raw bytes by index (zero-copy view).
	 * Returns undefined for evicted or out-of-range indices.
	 */
	getBytes(index: DictIndex): Uint8Array | undefined {
		if (index >= this.count || !this.alive[index]) {
			return undefined;
		}
		// Mark as recently used
		this.recentBit[index] = 1;

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
		if (this.count > 0 && this.alive[0]) {
			const len = this.offsets[1] ?? 0;
			if (len === 0) {
				this.recentBit[0] = 1;
				return 0;
			}
		}

		if (this.count === 0) {
			this.offsets[0] = 0;
			this.offsets[1] = 0;
			this.alive[0] = 1;
			this.recentBit[0] = 1;
			this.count = 1;
			this.liveCount = 1;
			return 0;
		}

		if (this.maxEntries > 0 && this.liveCount >= this.maxEntries) {
			this.evictClock();
		}
		const emptyBytes = new Uint8Array(0);
		const hash = this.hash(emptyBytes);
		const slot = hash & (this.hashTableSize - 1);
		return this.addEntry(emptyBytes, hash, slot);
	}

	/** Add a new entry to the dictionary */
	private addEntry(bytes: Uint8Array, hash: number, slot: number): DictIndex {
		let index: number;
		if (this.freeIndices.length > 0) {
			index = this.freeIndices.pop()!;
		} else {
			if (this.count >= this.chains.length) {
				this.grow();
				slot = hash & (this.hashTableSize - 1);
			}
			index = this.count;
			this.count++;
		}

		if (this.dataOffset + bytes.length > this.data.length) {
			this.growData(bytes.length);
		}

		const offsetIdx = index * 2;
		if (offsetIdx + 1 >= this.offsets.length) {
			const newOffsets = new Uint32Array(this.offsets.length * 2);
			newOffsets.set(this.offsets);
			this.offsets = newOffsets;
		}
		this.offsets[offsetIdx] = this.dataOffset;
		this.offsets[offsetIdx + 1] = bytes.length;

		this.data.set(bytes, this.dataOffset);
		this.dataOffset += bytes.length;

		if (index >= this.hashes.length) {
			const newHashes = new Uint32Array(this.hashes.length * 2);
			newHashes.set(this.hashes);
			this.hashes = newHashes;
		}
		this.hashes[index] = hash;

		// Grow alive and recentBit if needed
		if (index >= this.alive.length) {
			const newAlive = new Uint8Array(this.alive.length * 2);
			newAlive.set(this.alive);
			this.alive = newAlive;
			const newRecent = new Uint8Array(this.recentBit.length * 2);
			newRecent.set(this.recentBit);
			this.recentBit = newRecent;
		}
		this.alive[index] = 1;
		this.recentBit[index] = 1;
		this.liveCount++;

		if (this.stringCache[index] !== undefined) {
			this.stringCache[index] = undefined as unknown as string;
		}

		if (index >= this.chains.length) {
			const newChains = new Int32Array(this.chains.length * 2).fill(-1);
			newChains.set(this.chains);
			this.chains = newChains;
		}
		this.chains[index] = this.hashTable[slot] ?? -1;
		this.hashTable[slot] = index;

		if (this.count > this.hashTableSize * LOAD_FACTOR) {
			this.rehash();
		}

		return index;
	}

	/**
	 * Two-clock eviction: O(capacity) worst case — evicts ~10% of maxEntries.
	 *
	 * Pass 1: For each live entry, if recentBit=1, clear it (second chance).
	 *         If recentBit=0, evict the entry.
	 * The clock hand advances forward, wrapping around. This approximates LRU
	 * without maintaining a sorted structure or scanning accessGen arrays.
	 */
	private evictClock(): void {
		const evictTarget = Math.max(1, Math.floor(this.maxEntries * 0.1));
		let evicted = 0;
		const n = this.count;

		// We may need up to two full sweeps to evict enough entries
		// (first sweep gives second chances, second sweep evicts them).
		for (let sweep = 0; sweep < 2 && evicted < evictTarget; sweep++) {
			for (let i = 0; i < n && evicted < evictTarget; i++) {
				const idx = this.clockHand % n;
				this.clockHand = (this.clockHand + 1) % n;

				if (!this.alive[idx]) continue;

				if (this.recentBit[idx]) {
					// Second chance: clear the bit and skip
					this.recentBit[idx] = 0;
				} else {
					// Not recently used — evict
					this.alive[idx] = 0;
					this.freeIndices.push(idx);
					if (this.stringCache[idx] !== undefined) {
						this.stringCache[idx] = undefined as unknown as string;
					}
					evicted++;
					this.liveCount--;
				}
			}
		}

		// Rebuild hash chains to remove evicted entries
		this.rebuildHashTable();
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
		const newSize = this.chains.length * 2;
		const newChains = new Int32Array(newSize).fill(-1);
		newChains.set(this.chains);
		this.chains = newChains;

		// Also grow alive and recentBit arrays
		if (newSize > this.alive.length) {
			const newAlive = new Uint8Array(newSize);
			newAlive.set(this.alive);
			this.alive = newAlive;
			const newRecent = new Uint8Array(newSize);
			newRecent.set(this.recentBit);
			this.recentBit = newRecent;
		}
	}

	/** Grow the data array */
	private growData(needed: number): void {
		const newSize = Math.max(this.data.length * 2, this.dataOffset + needed);
		const newData = new Uint8Array(newSize);
		newData.set(this.data);
		this.data = newData;
	}

	/** Rehash when load factor exceeded (doubles hash table size) */
	private rehash(): void {
		this.hashTableSize = nextPowerOf2(this.hashTableSize * 2);
		this.rebuildHashTable();
	}

	/** Rebuild hash table and chains without changing size (used after eviction) */
	private rebuildHashTable(): void {
		this.hashTable = new Int32Array(this.hashTableSize).fill(-1);

		// Grow chains if needed
		if (this.count > this.chains.length) {
			const newChains = new Int32Array(this.count * 2).fill(-1);
			this.chains = newChains;
		} else {
			this.chains.fill(-1);
		}

		// Re-insert only live entries using cached hashes
		const mask = this.hashTableSize - 1;
		for (let i = 0; i < this.count; i++) {
			if (!this.alive[i]) continue; // Skip evicted entries
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
