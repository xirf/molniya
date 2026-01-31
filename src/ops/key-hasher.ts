/**
 * Key Hasher for GroupBy operations.
 *
 * Provides fast numeric hashing for group keys to avoid GC pressure
 * from string serialization. Uses FNV-1a hash algorithm adapted for
 * mixed numeric types.
 *
 * Key features:
 * - No string allocations during hashing
 * - O(1) hash computation
 * - Handles nulls, numbers, and bigints
 * - Collision-resistant for typical groupby workloads
 */

/** FNV-1a hash constants */
const FNV_OFFSET_BASIS = 2166136261;
const FNV_PRIME = 16777619;

/**
 * Hash a single value using FNV-1a.
 * Returns a 32-bit hash code.
 */
function hashValue(value: number | bigint | null): number {
	if (value === null) {
		return 0; // Null hashes to 0
	}

	if (typeof value === "bigint") {
		// Hash bigint by combining high and low 32 bits
		const low = Number(value & BigInt(0xffffffff));
		const high = Number(value >> BigInt(32));
		return hashCombine(hashValue(low), high);
	}

	// Hash number using FNV-1a on its 32-bit representation
	// Use a simple but effective hash for numbers
	let hash = FNV_OFFSET_BASIS;

	// For integer values, use direct hashing
	if (Number.isInteger(value)) {
		// Mix the bits
		const v = value | 0; // Convert to 32-bit signed integer
		hash ^= (v >>> 0) & 0xff;
		hash = Math.imul(hash, FNV_PRIME);
		hash ^= (v >>> 8) & 0xff;
		hash = Math.imul(hash, FNV_PRIME);
		hash ^= (v >>> 16) & 0xff;
		hash = Math.imul(hash, FNV_PRIME);
		hash ^= (v >>> 24) & 0xff;
		hash = Math.imul(hash, FNV_PRIME);
	} else {
		// For floats, use a simpler hash based on the bit pattern
		const buffer = new ArrayBuffer(8);
		const view = new DataView(buffer);
		view.setFloat64(0, value, true); // little-endian
		const low = view.getUint32(0, true);
		const high = view.getUint32(4, true);
		hash = hashCombine(hashValue(low), high);
	}

	return hash >>> 0; // Ensure unsigned 32-bit
}

/**
 * Combine two hash values.
 */
function hashCombine(h1: number, h2: number): number {
	// Use a simple but effective combination
	let hash = h1;
	hash ^= h2 + 0x9e3779b9 + (hash << 6) + (hash >> 2);
	return hash >>> 0;
}

/**
 * Hash multiple values into a single hash code.
 * This is the primary entry point for GroupBy key hashing.
 */
export function hashKey(values: (number | bigint | null)[]): number {
	if (values.length === 0) {
		return 0;
	}

	if (values.length === 1) {
		return hashValue(values[0] as number | bigint | null);
	}

	// Combine hashes of all values
	let hash = FNV_OFFSET_BASIS;
	for (const value of values) {
		hash = hashCombine(hash, hashValue(value));
	}

	return hash >>> 0;
}

/**
 * Check if two keys are equal.
 */
export function keysEqual(
	a: (number | bigint | null)[],
	b: (number | bigint | null)[],
): boolean {
	if (a.length !== b.length) {
		return false;
	}

	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) {
			return false;
		}
	}

	return true;
}

/**
 * Entry in the hash table.
 */
interface HashEntry {
	hash: number;
	key: (number | bigint | null)[];
	groupId: number;
}

/**
 * Hash table for group key lookup.
 * Uses open addressing with linear probing.
 */
export class KeyHashTable {
	private table: (HashEntry | undefined)[];
	private size: number = 0;
	private capacity: number;
	private readonly loadFactor = 0.75;

	constructor(initialCapacity: number = 16) {
		this.capacity = Math.max(16, nextPowerOf2(initialCapacity));
		this.table = new Array(this.capacity);
	}

	/**
	 * Get the number of entries in the table.
	 */
	getSize(): number {
		return this.size;
	}

	/**
	 * Look up a key and return its group ID, or -1 if not found.
	 */
	get(key: (number | bigint | null)[]): number {
		const hash = hashKey(key);
		const idx = this.findSlot(hash, key);
		const entry = this.table[idx];
		return entry ? entry.groupId : -1;
	}

	/**
	 * Insert a key and return its group ID.
	 * If the key already exists, returns the existing group ID.
	 * Otherwise, assigns a new group ID.
	 */
	insert(key: (number | bigint | null)[], nextGroupId: number): number {
		if (this.size >= this.capacity * this.loadFactor) {
			this.resize();
		}

		const hash = hashKey(key);
		const idx = this.findSlot(hash, key);

		if (this.table[idx]) {
			// Key already exists
			return this.table[idx]?.groupId;
		}

		// Insert new entry
		this.table[idx] = { hash, key: key.slice(), groupId: nextGroupId };
		this.size++;
		return nextGroupId;
	}

	/**
	 * Get all entries as [groupId, key] pairs.
	 */
	entries(): Array<[number, (number | bigint | null)[]]> {
		const result: Array<[number, (number | bigint | null)[]]> = [];
		for (const entry of this.table) {
			if (entry) {
				result.push([entry.groupId, entry.key]);
			}
		}
		// Sort by groupId for deterministic output
		result.sort((a, b) => a[0] - b[0]);
		return result;
	}

	/**
	 * Clear all entries.
	 */
	clear(): void {
		this.table.fill(undefined);
		this.size = 0;
	}

	private findSlot(hash: number, key: (number | bigint | null)[]): number {
		let idx = hash & (this.capacity - 1);

		while (true) {
			const entry = this.table[idx];
			if (!entry) {
				return idx; // Empty slot
			}
			if (entry.hash === hash && keysEqual(entry.key, key)) {
				return idx; // Found matching key
			}
			// Linear probing
			idx = (idx + 1) & (this.capacity - 1);
		}
	}

	private resize(): void {
		const oldTable = this.table;
		this.capacity *= 2;
		this.table = new Array(this.capacity);
		this.size = 0;

		for (const entry of oldTable) {
			if (entry) {
				const idx = this.findSlot(entry.hash, entry.key);
				this.table[idx] = entry;
				this.size++;
			}
		}
	}
}

/**
 * Get the next power of 2 greater than or equal to n.
 */
function nextPowerOf2(n: number): number {
	return 2 ** Math.ceil(Math.log2(n));
}
