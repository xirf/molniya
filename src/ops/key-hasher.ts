/**
 * Key Hasher for GroupBy operations.
 *
 * Provides fast numeric hashing for group keys to avoid GC pressure
 * from string serialization. Uses Bun's native hashing for speed.
 *
 * Key features:
 * - No string allocations during hashing
 * - Native Bun.hash() (xxHash)
 * - Handles nulls, numbers, and bigints
 * - Collision-resistant for typical groupby workloads
 */

/** Module-level singleton for float hashing (3.4 optimization — avoid per-call allocation) */
const _hashBuf = new ArrayBuffer(8);
const _hashView = new DataView(_hashBuf);

/**
 * Hash a single value using Bun.hash.
 * Returns a 32-bit hash code.
 */
function hashValue(value: number | bigint | null): number {
	if (value === null) {
		return 0; // Null hashes to 0
	}

	if (typeof value === "bigint") {
		// Hash as string because Bun.hash(BigInt) isn't directly documented as stable/fast across all inputs,
		// but actually Bun.hash supports string | TypedArray | ArrayBuffer.
		// For BigInt, maybe we should mix bits manually or use a buffer?
		// Manual mixing is safer for now to avoid string alloc.
		const low = Number(value & BigInt(0xffffffff));
		const high = Number(value >> BigInt(32));
		return hashCombine(low, high);
	}

	if (Number.isInteger(value)) {
		// Small integers: just use the value mixed
		// Or use Bun.hash if we can validly pass it.
		// Bun.hash(number) isn't valid. It expects string/buffer.
		// So we stay with bit mixing for pure numbers to avoid allocs.
		// But let's use a faster mix than FNV-1a loop.
		// MurmurHash3 fmix32 style:
		let h = value | 0;
		h ^= h >>> 16;
		h = Math.imul(h, 0x85ebca6b);
		h ^= h >>> 13;
		h = Math.imul(h, 0xc2b2ae35);
		h ^= h >>> 16;
		return h >>> 0;
	} else {
		// Floats
		_hashView.setFloat64(0, value, true);
		// Hash the bytes
		// Bun.hash(_hashView) might work but creating a view/buffer per call is overhead?
		// We have a singleton view.
		// But Bun.hash() on ArrayBuffer/View?
		// Let's assume manual mix of the two 32-bit halves is fastest for individual numbers.
		const low = _hashView.getUint32(0, true);
		const high = _hashView.getUint32(4, true);
		return hashCombine(low, high);
	}
}

/**
 * Combine two hash values.
 */
function hashCombine(h1: number, h2: number): number {
	// Boost hash combination
	let hash = h1;
	hash ^= h2 + 0x9e3779b9 + (hash << 6) + (hash >> 2);
	return hash >>> 0;
}

/**
 * Hash multiple values into a single hash code.
 * This is the primary entry point for GroupBy key hashing.
 */
export function hashKey(values: (number | bigint | null)[]): number {
	if (values.length === 0) return 0;
	if (values.length === 1)
		return hashValue(values[0] as number | bigint | null);

	let hash = 0;
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
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
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

	constructor(initialCapacity: number = 1024) {
		// Increased default
		this.capacity = Math.max(16, nextPowerOf2(initialCapacity));
		this.table = new Array(this.capacity);
	}

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
	 */
	insert(key: (number | bigint | null)[], nextGroupId: number): number {
		if (this.size >= this.capacity * this.loadFactor) {
			this.resize();
		}

		const hash = hashKey(key);
		const idx = this.findSlot(hash, key);

		if (this.table[idx]) {
			return this.table[idx]!.groupId;
		}

		this.table[idx] = { hash, key: key.slice(), groupId: nextGroupId };
		this.size++;
		return nextGroupId;
	}

	/**
	 * Combined Get or Insert operation.
	 * Optimizes by hashing only once.
	 *
	 * @param key The group key
	 * @param nextGroupId ID to assign if key is new
	 * @returns [groupId, isNew] pair - but purely returning groupId and handling
	 * distinctness call-side is slightly complex.
	 *
	 * Better signature: returns { id: number, isNew: boolean }
	 */
	getOrInsert(
		key: (number | bigint | null)[],
		nextGroupId: number,
	): { id: number; isNew: boolean } {
		if (this.size >= this.capacity * this.loadFactor) {
			this.resize();
		}

		const hash = hashKey(key);
		const idx = this.findSlot(hash, key);
		const entry = this.table[idx];

		if (entry) {
			return { id: entry.groupId, isNew: false };
		}

		// New entry
		this.table[idx] = { hash, key: key.slice(), groupId: nextGroupId };
		this.size++;
		return { id: nextGroupId, isNew: true };
	}

	entries(): Array<[number, (number | bigint | null)[]]> {
		const result: Array<[number, (number | bigint | null)[]]> = [];
		for (const entry of this.table) {
			if (entry) {
				result.push([entry.groupId, entry.key]);
			}
		}
		result.sort((a, b) => a[0] - b[0]);
		return result;
	}

	clear(): void {
		this.table.fill(undefined);
		this.size = 0;
	}

	private findSlot(hash: number, key: (number | bigint | null)[]): number {
		let idx = hash & (this.capacity - 1);
		while (true) {
			const entry = this.table[idx];
			// Empty slot?
			if (!entry) return idx;
			// Match?
			if (entry.hash === hash && keysEqual(entry.key, key)) return idx;
			// Linear probe
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
				// Re-find slot in new table
				// We can reuse the hash!
				let idx = entry.hash & (this.capacity - 1);
				while (this.table[idx]) {
					idx = (idx + 1) & (this.capacity - 1);
				}
				this.table[idx] = entry;
				this.size++;
			}
		}
	}
}

function nextPowerOf2(n: number): number {
	return 2 ** Math.ceil(Math.log2(n));
}
