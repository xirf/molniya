/** biome-ignore-all lint/style/noNonNullAssertion: Performance critical inner loops */
/**
 * Key Hasher for GroupBy operations.
 *
 * Uses flat TypedArrays instead of JS objects per entry to avoid GC pressure.
 * A typical JS object (HashEntry) costs ~80 bytes; using parallel Uint32Array/
 * Int32Array slabs reduces per-group memory to ~8 bytes for the hot table data.
 *
 * Layout of the flat table:
 *   - slots: Int32Array of length capacity  (groupId per slot, -1 = empty)
 *   - hashes: Uint32Array of length capacity (cached hash per slot)
 *   - keystore: Array of key arrays (only populated for occupied slots)
 *
 * Key features:
 * - No per-entry JS object allocation
 * - O(1) hash computation
 * - Handles nulls, numbers, and bigints
 * - Collision-resistant for typical groupby workloads
 */

/** FNV-1a hash constants */
const FNV_OFFSET_BASIS = 2166136261;
const FNV_PRIME = 16777619;

/** Module-level singleton for float hashing (avoid per-call allocation) */
const _hashBuf = new ArrayBuffer(8);
const _hashView = new DataView(_hashBuf);

/**
 * Hash a single value using FNV-1a.
 * Returns a 32-bit hash code.
 */
function hashValue(value: number | bigint | Uint8Array | null): number {
	if (value === null) {
		return 0; // Null hashes to 0
	}

	if (value instanceof Uint8Array) {
		let hash = FNV_OFFSET_BASIS;
		for (let i = 0; i < value.length; i++) {
			hash ^= value[i]!;
			hash = Math.imul(hash, FNV_PRIME);
		}
		return hash >>> 0;
	}

	if (typeof value === "bigint") {
		// Hash bigint by combining high and low 32 bits
		const low = Number(value & BigInt(0xffffffff));
		const high = Number(value >> BigInt(32));
		return hashCombine(hashValue(low), high);
	}

	let hash = FNV_OFFSET_BASIS;

	if (Number.isInteger(value)) {
		const v = value | 0;
		hash ^= (v >>> 0) & 0xff;
		hash = Math.imul(hash, FNV_PRIME);
		hash ^= (v >>> 8) & 0xff;
		hash = Math.imul(hash, FNV_PRIME);
		hash ^= (v >>> 16) & 0xff;
		hash = Math.imul(hash, FNV_PRIME);
		hash ^= (v >>> 24) & 0xff;
		hash = Math.imul(hash, FNV_PRIME);
	} else {
		_hashView.setFloat64(0, value, true);
		const low = _hashView.getUint32(0, true);
		const high = _hashView.getUint32(4, true);
		hash = hashCombine(hashValue(low), high);
	}

	return hash >>> 0;
}

/**
 * Combine two hash values.
 */
function hashCombine(h1: number, h2: number): number {
	let hash = h1;
	hash ^= h2 + 0x9e3779b9 + (hash << 6) + (hash >> 2);
	return hash >>> 0;
}

/**
 * Hash multiple values into a single hash code.
 */
export function hashKey(values: (number | bigint | Uint8Array | null)[]): number {
	if (values.length === 0) return 0;
	if (values.length === 1) return hashValue(values[0] as number | bigint | Uint8Array | null);

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
	a: (number | bigint | Uint8Array | null)[],
	b: (number | bigint | Uint8Array | null)[],
): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		const va = a[i];
		const vb = b[i];
		if (va instanceof Uint8Array && vb instanceof Uint8Array) {
			if (va.length !== vb.length) return false;
			for (let j = 0; j < va.length; j++) {
				if (va[j] !== vb[j]) return false;
			}
		} else if (va as any !== vb as any) {
			return false;
		}
	}
	return true;
}

/** Sentinel for an empty slot */
const EMPTY = -1;

/**
 * Hash table for group key lookup.
 *
 * Uses flat TypedArrays (Uint32Array for hashes, Int32Array for groupIds)
 * instead of one JS object per slot, cutting per-group memory by ~10×.
 * Collision resolution: open addressing with linear probing.
 */
export class KeyHashTable {
	/** Cached hash per slot (Uint32, 0 = empty-ish but we check groupIds too) */
	private hashes: Uint32Array;
	/** Group ID per slot (-1 = empty) */
	private groupIds: Int32Array;

	private size = 0;
	private capacity: number;
	private readonly loadFactor = 0.75;

	constructor(initialCapacity = 16) {
		this.capacity = Math.max(16, nextPowerOf2(initialCapacity));
		this.hashes = new Uint32Array(this.capacity);
		this.groupIds = new Int32Array(this.capacity).fill(EMPTY);
	}

	/**
	 * Get the number of entries in the table.
	 */
	getSize(): number {
		return this.size;
	}

	/**
	 * Look up a key or insert it.
	 * 
	 * @param hash The 32-bit hash of the key
	 * @param keysEqual A function that returns true if the current key equals the key at the given group ID
	 * @param insertIfMissing If true, associates the key with `nextGroupId` and returns it.
	 * @param nextGroupId The group ID to use if the key is inserted.
	 * @returns The group ID of the key, or -1 if not found and `insertIfMissing` is false.
	 */
	getOrInsert(
		hash: number,
		keysEqual: (gid: number) => boolean,
		insertIfMissing: boolean,
		nextGroupId: number,
	): number {
		if (insertIfMissing && this.size >= this.capacity * this.loadFactor) {
			this.resize();
		}

		const cap = this.capacity;
		let idx = hash & (cap - 1);

		while (true) {
			const gid = this.groupIds[idx]!;
			if (gid === EMPTY) {
				if (insertIfMissing) {
					this.hashes[idx] = hash;
					this.groupIds[idx] = nextGroupId;
					this.size++;
					return nextGroupId;
				}
				return -1;
			}
			if (this.hashes[idx] === hash && keysEqual(gid)) {
				return gid;
			}
			idx = (idx + 1) & (cap - 1); // linear probing
		}
	}

	/**
	 * Clear all entries (reuse existing arrays without realloc).
	 */
	clear(): void {
		this.groupIds.fill(EMPTY);
		this.hashes.fill(0);
		this.size = 0;
	}

	private resize(): void {
		const oldGroupIds = this.groupIds;
		const oldHashes = this.hashes;
		const oldCap = this.capacity;

		this.capacity *= 2;
		this.hashes = new Uint32Array(this.capacity);
		this.groupIds = new Int32Array(this.capacity).fill(EMPTY);
		this.size = 0;

		const cap = this.capacity;

		for (let i = 0; i < oldCap; i++) {
			const gid = oldGroupIds[i]!;
			if (gid !== EMPTY) {
				const hash = oldHashes[i]!;
				let idx = hash & (cap - 1);

				// Find an empty slot
				while (this.groupIds[idx] !== EMPTY) {
					idx = (idx + 1) & (cap - 1);
				}

				this.hashes[idx] = hash;
				this.groupIds[idx] = gid;
				this.size++;
			}
		}
	}
}

/**
 * Get the next power of 2 >= n.
 */
function nextPowerOf2(n: number): number {
	return 2 ** Math.ceil(Math.log2(Math.max(n, 1)));
}
