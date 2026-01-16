/**
 * LRU Chunk Cache - Memory-efficient caching for parsed row data.
 *
 * Caches parsed row chunks with configurable memory budget.
 * Automatically evicts least-recently-used chunks when budget exceeded.
 */

/** Parsed row data for a chunk */
export interface ChunkData<T = unknown> {
  /** Starting row index of this chunk */
  startRow: number;
  /** Parsed row objects */
  rows: T[];
  /** Approximate memory size in bytes */
  sizeBytes: number;
}

/** Cache configuration */
export interface CacheConfig {
  /** Maximum memory budget in bytes (default: 100MB) */
  maxMemoryBytes: number;
  /** Number of rows per chunk (default: 10,000) */
  chunkSize: number;
}

const DEFAULT_CONFIG: CacheConfig = {
  maxMemoryBytes: 100 * 1024 * 1024, // 100MB
  chunkSize: 10_000,
};

/**
 * LRU cache for parsed row chunks.
 * Provides O(1) access and automatic eviction.
 */
export class ChunkCache<T = unknown> {
  private readonly _config: CacheConfig;

  /** Map of chunkIndex -> ChunkData */
  private readonly _cache: Map<number, ChunkData<T>>;

  /** LRU order - most recent at end */
  private readonly _lruOrder: number[];

  /** Current memory usage in bytes */
  private _memoryUsed: number;

  constructor(config: Partial<CacheConfig> = {}) {
    this._config = { ...DEFAULT_CONFIG, ...config };
    this._cache = new Map();
    this._lruOrder = [];
    this._memoryUsed = 0;
  }

  /**
   * Get chunk size configuration.
   */
  get chunkSize(): number {
    return this._config.chunkSize;
  }

  /**
   * Get current memory usage in bytes.
   */
  get memoryUsed(): number {
    return this._memoryUsed;
  }

  /**
   * Get number of cached chunks.
   */
  get size(): number {
    return this._cache.size;
  }

  /**
   * Calculate chunk index for a given row.
   */
  getChunkIndex(rowIndex: number): number {
    return Math.floor(rowIndex / this._config.chunkSize);
  }

  /**
   * Get row range for a chunk index.
   * @returns [startRow, endRow) - endRow is exclusive
   */
  getChunkRange(chunkIndex: number): [number, number] {
    const start = chunkIndex * this._config.chunkSize;
    const end = start + this._config.chunkSize;
    return [start, end];
  }

  /**
   * Check if a chunk is cached.
   */
  has(chunkIndex: number): boolean {
    return this._cache.has(chunkIndex);
  }

  /**
   * Get a cached chunk, marking it as recently used.
   * Returns undefined if not cached.
   */
  get(chunkIndex: number): ChunkData<T> | undefined {
    const chunk = this._cache.get(chunkIndex);
    if (chunk) {
      this._markUsed(chunkIndex);
    }
    return chunk;
  }

  /**
   * Store a chunk in cache, evicting old chunks if needed.
   */
  set(chunkIndex: number, chunk: ChunkData<T>): void {
    // If already exists, remove old size from memory count
    const existing = this._cache.get(chunkIndex);
    if (existing) {
      this._memoryUsed -= existing.sizeBytes;
    }

    // Evict until we have room
    while (
      this._memoryUsed + chunk.sizeBytes > this._config.maxMemoryBytes &&
      this._lruOrder.length > 0
    ) {
      this._evictLru();
    }

    // Store chunk
    this._cache.set(chunkIndex, chunk);
    this._memoryUsed += chunk.sizeBytes;
    this._markUsed(chunkIndex);
  }

  /**
   * Clear all cached chunks.
   */
  clear(): void {
    this._cache.clear();
    this._lruOrder.length = 0;
    this._memoryUsed = 0;
  }

  /**
   * Get rows from cache if available.
   * @param startRow - Starting row index
   * @param count - Number of rows to retrieve
   * @returns Array of [rowIndex, rowData] or undefined entries for uncached rows
   */
  getRows(startRow: number, count: number): (T | undefined)[] {
    const results: (T | undefined)[] = new Array(count);

    for (let i = 0; i < count; i++) {
      const rowIndex = startRow + i;
      const chunkIndex = this.getChunkIndex(rowIndex);
      const chunk = this.get(chunkIndex);

      if (chunk) {
        const offsetInChunk = rowIndex - chunk.startRow;
        results[i] = chunk.rows[offsetInChunk];
      } else {
        results[i] = undefined;
      }
    }

    return results;
  }

  /**
   * Mark a chunk as recently used (move to end of LRU list).
   */
  private _markUsed(chunkIndex: number): void {
    const idx = this._lruOrder.indexOf(chunkIndex);
    if (idx !== -1) {
      this._lruOrder.splice(idx, 1);
    }
    this._lruOrder.push(chunkIndex);
  }

  /**
   * Evict the least recently used chunk.
   */
  private _evictLru(): void {
    const oldest = this._lruOrder.shift();
    if (oldest !== undefined) {
      const chunk = this._cache.get(oldest);
      if (chunk) {
        this._memoryUsed -= chunk.sizeBytes;
        this._cache.delete(oldest);
      }
    }
  }

  /**
   * Estimate memory size of a row array.
   * Rough estimate: ~100 bytes per field for strings, 8 bytes for numbers.
   */
  static estimateSize<R>(rows: R[], fieldCount: number): number {
    if (rows.length === 0) return 0;

    // Sample first row to estimate
    const sample = rows[0];
    let rowSize = 0;

    if (sample && typeof sample === 'object') {
      for (const value of Object.values(sample)) {
        if (typeof value === 'string') {
          rowSize += 50 + value.length * 2; // String overhead + chars
        } else if (typeof value === 'number') {
          rowSize += 8; // Float64
        } else if (typeof value === 'boolean') {
          rowSize += 4;
        } else {
          rowSize += 20; // Default overhead
        }
      }
    } else {
      rowSize = fieldCount * 50; // Fallback estimate
    }

    return rows.length * rowSize;
  }
}
