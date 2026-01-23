import { DataFrame } from '../dataframe';
import type { Series } from '../series';
import type { DTypeKind, InferSchema, Schema } from '../types';
import { type BinaryChunk, ChunkCache, type Vector } from './chunk-cache';
import { BinaryGroupBy } from './groupby-binary';
import type { AggDef, ILazyFrame, LazyFrameConfig, LazyFrameResult } from './interface';
import { batof, batoi, parseChunkBytes } from './parser';
import { RowIndex } from './row-index';
import { LazyFrameColumnView } from './view';

/**
 * LazyFrame - Memory-efficient DataFrame for large CSV files.
 *
 * Keeps data on disk and loads rows on-demand using a chunked LRU cache.
 * Suitable for files that exceed available RAM.
 */
export class LazyFrame<S extends Schema> implements ILazyFrame<S> {
  readonly schema: S;
  readonly shape: readonly [rows: number, cols: number];
  readonly path: string;

  private readonly _file: ReturnType<typeof Bun.file>;
  private readonly _rowIndex: RowIndex;
  private readonly _columnOrder: (keyof S)[];
  private readonly _cache: ChunkCache;
  private readonly _hasHeader: boolean;
  private readonly _delimiter: string;
  private readonly _config: LazyFrameConfig;

  /**
   * Private constructor - use scanCsv() factory function instead.
   */
  private constructor(
    path: string,
    file: ReturnType<typeof Bun.file>,
    schema: S,
    columnOrder: (keyof S)[],
    rowIndex: RowIndex,
    config: LazyFrameConfig,
    hasHeader: boolean,
    delimiter: string,
  ) {
    this.path = path;
    this._file = file;
    this.schema = schema;
    this._columnOrder = columnOrder;
    this._rowIndex = rowIndex;
    this.shape = [rowIndex.rowCount, columnOrder.length] as const;
    this._config = config;
    this._cache = new ChunkCache({
      maxMemoryBytes: config.maxCacheMemory ?? 100 * 1024 * 1024,
      chunkSize: config.chunkSize ?? 10_000,
    });
    this._hasHeader = hasHeader;
    this._delimiter = delimiter;
  }

  /**
   * Create a LazyFrame from file path - internal factory method.
   */
  static async _create<S extends Schema>(
    path: string,
    schema: S,
    columnOrder: (keyof S)[],
    config: LazyFrameConfig = {},
    hasHeader = true,
    delimiter = ',',
  ): Promise<LazyFrame<S>> {
    const file = Bun.file(path);
    const rowIndex = await RowIndex.build(file, hasHeader);

    return new LazyFrame<S>(
      path,
      file,
      schema,
      columnOrder,
      rowIndex,
      config,
      hasHeader,
      delimiter,
    );
  }

  // Column Access
  // ===============================================================

  /**
   * Get a column as Series (loads all data for that column).
   */
  async col<K extends keyof S>(name: K): Promise<Series<S[K]['kind']>> {
    const result = await this.collect();
    if (result.memoryError) throw result.memoryError;
    return result.data!.col(name);
  }

  /**
   * Get column names in order.
   */
  columns(): (keyof S)[] {
    return [...this._columnOrder];
  }

  // Row Operations
  // ===============================================================

  /**
   * Get first n rows as DataFrame.
   */
  async head(n = 5): Promise<DataFrame<S>> {
    const count = Math.min(n, this.shape[0]);
    const cols = await this._loadColumns(0, count);
    return this._toDataFrame(cols, count);
  }

  /**
   * Get last n rows as DataFrame.
   */
  async tail(n = 5): Promise<DataFrame<S>> {
    const count = Math.min(n, this.shape[0]);
    const startRow = this.shape[0] - count;
    const cols = await this._loadColumns(startRow, count);
    return this._toDataFrame(cols, count);
  }

  /**
   * Select specific columns (returns new LazyFrame).
   */
  select<K extends keyof S>(...cols: K[]): ILazyFrame<Pick<S, K>> {
    const newSchema = {} as Pick<S, K>;
    for (const colName of cols) {
      newSchema[colName] = this.schema[colName];
    }
    return new LazyFrameColumnView<S, K>(this, cols, newSchema);
  }

  /**
   * Count rows matching a predicate.
   */
  async count(predicate?: (row: InferSchema<S>, index: number) => boolean): Promise<number> {
    if (!predicate) return this.shape[0];

    let matchCount = 0;
    const chunkSize = this._cache.chunkSize;

    for (let startRow = 0; startRow < this.shape[0]; startRow += chunkSize) {
      // Use getChunk loop logic to avoid materializing columns
      const chunkIdx = this._cache.getChunkIndex(startRow);
      let chunk = this._cache.get(chunkIdx);

      if (!chunk) {
        chunk = await this._loadChunk(chunkIdx);
        this._cache.set(chunkIdx, chunk);
      }

      const chunkStartRow = chunkIdx * chunkSize;
      const localStart = Math.max(0, startRow - chunkStartRow);
      const count = Math.min(chunkSize, this.shape[0] - startRow);

      // Resolve vectors for columns we need access to?
      // predicate might need all columns.
      // We pass a proxy "row" object to the predicate?
      // Or we just decode values on demand.

      for (let i = 0; i < count; i++) {
        const rowIndex = localStart + i;

        // Create a lazy row proxy or simple object?
        // For performance, maybe simple object if predicate is complex.
        // But zero-copy means we read directly.
        // Let's manually construct 'row' object from vectors.

        // biome-ignore lint/suspicious/noExplicitAny: <explanation>
        const row = {} as any;
        for (let c = 0; c < this._columnOrder.length; c++) {
          const colName = this._columnOrder[c] as string;
          const vector = chunk.columns[c]!;
          row[colName] = this._readVectorValue(vector, rowIndex);
        }

        if (predicate(row, startRow + i)) {
          matchCount++;
        }
      }

      this.clearCache();
      if (this._config.forceGc && typeof Bun !== 'undefined' && Bun.gc) {
        // @ts-ignore
        Bun.gc(true);
      }
    }

    return matchCount;
  }

  /**
   * Filter rows by predicate function.
   */
  async filter(fn: (row: InferSchema<S>, index: number) => boolean): Promise<LazyFrameResult<S>> {
    const matchingRows: InferSchema<S>[] = [];
    const chunkSize = this._cache.chunkSize;

    for (let startRow = 0; startRow < this.shape[0]; startRow += chunkSize) {
      const chunkIdx = this._cache.getChunkIndex(startRow);
      let chunk = this._cache.get(chunkIdx);

      if (!chunk) {
        chunk = await this._loadChunk(chunkIdx);
        this._cache.set(chunkIdx, chunk);
      }

      const chunkStartRow = chunkIdx * chunkSize;
      const localStart = Math.max(0, startRow - chunkStartRow);
      const count = Math.min(chunkSize, this.shape[0] - startRow);

      for (let i = 0; i < count; i++) {
        const rowIndex = localStart + i;
        // biome-ignore lint/suspicious/noExplicitAny: <explanation>
        const row = {} as any;
        for (let c = 0; c < this._columnOrder.length; c++) {
          const colName = this._columnOrder[c] as string;
          const vector = chunk.columns[c]!;
          row[colName] = this._readVectorValue(vector, rowIndex);
        }

        if (fn(row, startRow + i)) {
          matchingRows.push(row);
        }
      }

      if (startRow % (chunkSize * 10) === 0) {
        this.clearCache();
        if (this._config.forceGc && typeof Bun !== 'undefined' && Bun.gc) {
          // @ts-ignore
          Bun.gc(true);
        }
      }
    }

    const df = DataFrame.from(this.schema, matchingRows);
    this.clearCache();
    return { data: df };
  }

  /**
   * Collect all rows.
   */
  async collect(limit?: number): Promise<LazyFrameResult<S>> {
    const count = limit !== undefined ? Math.min(limit, this.shape[0]) : this.shape[0];
    const cols = await this._loadColumns(0, count);

    // Estimate size of materialized columns
    let estimatedSize = 0;
    for (const values of Object.values(cols)) {
      if (values.length > 0) {
        const sample = values[0];
        if (typeof sample === 'string') {
          estimatedSize += count * 50;
        } else {
          estimatedSize += count * 8;
        }
      }
    }
    const allocation = this._cache.checkAllocation(estimatedSize);

    const df = this._toDataFrame(cols, count);
    this.clearCache();

    return {
      data: df,
      memoryError: allocation.success ? undefined : allocation.error,
    };
  }

  /**
   * Get info about the LazyFrame.
   */
  info(): { rows: number; columns: number; dtypes: Record<string, string>; cached: number } {
    const dtypes: Record<string, string> = {};
    for (const colName of this._columnOrder) {
      const dtype = this.schema[colName];
      if (dtype) dtypes[colName as string] = dtype.kind;
    }

    return {
      rows: this.shape[0],
      columns: this.shape[1],
      dtypes,
      cached: this._cache.size,
    };
  }

  /**
   * Print sample of data to console.
   */
  async print(): Promise<void> {
    const df = await this.head(10);
    console.log(`LazyFrame [${this.path}]`);
    df.print();
    if (this.shape[0] > 10) {
      console.log(`... ${this.shape[0] - 10} more rows`);
    }
  }

  clearCache(): void {
    this._cache.clear();
  }

  destroy(): void {
    this._cache.destroy();
  }

  // Internal: Column Loading
  // ===============================================================

  /**
   * Load columns from file (with caching).
   */
  private async _loadColumns(startRow: number, count: number): Promise<Record<string, unknown[]>> {
    const colNames = this._columnOrder as string[];
    const results: Record<string, unknown[]> = {};
    for (const name of colNames) {
      results[name] = new Array(count);
    }

    const chunkSize = this._cache.chunkSize;
    const startChunk = this._cache.getChunkIndex(startRow);
    const endChunk = this._cache.getChunkIndex(startRow + count - 1);

    for (let chunkIdx = startChunk; chunkIdx <= endChunk; chunkIdx++) {
      let chunk = this._cache.get(chunkIdx);

      if (!chunk) {
        chunk = await this._loadChunk(chunkIdx);
        this._cache.set(chunkIdx, chunk);
      }

      const chunkStartRow = chunkIdx * chunkSize;
      const globalStart = Math.max(startRow, chunkStartRow);
      const globalEnd = Math.min(startRow + count, chunkStartRow + chunk.rowCount);
      const length = globalEnd - globalStart;

      if (length <= 0) continue;

      const srcStart = globalStart - chunkStartRow;
      const destStart = globalStart - startRow;

      for (let c = 0; c < colNames.length; c++) {
        const colName = colNames[c]!;
        const vector = chunk.columns[c]!;
        const destArray = results[colName]!;

        // Fill destination array
        // We iterate and decode value-by-value for now.
        // Optimization: Use typed array `.set` for numerics if destArray is typed.
        // But results[colName] is initialized as Array(count) in generic case?
        // Let's check init: "results[name] = new Array(count);"
        // Ideally we should pre-allocate typed arrays if schema says so.
        // But for compatibility with existing code that expects Record<string, unknown[]>,
        // we can just fill it.

        for (let i = 0; i < length; i++) {
          destArray[destStart + i] = this._readVectorValue(vector, srcStart + i);
        }
      }
    }

    return results;
  }

  /**
   * Async Iterator - Yields chunks of DataFrames (Zero Copy!)
   */
  async *[Symbol.asyncIterator](): AsyncIterator<DataFrame<S>> {
    const chunkSize = this._cache.chunkSize;

    for (let startRow = 0; startRow < this.shape[0]; startRow += chunkSize) {
      const chunkIdx = this._cache.getChunkIndex(startRow);
      let chunk = this._cache.get(chunkIdx);

      if (!chunk) {
        chunk = await this._loadChunk(chunkIdx);
        this._cache.set(chunkIdx, chunk);
      }

      // Create DataFrame using existing schema (Zero Copy logic applied in loadChunk/toDataFrame construction effectively)
      // Create DataFrame using existing schema (Zero Copy logic applied in loadChunk/toDataFrame construction effectively)
      // Note: _toDataFrame expects Record, but we can update it or map chunk.columns (Vector[]) to Record.
      // But _toDataFrame was designed to take Record.
      // We should update _toDataFrame to take Vector[].
      // For now, let's map it to Record of Vectors (or decoded?)
      // Yield expects fully materialised DataFrame traditionally.
      // If we yield a wrapper that stays lazy, that's better?
      // But DataFrame is eager.
      // So we must decode.

      const dfData: Record<string, unknown[]> = {};
      for (let i = 0; i < this._columnOrder.length; i++) {
        const colName = this._columnOrder[i] as string;
        const vector = chunk.columns[i]!;
        // Decode entire vector to array
        const arr = new Array(chunk.rowCount);
        for (let r = 0; r < chunk.rowCount; r++) {
          arr[r] = this._readVectorValue(vector, r);
        }
        dfData[colName] = arr;
      }

      const df = this._toDataFrame(dfData, chunk.rowCount);

      yield df;

      this.clearCache();

      if (this._config.forceGc && typeof Bun !== 'undefined' && Bun.gc) {
        // @ts-ignore
        Bun.gc(true);
      }
    }
  }

  /**
   * Group by keys and aggregate (Binary-Optimized).
   */
  async groupby(keys: string[], aggs: AggDef[]): Promise<LazyFrameResult<S>> {
    const groupBy = new BinaryGroupBy(keys, aggs, this.schema);

    const chunkSize = this._cache.chunkSize;
    for (let startRow = 0; startRow < this.shape[0]; startRow += chunkSize) {
      const chunkIdx = this._cache.getChunkIndex(startRow);
      // Check cache first
      let chunk = this._cache.get(chunkIdx);

      if (!chunk) {
        chunk = await this._loadChunk(chunkIdx);
        this._cache.set(chunkIdx, chunk);
      }

      groupBy.processChunk(chunk, this._columnOrder as string[]);

      this.clearCache();

      if (this._config.forceGc && typeof Bun !== 'undefined' && Bun.gc) {
        // @ts-ignore
        Bun.gc(true);
      }
    }

    const df = groupBy.toDataFrame();
    return { data: df as any };
  }

  /**
   * Load a specific chunk from file.
   */
  /**
   * Load a specific chunk from file.
   */
  private async _loadChunk(chunkIndex: number): Promise<BinaryChunk> {
    const chunkSize = this._cache.chunkSize;
    const startRow = chunkIndex * chunkSize;
    const endRow = Math.min(startRow + chunkSize, this.shape[0]);
    const count = endRow - startRow;

    if (count <= 0) {
      return { startRow, columns: [], rowCount: 0, sizeBytes: 0 };
    }

    const [startByte, endByte, skipRows] = this._rowIndex.getLooseRange(startRow, endRow);
    const blob = this._file.slice(startByte, endByte);

    const arrayBuffer = await blob.arrayBuffer();
    const bytes = new Uint8Array(arrayBuffer);
    const columns = parseChunkBytes(
      bytes,
      count,
      this._columnOrder as string[],
      this.schema,
      this._delimiter.charCodeAt(0),
      skipRows,
    );

    const sizeBytes = ChunkCache.estimateSize(columns, count);

    return { startRow, columns, rowCount: count, sizeBytes };
  }

  private _readVectorValue(vector: Vector, index: number): unknown {
    switch (vector.kind) {
      case 'float64':
      case 'int32':
        return vector.data[index];
      case 'bool':
        return vector.data[index] === 1;
      case 'string': {
        const start = vector.offsets[index]!;
        const len = vector.lengths[index]!;
        const buf = vector.data.subarray(start, start + len);

        if (this._config.raw) {
          return buf;
        }

        const str = new TextDecoder().decode(buf);
        // handle quotes if needed
        if (vector.needsUnescape[index]) {
          return str.replace(/""/g, '"');
        }
        return str;
      }
    }
  }

  /**
   * Convert raw column data to typed DataFrame using this.schema.
   */
  private _toDataFrame(data: Record<string, unknown[]>, rowCount: number): DataFrame<S> {
    const columns = new Map<keyof S, Series<DTypeKind>>();

    for (const colName of this._columnOrder) {
      const values = data[colName as string];
      if (!values) {
        throw new Error(`Missing column data for '${String(colName)}'`);
      }
      const dtype = this.schema[colName]!;
      columns.set(colName, DataFrame._createSeries(dtype, values));
    }

    return DataFrame._fromColumns(this.schema, columns, this._columnOrder, rowCount);
  }
}
