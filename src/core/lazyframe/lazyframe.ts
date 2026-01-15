import { DataFrame } from '../dataframe';
import { Series } from '../series';
import type { DType, DTypeKind, InferSchema, Schema } from '../types';
import { ChunkCache, type ChunkData } from './chunk-cache';
import type { ILazyFrame, LazyFrameConfig } from './interface';
import { RowIndex } from './row-index';

/**
 * LazyFrame - Memory-efficient DataFrame for large CSV files.
 *
 * Keeps data on disk and loads rows on-demand using a chunked LRU cache.
 * Suitable for files that exceed available RAM.
 *
 * @example
 * ```ts
 * const lazy = await scanCsv('./huge_file.csv');
 * const first10 = await lazy.head(10);
 * first10.print();
 *
 * // Stream filter without loading all data
 * const filtered = await lazy.filter(row => row.price > 100);
 * ```
 */
export class LazyFrame<S extends Schema> implements ILazyFrame<S> {
  readonly schema: S;
  readonly shape: readonly [rows: number, cols: number];
  readonly path: string;

  private readonly _file: ReturnType<typeof Bun.file>;
  private readonly _buffer: Buffer;
  private readonly _rowIndex: RowIndex;
  private readonly _columnOrder: (keyof S)[];
  private readonly _cache: ChunkCache<InferSchema<S>>;
  private readonly _hasHeader: boolean;
  private readonly _delimiter: string;

  /**
   * Private constructor - use scanCsv() factory function instead.
   */
  private constructor(
    path: string,
    file: ReturnType<typeof Bun.file>,
    buffer: Buffer,
    schema: S,
    columnOrder: (keyof S)[],
    rowIndex: RowIndex,
    config: LazyFrameConfig,
    hasHeader: boolean,
    delimiter: string,
  ) {
    this.path = path;
    this._file = file;
    this._buffer = buffer;
    this.schema = schema;
    this._columnOrder = columnOrder;
    this._rowIndex = rowIndex;
    this.shape = [rowIndex.rowCount, columnOrder.length] as const;
    this._cache = new ChunkCache<InferSchema<S>>({
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
    const arrayBuffer = await file.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    const rowIndex = RowIndex.build(buffer, hasHeader);

    return new LazyFrame<S>(
      path,
      file,
      buffer,
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
    const df = await this.collect();
    return df.col(name);
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
    const rows = await this._loadRows(0, count);
    return DataFrame.from(this.schema, rows);
  }

  /**
   * Get last n rows as DataFrame.
   */
  async tail(n = 5): Promise<DataFrame<S>> {
    const count = Math.min(n, this.shape[0]);
    const startRow = this.shape[0] - count;
    const rows = await this._loadRows(startRow, count);
    return DataFrame.from(this.schema, rows);
  }

  /**
   * Select specific columns (returns new LazyFrame).
   */
  select<K extends keyof S>(...cols: K[]): ILazyFrame<Pick<S, K>> {
    const newSchema = {} as Pick<S, K>;
    for (const colName of cols) {
      newSchema[colName] = this.schema[colName];
    }

    // Return a view that filters columns on read
    return new LazyFrameColumnView<S, K>(this, cols, newSchema);
  }

  /**
   * Filter rows by predicate function (streaming - memory efficient).
   * Processes rows in chunks to limit memory usage.
   */
  async filter(fn: (row: InferSchema<S>, index: number) => boolean): Promise<DataFrame<S>> {
    const matchingRows: InferSchema<S>[] = [];
    const chunkSize = this._cache.chunkSize;

    for (let startRow = 0; startRow < this.shape[0]; startRow += chunkSize) {
      const count = Math.min(chunkSize, this.shape[0] - startRow);
      const chunk = await this._loadRows(startRow, count);

      for (let i = 0; i < chunk.length; i++) {
        const globalIdx = startRow + i;
        if (fn(chunk[i]!, globalIdx)) {
          matchingRows.push(chunk[i]!);
        }
      }
    }

    return DataFrame.from(this.schema, matchingRows);
  }

  /**
   * Collect all (or limited) rows into a DataFrame.
   * Warning: For large files, this loads everything into memory.
   */
  async collect(limit?: number): Promise<DataFrame<S>> {
    const count = limit !== undefined ? Math.min(limit, this.shape[0]) : this.shape[0];
    const rows = await this._loadRows(0, count);
    return DataFrame.from(this.schema, rows);
  }

  // Info & Display
  // ===============================================================

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

  /**
   * Clear the row cache to free memory.
   */
  clearCache(): void {
    this._cache.clear();
  }

  // Internal: Row Loading
  // ===============================================================

  /**
   * Load rows from file (with caching).
   */
  private async _loadRows(startRow: number, count: number): Promise<InferSchema<S>[]> {
    const results: InferSchema<S>[] = new Array(count);
    const chunkSize = this._cache.chunkSize;
    const startChunk = this._cache.getChunkIndex(startRow);
    const endChunk = this._cache.getChunkIndex(startRow + count - 1);

    for (let chunkIdx = startChunk; chunkIdx <= endChunk; chunkIdx++) {
      let chunk = this._cache.get(chunkIdx);

      if (!chunk) {
        // Load and cache this chunk
        chunk = await this._loadChunk(chunkIdx);
        this._cache.set(chunkIdx, chunk);
      }

      // Copy relevant rows to results
      const chunkStartRow = chunkIdx * chunkSize;
      const resultStartIdx = Math.max(0, chunkStartRow - startRow);
      const chunkOffsetStart = Math.max(0, startRow - chunkStartRow);
      const chunkOffsetEnd = Math.min(chunk.rows.length, startRow + count - chunkStartRow);

      for (let i = chunkOffsetStart; i < chunkOffsetEnd; i++) {
        const resultIdx = chunkStartRow + i - startRow;
        if (resultIdx >= 0 && resultIdx < count) {
          results[resultIdx] = chunk.rows[i]!;
        }
      }
    }

    return results;
  }

  /**
   * Load a specific chunk from file.
   */
  private async _loadChunk(chunkIndex: number): Promise<ChunkData<InferSchema<S>>> {
    const chunkSize = this._cache.chunkSize;
    const startRow = chunkIndex * chunkSize;
    const endRow = Math.min(startRow + chunkSize, this.shape[0]);
    const count = endRow - startRow;

    if (count <= 0) {
      return { startRow, rows: [], sizeBytes: 0 };
    }

    const [startByte, endByte] = this._rowIndex.getRowsRange(startRow, endRow);
    const chunkBytes = this._buffer.subarray(startByte, endByte);
    const chunkText = chunkBytes.toString('utf-8');

    const rows = this._parseChunk(chunkText, count);
    const sizeBytes = ChunkCache.estimateSize(rows, this._columnOrder.length);

    return { startRow, rows, sizeBytes };
  }

  /**
   * Parse a chunk of CSV text into row objects.
   */
  private _parseChunk(text: string, expectedRows: number): InferSchema<S>[] {
    const rows: InferSchema<S>[] = [];
    const lines = text.split('\n');

    for (let i = 0; i < lines.length && rows.length < expectedRows; i++) {
      let line = lines[i]!;
      // Trim CR if present
      if (line.endsWith('\r')) {
        line = line.slice(0, -1);
      }
      if (line.length === 0) continue;

      const fields = this._parseLine(line);
      const row = {} as InferSchema<S>;

      for (let col = 0; col < this._columnOrder.length; col++) {
        const colName = this._columnOrder[col]! as keyof InferSchema<S>;
        const dtype = this.schema[colName as keyof S];
        const fieldValue = fields[col] ?? '';

        (row as Record<string, unknown>)[colName as string] = this._parseValue(fieldValue, dtype);
      }

      rows.push(row);
    }

    return rows;
  }

  /**
   * Parse a single CSV line handling quotes.
   */
  private _parseLine(line: string): string[] {
    const fields: string[] = [];
    let current = '';
    let inQuotes = false;
    let i = 0;

    while (i < line.length) {
      const char = line[i]!;

      if (inQuotes) {
        if (char === '"') {
          if (i + 1 < line.length && line[i + 1] === '"') {
            current += '"';
            i += 2;
          } else {
            inQuotes = false;
            i++;
          }
        } else {
          current += char;
          i++;
        }
      } else {
        if (char === '"') {
          inQuotes = true;
          i++;
        } else if (char === this._delimiter) {
          fields.push(current);
          current = '';
          i++;
        } else {
          current += char;
          i++;
        }
      }
    }

    fields.push(current);
    return fields;
  }

  /**
   * Parse a string value according to dtype.
   */
  private _parseValue(value: string, dtype: DType<DTypeKind> | undefined): unknown {
    if (!dtype) return value;

    switch (dtype.kind) {
      case 'float64':
        return parseFloat(value) || 0;
      case 'int32':
        return parseInt(value, 10) || 0;
      case 'bool':
        return value === 'true' || value === 'True' || value === '1';
      default:
        return value;
    }
  }
}

/**
 * Column view wrapper for select() operation.
 * Wraps a LazyFrame and filters columns on access.
 */
class LazyFrameColumnView<S extends Schema, K extends keyof S> implements ILazyFrame<Pick<S, K>> {
  readonly schema: Pick<S, K>;
  readonly shape: readonly [rows: number, cols: number];
  readonly path: string;

  private readonly _source: LazyFrame<S>;
  private readonly _columns: K[];

  constructor(source: LazyFrame<S>, columns: K[], schema: Pick<S, K>) {
    this._source = source;
    this._columns = columns;
    this.schema = schema;
    this.shape = [source.shape[0], columns.length] as const;
    this.path = source.path;
  }

  async col<C extends K>(name: C): Promise<Series<Pick<S, K>[C]['kind']>> {
    return this._source.col(name) as Promise<Series<Pick<S, K>[C]['kind']>>;
  }

  columns(): K[] {
    return [...this._columns];
  }

  async head(n = 5): Promise<DataFrame<Pick<S, K>>> {
    const df = await this._source.head(n);
    return df.select(...this._columns);
  }

  async tail(n = 5): Promise<DataFrame<Pick<S, K>>> {
    const df = await this._source.tail(n);
    return df.select(...this._columns);
  }

  select<C extends K>(...cols: C[]): ILazyFrame<Pick<Pick<S, K>, C>> {
    const newSchema = {} as Pick<Pick<S, K>, C>;
    for (const colName of cols) {
      newSchema[colName] = this.schema[colName];
    }
    return new LazyFrameColumnView<S, C>(this._source, cols, newSchema as Pick<S, C>) as unknown as ILazyFrame<Pick<Pick<S, K>, C>>;
  }

  async filter(fn: (row: InferSchema<Pick<S, K>>, index: number) => boolean): Promise<DataFrame<Pick<S, K>>> {
    const df = await this._source.filter((row, idx) => {
      const projected = {} as InferSchema<Pick<S, K>>;
      for (const col of this._columns) {
        (projected as Record<string, unknown>)[col as string] = (row as Record<string, unknown>)[col as string];
      }
      return fn(projected, idx);
    });
    return df.select(...this._columns);
  }

  async collect(limit?: number): Promise<DataFrame<Pick<S, K>>> {
    const df = await this._source.collect(limit);
    return df.select(...this._columns);
  }

  info(): { rows: number; columns: number; dtypes: Record<string, string>; cached: number } {
    const baseInfo = this._source.info();
    const dtypes: Record<string, string> = {};
    for (const col of this._columns) {
      dtypes[col as string] = baseInfo.dtypes[col as string] ?? 'unknown';
    }
    return {
      rows: baseInfo.rows,
      columns: this._columns.length,
      dtypes,
      cached: baseInfo.cached,
    };
  }

  async print(): Promise<void> {
    const df = await this.head(10);
    console.log(`LazyFrame [${this.path}] (${this._columns.length} columns selected)`);
    df.print();
    if (this.shape[0] > 10) {
      console.log(`... ${this.shape[0] - 10} more rows`);
    }
  }

  clearCache(): void {
    this._source.clearCache();
  }
}
