/** biome-ignore-all lint/style/noNonNullAssertion: Performance optimization */
/**
 * Sort operator.
 *
 * In-memory sorting for bounded datasets.
 * External sort with disk spilling for datasets exceeding spillThreshold.
 * Supports multi-column sorting with ascending/descending order.
 */

import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { Chunk } from "../buffer/chunk.ts";
import { ColumnBuffer } from "../buffer/column-buffer.ts";
import { StringViewColumnBuffer } from "../buffer/string-view-column.ts";
import { recycleChunk } from "../buffer/pool.ts";
import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import { getColumnIndex, type Schema } from "../types/schema.ts";
import { type Operator, type OperatorResult, opEmpty } from "./operator.ts";
import { BinaryWriter } from "../io/mbf/writer.ts";
import { BinaryReader } from "../io/mbf/reader.ts";

/** Default spill threshold in rows (1M rows ≈ 400-600MB depending on schema width). */
const DEFAULT_SPILL_THRESHOLD = 1_000_000;

/** Sort order specification */
export interface SortKey {
	column: string;
	descending?: boolean;
	nullsFirst?: boolean;
}

/** Options for sort operator */
export interface SortOptions {
	/** Row count threshold before spilling to disk (default: 1M, 0 = never spill) */
	spillThreshold?: number;
}

/** Entry in the priority queue used by k-way merge */
interface MergeEntry {
	/** Which run (reader index) this entry came from */
	runIdx: number;
	/** Which row within the current chunk from that run */
	rowIdx: number;
	/** The current chunk of that run */
	chunk: Chunk;
}

/**
 * Sort operator that buffers all input and sorts on finish.
 * Spills sorted runs to disk when row count exceeds spillThreshold.
 * Uses a k-way heap merge to keep peak memory O(k × chunkSize) during merge,
 * rather than O(total rows) as a naïve load-everything approach would.
 */
export class SortOperator implements Operator {
	readonly name = "Sort";
	readonly outputSchema: Schema;
	private readonly sortKeys: SortKey[];
	private readonly columnIndices: number[];
	private bufferedChunks: Chunk[] = [];
	private bufferedRowCount = 0;
	private readonly spillThreshold: number;
	private spillFiles: string[] = [];
	private spillDir: string | null = null;

	private constructor(
		schema: Schema,
		sortKeys: SortKey[],
		columnIndices: number[],
		options?: SortOptions,
	) {
		this.outputSchema = schema;
		this.sortKeys = sortKeys;
		this.columnIndices = columnIndices;
		this.spillThreshold = options?.spillThreshold ?? DEFAULT_SPILL_THRESHOLD;
	}

	static create(
		schema: Schema,
		sortKeys: SortKey[],
		options?: SortOptions,
	): Result<SortOperator> {
		if (sortKeys.length === 0) {
			return err(ErrorCode.InvalidExpression);
		}

		const columnIndices: number[] = [];

		for (const key of sortKeys) {
			const indexResult = getColumnIndex(schema, key.column);
			if (indexResult.error !== ErrorCode.None) {
				return err(ErrorCode.UnknownColumn);
			}
			columnIndices.push(indexResult.value);
		}

		return ok(new SortOperator(schema, sortKeys, columnIndices, options));
	}

	process(chunk: Chunk): Result<OperatorResult> {
		this.bufferedChunks.push(chunk);
		this.bufferedRowCount += chunk.rowCount;

		// Spill to disk if threshold exceeded
		if (
			this.spillThreshold > 0 &&
			this.bufferedRowCount >= this.spillThreshold
		) {
			// Spill happens synchronously via a blocking pattern
			// We store a promise that finish() will await
			this._pendingSpill = this.spillToDisk();
		}

		return ok(opEmpty());
	}

	/** Pending spill promise (if any) */
	private _pendingSpill: Promise<void> | null = null;
	/** State for iterating over spilled chunks in finish() */
	private _mergeGenerator: AsyncGenerator<OperatorResult> | null = null;

	finish(): Result<OperatorResult> | Promise<Result<OperatorResult>> {
		// If there are spill files or pending spills, delegate to async path
		if (this.spillFiles.length > 0 || this._pendingSpill || this._mergeGenerator) {
			return this.finishAsync();
		}

		if (this.bufferedChunks.length === 0) {
			return ok(opEmpty());
		}

		// No spills, sort entirely in memory (sync path)
		return ok(this.sortInMemory());
	}

	/** Async finish path for external sort with spill files */
	private async finishAsync(): Promise<Result<OperatorResult>> {
		// Wait for any pending spill
		if (this._pendingSpill) {
			await this._pendingSpill;
			this._pendingSpill = null;
		}

		if (this.bufferedChunks.length === 0 && this.spillFiles.length === 0) {
			return ok(opEmpty());
		}

		if (!this._mergeGenerator) {
			// Spill remaining buffered chunks
			if (this.bufferedChunks.length > 0) {
				await this.spillToDisk();
			}
			this._mergeGenerator = this.mergeSpilledRuns();
		}

		try {
			const next = await this._mergeGenerator.next();
			if (next.done) {
				this._mergeGenerator = null;
				return ok(opEmpty());
			}
			return ok(next.value);
		} catch (e) {
			this._mergeGenerator = null;
			return err(ErrorCode.ExecutionFailed);
		}
	}

	reset(): void {
		for(const chunk of this.bufferedChunks) {
			recycleChunk(chunk);
		}
		this.bufferedChunks = [];
		this.bufferedRowCount = 0;
		this.cleanup();
	}

	/** Clean up spill files */
	async cleanup(): Promise<void> {
		for(const file of this.spillFiles) {
			try {
				await fs.unlink(file);
			} catch {
				// Ignore cleanup errors
			}
		}
		this.spillFiles = [];
		if (this.spillDir) {
			try {
				await fs.rmdir(this.spillDir);
			} catch {
				// Ignore if not empty or doesn't exist
			}
			this.spillDir = null;
		}
	}

	private buildSortedChunk(
	perm: Uint32Array,
	chunkIndices: Uint16Array | Uint32Array,
	rowIndices: Uint32Array,
): Chunk {
	const schema = this.outputSchema;
	const dictionary = this.bufferedChunks[0]!.dictionary;

	// Create new column buffers
	const columns: ColumnBuffer[] = [];
	for (const col of schema.columns) {
		if (col.dtype.kind === DTypeKind.StringView) {
			columns.push(new StringViewColumnBuffer(perm.length, col.dtype.nullable) as unknown as ColumnBuffer);
		} else {
			columns.push(new ColumnBuffer(col.dtype.kind, perm.length, col.dtype.nullable));
		}
	}

	// Optimization #13: Column-First Gather
	// processing column-by-column improves write locality and cache utilization
	for (let c = 0; c < schema.columnCount; c++) {
		const destCol = columns[c]!;
		const isNullable = schema.columns[c]!.dtype.nullable;

		for (let i = 0; i < perm.length; i++) {
			const p = perm[i]!;
			const chunkIdx = chunkIndices[p]!;
			const rowIdx = rowIndices[p]!;

			const srcChunk = this.bufferedChunks[chunkIdx]!;
			const srcCol = srcChunk.columns[c]!;
			const physRow = srcChunk.selection
				? srcChunk.selection[rowIdx]!
				: rowIdx;

			if (isNullable && srcCol.isNull(physRow)) {
				destCol.appendNull();
			} else {

				if (schema.columns[c]!.dtype.kind === DTypeKind.StringView) {
					const bytes = srcChunk.getBytesValue(c, physRow);
					(destCol as unknown as StringViewColumnBuffer).appendBytes(bytes!, 0, bytes!.length);
				} else {
					const value = srcChunk.getValue(c, physRow);
					destCol.append(value!);
				}
			}
		}
	}

	return new Chunk(schema, columns, dictionary);
}

	/** Sort buffered chunks in memory and return the result */
	private sortInMemory(): OperatorResult {
	const totalRows = this.bufferedChunks.reduce(
		(sum, c) => sum + c.rowCount,
		0,
	);

	if (totalRows === 0) {
		return opEmpty();
	}

	const chunkIndices = new Uint16Array(totalRows);
	const rowIndices = new Uint32Array(totalRows);
	let idx = 0;
	for (let c = 0; c < this.bufferedChunks.length; c++) {
		const chunk = this.bufferedChunks[c]!;
		for (let r = 0; r < chunk.rowCount; r++) {
			chunkIndices[idx] = c;
			rowIndices[idx] = r;
			idx++;
		}
	}

	const perm = new Uint32Array(totalRows);
	for (let i = 0; i < totalRows; i++) perm[i] = i;

	perm.sort((ai, bi) => this.compareRows(
		this.bufferedChunks[chunkIndices[ai]!]!,
		rowIndices[ai]!,
		this.bufferedChunks[chunkIndices[bi]!]!,
		rowIndices[bi]!,
	));

	const outputChunk = this.buildSortedChunk(perm, chunkIndices, rowIndices);
	for (const chunk of this.bufferedChunks) {
		recycleChunk(chunk);
	}
	this.bufferedChunks = [];
	this.bufferedRowCount = 0;

	return {
		chunk: outputChunk,
		done: true,
		hasMore: false,
	};
}

	/**
	 * Compare two rows from (possibly different) chunks.
	 * Returns negative if a < b, positive if a > b, 0 if equal.
	 * Uses byte-level comparison for string columns to avoid TextDecoder allocation.
	 */
	private compareRows(
	chunkA: Chunk,
	rowA: number,
	chunkB: Chunk,
	rowB: number,
): number {
	for (let k = 0; k < this.sortKeys.length; k++) {
		const key = this.sortKeys[k]!;
		const colIdx = this.columnIndices[k]!;
		const dtype = this.outputSchema.columns[colIdx]!.dtype;

		const nullA = chunkA.isNull(colIdx, rowA);
		const nullB = chunkB.isNull(colIdx, rowB);

		if (nullA && nullB) continue;
		if (nullA) return key.nullsFirst ? -1 : 1;
		if (nullB) return key.nullsFirst ? 1 : -1;

		let cmp: number;
		if (dtype.kind === DTypeKind.String) {
			// Use dictionary index comparison first (dict indices are interned per-chunk,
			// so only valid if both chunks share the same dictionary object).
			// Fall back to byte-level UTF-8 compare for cross-chunk comparisons.
			const dictA = chunkA.dictionary;
			const dictB = chunkB.dictionary;

			if (dictA !== null && dictB !== null && dictA === dictB) {
				// Same dictionary — compare dictionary indices directly is ambiguous
				// (index ordering ≠ lexicographic ordering). We must do byte comparison.
				const idxA = chunkA.getValue(colIdx, rowA) as number;
				const idxB = chunkB.getValue(colIdx, rowB) as number;
				cmp = dictA.compare(idxA, idxB);
			} else if (dictA !== null && dictB !== null) {
				// Different dictionaries: get bytes from each and compare
				const idxA = chunkA.getValue(colIdx, rowA) as number;
				const idxB = chunkB.getValue(colIdx, rowB) as number;
				const bytesA = dictA.getBytes(idxA);
				const bytesB = dictB.getBytes(idxB);
				cmp = compareBytesLex(bytesA, bytesB);
			} else {
				// StringView or fallback path: materialize strings
				const strA = chunkA.getStringValue(colIdx, rowA) ?? "";
				const strB = chunkB.getStringValue(colIdx, rowB) ?? "";
				cmp = strA < strB ? -1 : strA > strB ? 1 : 0;
			}
		} else {
			const valA = chunkA.getValue(colIdx, rowA);
			const valB = chunkB.getValue(colIdx, rowB);
			if (typeof valA === "bigint" && typeof valB === "bigint") {
				cmp = valA < valB ? -1 : valA > valB ? 1 : 0;
			} else {
				cmp = (valA as number) - (valB as number);
			}
		}

		if (cmp !== 0) {
			return key.descending ? -cmp : cmp;
		}
	}
	return 0;
}

	/** Sort buffered chunks and write them to a temp MBF file */
	private async spillToDisk(): Promise < void> {
	if(this.bufferedChunks.length === 0) return;

	// Create temp dir on first spill
	if(!this.spillDir) {
	this.spillDir = await fs.mkdtemp(
		path.join(os.tmpdir(), "molniya-sort-"),
	);
}

// Sort in memory first
const sorted = this.sortInMemory();
if (!sorted.chunk) return;

// Write sorted run to MBF file
const spillPath = path.join(
	this.spillDir,
	`run-${this.spillFiles.length}.mbf`,
);
const writer = new BinaryWriter(spillPath, this.outputSchema);
await writer.open();

try {
	await writer.writeChunk(sorted.chunk);
} finally {
	await writer.close();
}

this.spillFiles.push(spillPath);
this.bufferedChunks = [];
this.bufferedRowCount = 0;
	}

	/**
	 * Perform K-way merge of all spill files.
	 * Yields chunk outputs up to an imposed BATCH_SIZE so pipelines limit memory overhead.
	 */
	private async * mergeSpilledRuns(): AsyncGenerator < OperatorResult > {
	const readers: BinaryReader[] = [];
	const iterators: AsyncGenerator<Chunk>[] = [];
	let currentChunks: (Chunk | null)[] = [];

	try {
		for(const file of this.spillFiles) {
	const reader = new BinaryReader(file);
	await reader.open();
	readers.push(reader);
	iterators.push(reader.scan());
}

// Build a min-heap of { runIdx, rowIdx, chunk } entries.
// We seed the heap with the first row of each run's first chunk.
const heap: MergeEntry[] = [];
// currentChunks[i] = the currently buffered chunk from run i
currentChunks = new Array(iterators.length).fill(null);
// currentRowOf[i] = next unprocessed row index in currentChunks[i]
const currentRowOf: number[] = new Array(iterators.length).fill(0);

// Seed: advance each run to its first chunk
for (let runIdx = 0; runIdx < iterators.length; runIdx++) {
	const first = await iterators[runIdx]!.next();
	if (!first.done && first.value) {
		currentChunks[runIdx] = first.value;
		currentRowOf[runIdx] = 0;
		heapPush(heap, {
			runIdx,
			rowIdx: 0,
			chunk: first.value,
		}, (a, b) => this.compareRows(a.chunk, a.rowIdx, b.chunk, b.rowIdx));
	}
}

// Output: collect into bounded chunks per "batch".
const schema = this.outputSchema;
const BATCH_SIZE = 8192;
let outColumns: ColumnBuffer[] = schema.columns.map((col) => {
	if (col.dtype.kind === DTypeKind.StringView) {
		return new StringViewColumnBuffer(BATCH_SIZE, col.dtype.nullable) as unknown as ColumnBuffer;
	}
	return new ColumnBuffer(col.dtype.kind, BATCH_SIZE, col.dtype.nullable);
});
// Use the dictionary from the first available chunk
const outDictionary = currentChunks.find((c) => c !== null)?.dictionary ?? null;
let outRowCount = 0;

const compareEntries = (a: MergeEntry, b: MergeEntry) =>
	this.compareRows(a.chunk, a.rowIdx, b.chunk, b.rowIdx);

while (heap.length > 0) {
	const min = heapPop(heap, compareEntries)!;
	const { runIdx, rowIdx, chunk } = min;

	// Append this row to the output columns
	for (let c = 0; c < schema.columnCount; c++) {
		const destCol = outColumns[c]!;
		if (chunk.isNull(c, rowIdx)) {
			destCol.appendNull();
		} else {
			if (schema.columns[c]!.dtype.kind === DTypeKind.StringView) {
				const bytes = chunk.getBytesValue(c, rowIdx);
				(outColumns[c] as unknown as StringViewColumnBuffer).appendBytes(bytes!, 0, bytes!.length);
			} else {
				const value = chunk.getValue(c, rowIdx);
				outColumns[c]!.append(value!);
			}
		}
	}
	outRowCount++;

	if (outRowCount === BATCH_SIZE) {
		for (let c = 0; c < schema.columnCount; c++) {
			(outColumns[c] as unknown as { _length: number })._length = outRowCount;
		}
					yield { chunk: new Chunk(schema, outColumns, outDictionary), done: false, hasMore: true };
		outColumns = schema.columns.map((col) => {
			if (col.dtype.kind === DTypeKind.StringView) {
				return new StringViewColumnBuffer(BATCH_SIZE, col.dtype.nullable) as unknown as ColumnBuffer;
			}
			return new ColumnBuffer(col.dtype.kind, BATCH_SIZE, col.dtype.nullable);
		});
		outRowCount = 0;
	}

	// Advance this run: try next row in same chunk, then next chunk
	const nextRow = rowIdx + 1;
	if (nextRow < chunk.rowCount) {
		heapPush(heap, { runIdx, rowIdx: nextRow, chunk }, compareEntries);
	} else {
		// This chunk is exhausted — load next chunk from this run
		recycleChunk(chunk);
		const next = await iterators[runIdx]!.next();
		if (!next.done && next.value) {
			const nextChunk = next.value;
			currentChunks[runIdx] = nextChunk;
			currentRowOf[runIdx] = 0;
			heapPush(heap, { runIdx, rowIdx: 0, chunk: nextChunk }, compareEntries);
		} else {
			// Run exhausted
			currentChunks[runIdx] = null;
		}
	}
}

// Close readers
for (const chunk of currentChunks) {
	if (chunk) recycleChunk(chunk);
}
for (const reader of readers) {
	await reader.close();
}

await this.cleanup();

if (outRowCount === 0) {
	return ok(opEmpty());
}

const resultChunk = new Chunk(schema, outColumns, outDictionary);
return ok({ chunk: resultChunk, done: true, hasMore: false });
		} catch (e) {
	for (const chunk of currentChunks) {
		if (chunk) recycleChunk(chunk);
	}
	for (const reader of readers) {
		try {
			await reader.close();
		} catch {
			// Ignore
		}
	}
	await this.cleanup();
	throw e;
}
	}
}

// ─── Min-Heap helpers ────────────────────────────────────────────────────────

function heapPush<T>(
	heap: T[],
	item: T,
	compare: (a: T, b: T) => number,
): void {
	heap.push(item);
	siftUp(heap, heap.length - 1, compare);
}

function heapPop<T>(
	heap: T[],
	compare: (a: T, b: T) => number,
): T | undefined {
	if (heap.length === 0) return undefined;
	const top = heap[0]!;
	const last = heap.pop()!;
	if (heap.length > 0) {
		heap[0] = last;
		siftDown(heap, 0, compare);
	}
	return top;
}

function siftUp<T>(heap: T[], i: number, compare: (a: T, b: T) => number): void {
	while (i > 0) {
		const parent = (i - 1) >> 1;
		if (compare(heap[i]!, heap[parent]!) < 0) {
			[heap[i], heap[parent]] = [heap[parent]!, heap[i]!];
			i = parent;
		} else {
			break;
		}
	}
}

function siftDown<T>(heap: T[], i: number, compare: (a: T, b: T) => number): void {
	const n = heap.length;
	while (true) {
		let smallest = i;
		const left = 2 * i + 1;
		const right = 2 * i + 2;
		if (left < n && compare(heap[left]!, heap[smallest]!) < 0) smallest = left;
		if (right < n && compare(heap[right]!, heap[smallest]!) < 0) smallest = right;
		if (smallest !== i) {
			[heap[i], heap[smallest]] = [heap[smallest]!, heap[i]!];
			i = smallest;
		} else {
			break;
		}
	}
}

// ─── Byte-level lexicographic comparison ────────────────────────────────────

/**
 * Compare two UTF-8 byte slices lexicographically.
 * Returns <0, 0, >0. Avoids string materialization.
 */
function compareBytesLex(
	a: Uint8Array | undefined,
	b: Uint8Array | undefined,
): number {
	if (a === undefined) return b === undefined ? 0 : -1;
	if (b === undefined) return 1;
	const minLen = Math.min(a.length, b.length);
	for (let i = 0; i < minLen; i++) {
		const diff = (a[i] ?? 0) - (b[i] ?? 0);
		if (diff !== 0) return diff;
	}
	return a.length - b.length;
}

// ─── Column buffer growth helper ────────────────────────────────────────────

/**
 * Return a new ColumnBuffer with double the capacity, copying existing data.
 * Used during the k-way merge when the output estimate was too small.
 */
function growColumn(col: ColumnBuffer): ColumnBuffer {
	const newCap = Math.max(col.capacity * 2, 1024);
	if (col.kind === DTypeKind.StringView) {
		const newCol = new StringViewColumnBuffer(newCap, col.isNullable) as unknown as ColumnBuffer;
		const srcCol = col as unknown as StringViewColumnBuffer;
		const dstCol = newCol as unknown as StringViewColumnBuffer;
		for (let i = 0; i < srcCol.length; i++) {
			if (srcCol.isNull(i)) {
				dstCol.appendNull();
			} else {
				const bytes = srcCol.getBytes(i);
				if (bytes) {
					dstCol.appendBytes(bytes, 0, bytes.length);
				} else {
					dstCol.appendBytes(new Uint8Array(0), 0, 0);
				}
			}
		}
		return newCol;
	}

	const newCol = new ColumnBuffer(col.kind, newCap, col.isNullable);
	// Bulk-copy valid data
	const srcData = col.data;
	const dstData = newCol.data;
	// biome-ignore lint/suspicious/noExplicitAny: TypedArray interop
	(dstData as any).set((srcData as any).subarray(0, col.length));
	(newCol as unknown as { _length: number })._length = col.length;
	// Copy null bitmap if present
	if (col.isNullable) {
		for (let i = 0; i < col.length; i++) {
			if (col.isNull(i)) newCol.setNull(i, true);
		}
	}
	return newCol;
}

/**
 * Sort expression builder for configuring sort options.
 */
export class SortExpression {
	constructor(
		public readonly column: string,
		public readonly descending: boolean = false,
		private readonly _nullsFirst?: boolean,
	) { }

	/**
	 * Configure nulls to appear first.
	 */
	nullsFirst(): SortExpression {
		return new SortExpression(this.column, this.descending, true);
	}

	/**
	 * Configure nulls to appear last.
	 */
	nullsLast(): SortExpression {
		return new SortExpression(this.column, this.descending, false);
	}

	/** Conversion to SortKey interface */
	toSortKey(): SortKey {
		return {
			column: this.column,
			descending: this.descending,
			nullsFirst: this._nullsFirst,
		};
	}
}

/**
 * Handle input that can be string, SortKey, or SortExpression
 */
function toSortKey(k: string | SortKey | SortExpression): SortKey {
	if (typeof k === "string") return { column: k };
	if (k instanceof SortExpression) return k.toSortKey();
	return k;
}

/**
 * Create a sort operator.
 */
export function sort(
	schema: Schema,
	...keys: (string | SortKey | SortExpression)[]
): Result<SortOperator> {
	// Check if last arg is SortOptions
	let options: SortOptions | undefined;
	const actualKeys = [...keys];
	const last = actualKeys[actualKeys.length - 1];
	if (
		last &&
		typeof last === "object" &&
		!(last instanceof SortExpression) &&
		!("column" in last)
	) {
		options = actualKeys.pop() as unknown as SortOptions;
	}

	const sortKeys: SortKey[] = actualKeys.map(toSortKey);
	return SortOperator.create(schema, sortKeys, options);
}

/**
 * Create an ascending sort key.
 */
export function asc(column: string): SortExpression {
	return new SortExpression(column, false);
}

/**
 * Create a descending sort key.
 */
export function desc(column: string): SortExpression {
	return new SortExpression(column, true);
}
