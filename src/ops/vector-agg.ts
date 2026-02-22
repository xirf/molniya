/**
 * Vectorized Aggregation Primitives.
 *
 * Provides batch processing for aggregations, operating on arrays of values
 * and group IDs instead of single value/state pairs.
 */
/** biome-ignore-all lint/style/noNonNullAssertion: Performance critical inner loops */

import { ColumnBuffer, type TypedArray } from "../buffer/column-buffer.ts";
import { type DType, DType as DTypeFactory, DTypeKind } from "../types/dtypes.ts";
import { AggType } from "./agg-state.ts";

/**
 * Interface for vectorized aggregation.
 */
export interface BatchAggregator {
	/** Resize state storage for N groups */
	resize(numGroups: number): void;

	/**
	 * Accumulate a batch of values.
	 * @param data Raw columnar data (TypedArray) or null for CountAll
	 * @param groupIds Array of group IDs corresponding to data indices
	 * @param count Number of rows to process
	 * @param selection Optional selection vector (row indices)
	 * @param nullBitmap Optional null bitmap from the column
	 */
	accumulateBatch(
		_data: TypedArray | null,
		groupIds: Int32Array,
		count: number,
		_selection: Uint32Array | null,
		_column: ColumnBuffer | null,
		_dictionary?: import("../buffer/dictionary.ts").Dictionary | null,
	): void;

	/** Finalize and return results as a column buffer */
	finish(): ColumnBuffer;

	/**
	 * Reset state for reuse without reallocating backing arrays.
	 * Called between spill cycles in GroupBy to avoid GC churn.
	 */
	resetState(): void;

	/** Get output dtype */
	readonly outputDType: DType;
}

/** Base class for aggregators managing a result buffer */
abstract class BaseVectorAggregator implements BatchAggregator {
	protected values: Float64Array | BigInt64Array; // Store sums/counts/etc
	protected hasValue: Uint8Array; // Bitmask or byte-array for null tracking? Byte for speed.
	protected size: number = 0;

	abstract readonly outputDType: DType;

	constructor() {
		// Start with small capacity
		this.values = new Float64Array(0);
		this.hasValue = new Uint8Array(0);
	}

	resize(numGroups: number): void {
		if (numGroups > this.values.length) {
			const newSize = Math.max(numGroups, this.values.length * 2);
			this.grow(newSize);
		}

		// Initialize new slots if necessary (depends on agg type)
		// For Sum/Count 0 is fine. For Min/Max we need initialization.
		this.initialize(this.size, numGroups);
		this.size = numGroups;
	}

	protected abstract grow(newSize: number): void;
	protected abstract initialize(start: number, end: number): void;

	abstract accumulateBatch(
		_data: TypedArray | null,
		groupIds: Int32Array,
		count: number,
		_selection: Uint32Array | null,
		_column: ColumnBuffer | null,
	): void;

	finish(): ColumnBuffer {
		const col = new ColumnBuffer(
			this.outputDType.kind,
			this.size,
			this.outputDType.nullable,
		);
		// Copy data
		const count = this.size;

		for (let i = 0; i < count; i++) {
			if (this.hasValue[i]) {
				// @ts-expect-error - copying typed array elements
				col.set(i, this.values[i]);
			} else {
				col.setNull(i, true);
			}
		}
		(col as unknown as { _length: number })._length = count;
		return col;
	}

	/**
	 * Reset state for reuse: zero the arrays, reset size counter.
	 * Does NOT reallocate — preserves existing capacity.
	 */
	resetState(): void {
		if (this.size > 0) {
			// Can't use a single fill() because Float64Array and BigInt64Array have incompatible element types.
			// Reset via subarray to handle both cases without casting to 'never'.
			if (this.values instanceof BigInt64Array) {
				(this.values as BigInt64Array).fill(BigInt(0), 0, this.size);
			} else {
				(this.values as Float64Array).fill(0, 0, this.size);
			}
			this.hasValue.fill(0, 0, this.size);
		}
		this.size = 0;
	}
}

/** Vector Sum */
export class VectorSum extends BaseVectorAggregator {
	protected declare values: Float64Array; // Override type
	readonly outputDType = DTypeFactory.float64;

	constructor() {
		super();
		this.values = new Float64Array(1024);
		this.hasValue = new Uint8Array(1024);
	}

	protected grow(newSize: number) {
		const newValues = new Float64Array(newSize);
		newValues.set(this.values);
		this.values = newValues;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(_start: number, _end: number) {
		// Float64Array initializes to 0, which is correct for Sum
	}

	accumulateBatch(
		data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const vals = this.values;
		const hasVal = this.hasValue;

		// Tight inner loop selection
		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				vals[gid]! += Number(data[row]);
				hasVal[gid] = 1;
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				vals[gid]! += Number(data[i]);
				hasVal[gid] = 1;
			}
		}
	}
}

/** Vector Count (Non-null) */
export class VectorCount extends BaseVectorAggregator {
	protected declare values: BigInt64Array;
	readonly outputDType = DTypeFactory.int64;

	constructor() {
		super();
		this.values = new BigInt64Array(1024);
		this.hasValue = new Uint8Array(1024); // Count is never null usually, but we keep structure
	}

	override resize(numGroups: number) {
		super.resize(numGroups);
		// Count is never null — mark all slots as having a value
		for (let i = 0; i < numGroups; i++) this.hasValue[i] = 1;
	}

	override resetState(): void {
		if (this.size > 0) {
			(this.values as BigInt64Array).fill(BigInt(0), 0, this.size);
			this.hasValue.fill(1, 0, this.size); // counts always "have value"
		}
		this.size = 0;
	}

	protected grow(newSize: number) {
		const newValues = new BigInt64Array(newSize);
		newValues.set(this.values);
		this.values = newValues;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(_start: number, _end: number) {
		// BigInt64 initializes to 0n
	}

	accumulateBatch(
		_data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const vals = this.values;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				vals[gid]!++;
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				vals[gid]!++;
			}
		}
	}

	// Override finish because Count should not return nulls
	override finish(): ColumnBuffer {
		const col = new ColumnBuffer(this.outputDType.kind, this.size, false);
		col.data.set(this.values.subarray(0, this.size));
		(col as unknown as { _length: number })._length = this.size;
		return col;
	}
}

/** Vector Count All (Star) */
export class VectorCountAll extends VectorCount {
	override accumulateBatch(
		_data: TypedArray | null,
		groupIds: Int32Array,
		count: number,
		_selection: Uint32Array | null,
		_column: ColumnBuffer | null,
	): void {
		const vals = this.values;
		// Count ALL rows, ignoring nulls in data
		// selection still matters (we only count selected rows)
		// But we just iterate 0..count because groupIds is already aligned with selection logic in GroupBy

		for (let i = 0; i < count; i++) {
			const gid = groupIds[i]!;
			vals[gid]!++;
		}
	}
}

/** Vector Min */
export class VectorMin extends BaseVectorAggregator {
	protected declare values: Float64Array;
	readonly outputDType = DTypeFactory.float64;

	constructor() {
		super();
		this.values = new Float64Array(1024).fill(Infinity);
		this.hasValue = new Uint8Array(1024);
	}

	protected grow(newSize: number) {
		const newValues = new Float64Array(newSize).fill(Infinity);
		newValues.set(this.values);
		this.values = newValues;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(start: number, end: number) {
		this.values.fill(Infinity, start, end);
	}

	accumulateBatch(
		data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const vals = this.values;
		const hasVal = this.hasValue;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				const v = Number(data[row]);
				if (v < vals[gid]!) {
					vals[gid] = v;
					hasVal[gid] = 1;
				}
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				const v = Number(data[i]);
				if (v < vals[gid]!) {
					vals[gid] = v;
					hasVal[gid] = 1;
				}
			}
		}
	}
}

/** Vector Max */
export class VectorMax extends BaseVectorAggregator {
	protected declare values: Float64Array;
	readonly outputDType = DTypeFactory.float64;

	constructor() {
		super();
		this.values = new Float64Array(1024).fill(-Infinity);
		this.hasValue = new Uint8Array(1024);
	}

	protected grow(newSize: number) {
		const newValues = new Float64Array(newSize).fill(-Infinity);
		newValues.set(this.values);
		this.values = newValues;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(start: number, end: number) {
		this.values.fill(-Infinity, start, end);
	}

	accumulateBatch(
		data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const vals = this.values;
		const hasVal = this.hasValue;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				const v = Number(data[row]);
				if (v > vals[gid]!) {
					vals[gid] = v;
					hasVal[gid] = 1;
				}
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				const v = Number(data[i]);
				if (v > vals[gid]!) {
					vals[gid] = v;
					hasVal[gid] = 1;
				}
			}
		}
	}
}

/** Vector Avg (tracks sum and count) */
export class VectorAvg extends BaseVectorAggregator {
	protected declare values: Float64Array; // sums
	protected counts: Float64Array;
	readonly outputDType = DTypeFactory.float64;

	constructor() {
		super();
		this.values = new Float64Array(1024);
		this.counts = new Float64Array(1024);
		this.hasValue = new Uint8Array(1024);
	}

	protected grow(newSize: number) {
		const newValues = new Float64Array(newSize);
		newValues.set(this.values);
		this.values = newValues;

		const newCounts = new Float64Array(newSize);
		newCounts.set(this.counts);
		this.counts = newCounts;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(_start: number, _end: number) {
		// Float64Array initializes to 0
	}

	accumulateBatch(
		data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const vals = this.values;
		const cnts = this.counts;
		const hasVal = this.hasValue;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				vals[gid]! += Number(data[row]);
				cnts[gid]!++;
				hasVal[gid] = 1;
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				vals[gid]! += Number(data[i]);
				cnts[gid]!++;
				hasVal[gid] = 1;
			}
		}
	}

	override finish(): ColumnBuffer {
		const col = new ColumnBuffer(
			this.outputDType.kind,
			this.size,
			this.outputDType.nullable,
		);
		for (let i = 0; i < this.size; i++) {
			if (this.hasValue[i] && this.counts[i]! > 0) {
				col.set(i, this.values[i]! / this.counts[i]!);
			} else {
				col.setNull(i, true);
			}
		}
		(col as unknown as { _length: number })._length = this.size;
		return col;
	}

	override resetState(): void {
		if (this.size > 0) {
			this.values.fill(0, 0, this.size);
			this.counts.fill(0, 0, this.size);
			this.hasValue.fill(0, 0, this.size);
		}
		this.size = 0;
	}
}

/** Vector Std (Welford's online algorithm) */
export class VectorStd extends BaseVectorAggregator {
	protected declare values: Float64Array; // M2 values
	protected means: Float64Array;
	protected counts: Float64Array;
	readonly outputDType = DTypeFactory.float64;

	constructor() {
		super();
		this.values = new Float64Array(1024); // M2
		this.means = new Float64Array(1024);
		this.counts = new Float64Array(1024);
		this.hasValue = new Uint8Array(1024);
	}

	protected grow(newSize: number) {
		const newValues = new Float64Array(newSize);
		newValues.set(this.values);
		this.values = newValues;

		const newMeans = new Float64Array(newSize);
		newMeans.set(this.means);
		this.means = newMeans;

		const newCounts = new Float64Array(newSize);
		newCounts.set(this.counts);
		this.counts = newCounts;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(_start: number, _end: number) {
		// Float64Array initializes to 0
	}

	accumulateBatch(
		data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const m2 = this.values;
		const means = this.means;
		const counts = this.counts;
		const hasVal = this.hasValue;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				const x = Number(data[row]);

				counts[gid]!++;
				const delta = x - means[gid]!;
				means[gid]! += delta / counts[gid]!;
				const delta2 = x - means[gid]!;
				m2[gid]! += delta * delta2;
				hasVal[gid] = 1;
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				const x = Number(data[i]);

				counts[gid]!++;
				const delta = x - means[gid]!;
				means[gid]! += delta / counts[gid]!;
				const delta2 = x - means[gid]!;
				m2[gid]! += delta * delta2;
				hasVal[gid] = 1;
			}
		}
	}

	override finish(): ColumnBuffer {
		const col = new ColumnBuffer(
			this.outputDType.kind,
			this.size,
			this.outputDType.nullable,
		);
		for (let i = 0; i < this.size; i++) {
			if (this.hasValue[i] && this.counts[i]! >= 2) {
				// Sample std dev: sqrt(M2 / (n-1))
				col.set(i, Math.sqrt(this.values[i]! / (this.counts[i]! - 1)));
			} else {
				col.setNull(i, true);
			}
		}
		(col as unknown as { _length: number })._length = this.size;
		return col;
	}

	override resetState(): void {
		if (this.size > 0) {
			this.values.fill(0, 0, this.size);
			this.means.fill(0, 0, this.size);
			this.counts.fill(0, 0, this.size);
			this.hasValue.fill(0, 0, this.size);
		}
		this.size = 0;
	}
}

/** Vector Var (Welford's online algorithm) */
export class VectorVar extends VectorStd {
	override finish(): ColumnBuffer {
		const col = new ColumnBuffer(
			this.outputDType.kind,
			this.size,
			this.outputDType.nullable,
		);
		for (let i = 0; i < this.size; i++) {
			if (this.hasValue[i] && this.counts[i]! >= 2) {
				// Sample variance: M2 / (n-1)
				col.set(i, this.values[i]! / (this.counts[i]! - 1));
			} else {
				col.setNull(i, true);
			}
		}
		(col as unknown as { _length: number })._length = this.size;
		return col;
	}
}

/**
 * Vector Median.
 *
 * Uses contiguous Float64Array + Int32Array backing stores (values + group tags)
 * to store all values in two TypedArray allocations instead of one JS array per group.
 * At finish() time, values are scattered into per-group Float64Arrays for sorting.
 */
export class VectorMedian implements BatchAggregator {
	/** Packed values: all groups interleaved */
	private data: Float64Array = new Float64Array(4096);
	/** Parallel group-id tag for each value slot */
	private tags: Int32Array = new Int32Array(4096);
	/** Next write index into data/tags */
	private dataLen = 0;
	/** Per-group: value count */
	private groupCount: Int32Array = new Int32Array(256);
	private size = 0;
	readonly outputDType = DTypeFactory.float64;

	resize(numGroups: number): void {
		this.size = numGroups;
		if (numGroups > this.groupCount.length) {
			const newCount = new Int32Array(numGroups * 2);
			newCount.set(this.groupCount);
			this.groupCount = newCount;
		}
	}

	private appendValue(gid: number, value: number): void {
		if (this.dataLen >= this.data.length) {
			const newData = new Float64Array(this.data.length * 2);
			newData.set(this.data);
			this.data = newData;
			const newTags = new Int32Array(this.tags.length * 2);
			newTags.set(this.tags);
			this.tags = newTags;
		}
		this.data[this.dataLen] = value;
		this.tags[this.dataLen] = gid;
		this.dataLen++;
		const curCount = this.groupCount[gid];
		if (curCount !== undefined) this.groupCount[gid] = curCount + 1;
	}

	accumulateBatch(
		data: TypedArray | null,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		if (!data) return;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;
				this.appendValue(groupIds[i]!, Number(data[row]));
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;
				this.appendValue(groupIds[i]!, Number(data[i]));
			}
		}
	}

	finish(): ColumnBuffer {
		const col = new ColumnBuffer(this.outputDType.kind, this.size, this.outputDType.nullable);

		// Allocate per-group Float64Arrays and fill-pointer array
		const perGroup: Float64Array[] = new Array(this.size);
		const filled: Int32Array = new Int32Array(this.size);
		for (let g = 0; g < this.size; g++) {
			perGroup[g] = new Float64Array(this.groupCount[g]!);
		}

		// Scatter values into per-group arrays using stored group tags
		for (let i = 0; i < this.dataLen; i++) {
			const gid = this.tags[i]!;
			const arr = perGroup[gid];
			if (arr) {
				arr[filled[gid]!] = this.data[i]!;
				(filled as Int32Array)[gid]!++;
			}
		}

		// Compute median per group
		for (let g = 0; g < this.size; g++) {
			const cnt = this.groupCount[g]!;
			if (cnt === 0) {
				col.setNull(g, true);
				continue;
			}
			const arr = perGroup[g]!;
			arr.sort();
			const mid = Math.floor(arr.length / 2);
			const median = arr.length % 2 === 0
				? (arr[mid - 1]! + arr[mid]!) / 2
				: arr[mid]!;
			col.set(g, median);
		}
		(col as unknown as { _length: number })._length = this.size;
		return col;
	}

	resetState(): void {
		this.dataLen = 0;
		if (this.size > 0) {
			this.groupCount.fill(0, 0, this.size);
		}
		this.size = 0;
	}
}

/** Vector CountDistinct (tracks unique values per group). */
export class VectorCountDistinct implements BatchAggregator {
	private groupSets: Map<number, Set<number | string>> = new Map();
	private size = 0;
	readonly outputDType = DTypeFactory.int64;

	resize(numGroups: number): void {
		this.size = numGroups;
	}

	accumulateBatch(
		data: TypedArray | null,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
		dictionary?: import("../buffer/dictionary.ts").Dictionary | null,
	): void {
		if (!data) return;

		const isString = column && (column.kind === DTypeFactory.string.kind || column.kind === DTypeFactory.stringDict.kind);

		const isStringView = column && column.kind === DTypeKind.StringView;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;
				const gid = groupIds[i]!;
				let set = this.groupSets.get(gid);
				if (!set) { set = new Set(); this.groupSets.set(gid, set); }
				if (isString && dictionary) {
					set.add(dictionary.getString(data[row] as number)!);
				} else if (isStringView) {
					set.add((column as any as import("../buffer/string-view-column.ts").StringViewColumnBuffer).getString(row)!);
				} else {
					set.add(Number(data[row]));
				}
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;
				const gid = groupIds[i]!;
				let set = this.groupSets.get(gid);
				if (!set) { set = new Set(); this.groupSets.set(gid, set); }
				if (isString && dictionary) {
					set.add(dictionary.getString(data[i] as number)!);
				} else if (isStringView) {
					set.add((column as any as import("../buffer/string-view-column.ts").StringViewColumnBuffer).getString(i)!);
				} else {
					set.add(Number(data[i]));
				}
			}
		}
	}

	finish(): ColumnBuffer {
		const col = new ColumnBuffer(this.outputDType.kind, this.size, false);
		for (let gid = 0; gid < this.size; gid++) {
			col.set(gid, BigInt(this.groupSets.get(gid)?.size ?? 0));
		}
		(col as unknown as { _length: number })._length = this.size;
		return col;
	}

	resetState(): void {
		for (const set of this.groupSets.values()) set.clear();
		this.groupSets.clear();
		this.size = 0;
	}
}

/** Vector First (stores first non-null value per group) */
export class VectorFirst extends BaseVectorAggregator {
	protected declare values: Float64Array;
	readonly outputDType = DTypeFactory.float64;

	constructor() {
		super();
		this.values = new Float64Array(1024);
		this.hasValue = new Uint8Array(1024);
	}

	protected grow(newSize: number) {
		const newValues = new Float64Array(newSize);
		newValues.set(this.values);
		this.values = newValues;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(_start: number, _end: number) {
		// No special initialization needed
	}

	accumulateBatch(
		data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const vals = this.values;
		const hasVal = this.hasValue;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				if (!hasVal[gid]) {
					vals[gid] = Number(data[row]);
					hasVal[gid] = 1;
				}
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				if (!hasVal[gid]) {
					vals[gid] = Number(data[i]);
					hasVal[gid] = 1;
				}
			}
		}
	}
	// resetState() inherited from BaseVectorAggregator
}

/** Vector Last (stores last non-null value per group) */
export class VectorLast extends BaseVectorAggregator {
	protected declare values: Float64Array;
	readonly outputDType = DTypeFactory.float64;

	constructor() {
		super();
		this.values = new Float64Array(1024);
		this.hasValue = new Uint8Array(1024);
	}

	protected grow(newSize: number) {
		const newValues = new Float64Array(newSize);
		newValues.set(this.values);
		this.values = newValues;

		const newHasValue = new Uint8Array(newSize);
		newHasValue.set(this.hasValue);
		this.hasValue = newHasValue;
	}

	protected initialize(_start: number, _end: number) {
		// No special initialization needed
	}

	accumulateBatch(
		data: TypedArray,
		groupIds: Int32Array,
		count: number,
		selection: Uint32Array | null,
		column: ColumnBuffer | null,
	): void {
		const vals = this.values;
		const hasVal = this.hasValue;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				vals[gid] = Number(data[row]);
				hasVal[gid] = 1;
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				vals[gid] = Number(data[i]);
				hasVal[gid] = 1;
			}
		}
	}
}

/** Factory */
export function createVectorAggregator(aggType: AggType): BatchAggregator {
	switch (aggType) {
		case AggType.Sum:
			return new VectorSum();
		case AggType.Avg:
			return new VectorAvg();
		case AggType.Count:
			return new VectorCount();
		case AggType.CountAll:
			return new VectorCountAll();
		case AggType.Min:
			return new VectorMin();
		case AggType.Max:
			return new VectorMax();
		case AggType.First:
			return new VectorFirst();
		case AggType.Last:
			return new VectorLast();
		case AggType.Std:
			return new VectorStd();
		case AggType.Var:
			return new VectorVar();
		case AggType.Median:
			return new VectorMedian();
		case AggType.CountDistinct:
			return new VectorCountDistinct();
		default:
			throw new Error(`Vector aggregation not implemented for type ${aggType}`);
	}
}
