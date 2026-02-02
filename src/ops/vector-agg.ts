/**
 * Vectorized Aggregation Primitives.
 *
 * Provides batch processing for aggregations, operating on arrays of values
 * and group IDs instead of single value/state pairs.
 */
/** biome-ignore-all lint/style/noNonNullAssertion: Performance critical inner loops */

import { ColumnBuffer, type TypedArray } from "../buffer/column-buffer.ts";
import { type DType, DType as DTypeFactory } from "../types/dtypes.ts";
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
	): void;

	/** Finalize and return results as a column buffer */
	finish(): ColumnBuffer;

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

		// We can iterate and set.
		// Optimization: Batch copy if possible, but null handling requires loop for now.
		for (let i = 0; i < count; i++) {
			if (this.hasValue[i]) {
				// @ts-expect-error - copying typed array elements
				col.set(i, this.values[i]);
			} else {
				col.setNull(i, true);
			}
		}
		// Hack: manually set length
		(col as unknown as { _length: number })._length = count;
		return col;
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
		// Ensure "hasValue" is all set for counts (0 is a valid count)
		// Actually per spec, Count returns 0 if empty? or Null?
		// SQL says Count is 0 if no rows.
		// But standard AggState implementation returns 0n.
		for (let i = 0; i < numGroups; i++) this.hasValue[i] = 1;
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

/** Vector Median (collects all values per group) */
export class VectorMedian implements BatchAggregator {
	private groupValues: Map<number, number[]> = new Map();
	private size: number = 0;
	readonly outputDType = DTypeFactory.float64;

	resize(numGroups: number): void {
		this.size = numGroups;
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

				const gid = groupIds[i]!;
				let arr = this.groupValues.get(gid);
				if (!arr) {
					arr = [];
					this.groupValues.set(gid, arr);
				}
				arr.push(Number(data[row]));
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				let arr = this.groupValues.get(gid);
				if (!arr) {
					arr = [];
					this.groupValues.set(gid, arr);
				}
				arr.push(Number(data[i]));
			}
		}
	}

	finish(): ColumnBuffer {
		const col = new ColumnBuffer(
			this.outputDType.kind,
			this.size,
			this.outputDType.nullable,
		);

		for (let gid = 0; gid < this.size; gid++) {
			const values = this.groupValues.get(gid);
			if (!values || values.length === 0) {
				col.setNull(gid, true);
			} else {
				values.sort((a, b) => a - b);
				const mid = Math.floor(values.length / 2);
				if (values.length % 2 === 0) {
					col.set(gid, (values[mid - 1]! + values[mid]!) / 2);
				} else {
					col.set(gid, values[mid]!);
				}
			}
		}

		(col as unknown as { _length: number })._length = this.size;
		return col;
	}
}

/** Vector CountDistinct (tracks unique values per group) */
export class VectorCountDistinct implements BatchAggregator {
	private groupSets: Map<number, Set<number | bigint | string>> = new Map();
	private size: number = 0;
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
	): void {
		if (!data) return;

		if (selection) {
			for (let i = 0; i < count; i++) {
				const row = selection[i]!;
				if (column?.isNull(row)) continue;

				const gid = groupIds[i]!;
				let set = this.groupSets.get(gid);
				if (!set) {
					set = new Set();
					this.groupSets.set(gid, set);
				}
				set.add(data[row] as number | bigint);
			}
		} else {
			for (let i = 0; i < count; i++) {
				if (column?.isNull(i)) continue;

				const gid = groupIds[i]!;
				let set = this.groupSets.get(gid);
				if (!set) {
					set = new Set();
					this.groupSets.set(gid, set);
				}
				set.add(data[i] as number | bigint);
			}
		}
	}

	finish(): ColumnBuffer {
		const col = new ColumnBuffer(this.outputDType.kind, this.size, false);

		for (let gid = 0; gid < this.size; gid++) {
			const set = this.groupSets.get(gid);
			col.set(gid, BigInt(set?.size ?? 0));
		}

		(col as unknown as { _length: number })._length = this.size;
		return col;
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
