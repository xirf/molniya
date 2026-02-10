/**
 * Aggregation state interface.
 *
 * Each aggregation function has a state object that accumulates values
 * across chunks and produces a final result.
 */
/** biome-ignore-all lint/style/noNonNullAssertion: Intenional, data is impossible to be null */

import { type DType, DType as DTypeFactory } from "../types/dtypes.ts";

/** Aggregation state for accumulating values across chunks */
export interface AggState {
	/** Reset state for a new group */
	reset(): void;

	/** Accumulate a value (null values are skipped) */
	accumulate(value: number | bigint | null): void;

	/** Get the current aggregated result */
	result(): number | bigint | null;

	/** Output data type */
	readonly outputDType: DType;
}

/** Sum aggregation */
export class SumState implements AggState {
	private sum: number = 0;
	private hasValue: boolean = false;
	readonly outputDType = DTypeFactory.float64;

	reset(): void {
		this.sum = 0;
		this.hasValue = false;
	}

	accumulate(value: number | bigint | null): void {
		if (value === null) return;
		this.sum += typeof value === "bigint" ? Number(value) : value;
		this.hasValue = true;
	}

	result(): number | null {
		return this.hasValue ? this.sum : null;
	}
}

/** Average aggregation */
export class AvgState implements AggState {
	private sum: number = 0;
	private count: number = 0;
	readonly outputDType = DTypeFactory.float64;

	reset(): void {
		this.sum = 0;
		this.count = 0;
	}

	accumulate(value: number | bigint | null): void {
		if (value === null) return;
		this.sum += typeof value === "bigint" ? Number(value) : value;
		this.count++;
	}

	result(): number | null {
		return this.count > 0 ? this.sum / this.count : null;
	}
}

/** Count aggregation */
export class CountState implements AggState {
	private count: bigint = 0n;
	readonly outputDType = DTypeFactory.int64;

	reset(): void {
		this.count = 0n;
	}

	accumulate(value: number | bigint | null): void {
		// Count all non-null values
		if (value !== null) {
			this.count++;
		}
	}

	result(): bigint {
		return this.count;
	}
}

/** Count all rows (including nulls) */
export class CountAllState implements AggState {
	private count: bigint = 0n;
	readonly outputDType = DTypeFactory.int64;

	reset(): void {
		this.count = 0n;
	}

	accumulate(_value: number | bigint | null): void {
		this.count++;
	}

	result(): bigint {
		return this.count;
	}
}

/** Min aggregation */
export class MinState implements AggState {
	private min: number = Infinity;
	private hasValue: boolean = false;
	readonly outputDType: DType;

	constructor(inputDType: DType) {
		this.outputDType = inputDType;
	}

	reset(): void {
		this.min = Infinity;
		this.hasValue = false;
	}

	accumulate(value: number | bigint | null): void {
		if (value === null) return;
		const num = typeof value === "bigint" ? Number(value) : value;
		if (num < this.min) {
			this.min = num;
			this.hasValue = true;
		}
	}

	result(): number | null {
		return this.hasValue ? this.min : null;
	}
}

/** Max aggregation */
export class MaxState implements AggState {
	private max: number = -Infinity;
	private hasValue: boolean = false;
	readonly outputDType: DType;

	constructor(inputDType: DType) {
		this.outputDType = inputDType;
	}

	reset(): void {
		this.max = -Infinity;
		this.hasValue = false;
	}

	accumulate(value: number | bigint | null): void {
		if (value === null) return;
		const num = typeof value === "bigint" ? Number(value) : value;
		if (num > this.max) {
			this.max = num;
			this.hasValue = true;
		}
	}

	result(): number | null {
		return this.hasValue ? this.max : null;
	}
}

/** First non-null value */
export class FirstState implements AggState {
	private first: number | bigint | null = null;
	private hasValue: boolean = false;
	readonly outputDType: DType;

	constructor(inputDType: DType) {
		this.outputDType = inputDType;
	}

	reset(): void {
		this.first = null;
		this.hasValue = false;
	}

	accumulate(value: number | bigint | null): void {
		if (!this.hasValue && value !== null) {
			this.first = value;
			this.hasValue = true;
		}
	}

	result(): number | bigint | null {
		return this.first;
	}
}

/** Last non-null value */
export class LastState implements AggState {
	private last: number | bigint | null = null;
	readonly outputDType: DType;

	constructor(inputDType: DType) {
		this.outputDType = inputDType;
	}

	reset(): void {
		this.last = null;
	}

	accumulate(value: number | bigint | null): void {
		if (value !== null) {
			this.last = value;
		}
	}

	result(): number | bigint | null {
		return this.last;
	}
}

/** Standard deviation aggregation (sample, using Welford's online algorithm) */
export class StdState implements AggState {
	private count: number = 0;
	private mean: number = 0;
	private m2: number = 0;
	readonly outputDType = DTypeFactory.float64;

	reset(): void {
		this.count = 0;
		this.mean = 0;
		this.m2 = 0;
	}

	accumulate(value: number | bigint | null): void {
		if (value === null) return;
		const num = typeof value === "bigint" ? Number(value) : value;
		this.count++;
		const delta = num - this.mean;
		this.mean += delta / this.count;
		const delta2 = num - this.mean;
		this.m2 += delta * delta2;
	}

	result(): number | null {
		if (this.count < 2) return null;
		return Math.sqrt(this.m2 / (this.count - 1)); // Sample std dev
	}
}

/** Variance aggregation (sample, using Welford's online algorithm) */
export class VarState implements AggState {
	private count: number = 0;
	private mean: number = 0;
	private m2: number = 0;
	readonly outputDType = DTypeFactory.float64;

	reset(): void {
		this.count = 0;
		this.mean = 0;
		this.m2 = 0;
	}

	accumulate(value: number | bigint | null): void {
		if (value === null) return;
		const num = typeof value === "bigint" ? Number(value) : value;
		this.count++;
		const delta = num - this.mean;
		this.mean += delta / this.count;
		const delta2 = num - this.mean;
		this.m2 += delta * delta2;
	}

	result(): number | null {
		if (this.count < 2) return null;
		return this.m2 / (this.count - 1); // Sample variance
	}
}

/** Median aggregation (collects all values) */
export class MedianState implements AggState {
	private values: number[] = [];
	readonly outputDType = DTypeFactory.float64;

	reset(): void {
		this.values = [];
	}

	accumulate(value: number | bigint | null): void {
		if (value === null) return;
		this.values.push(typeof value === "bigint" ? Number(value) : value);
	}

	result(): number | null {
		if (this.values.length === 0) return null;
		this.values.sort((a, b) => a - b);
		const mid = Math.floor(this.values.length / 2);
		if (this.values.length % 2 === 0) {
			return (this.values[mid - 1]! + this.values[mid]!) / 2;
		}
		return this.values[mid]!;
	}
}

/** Count distinct aggregation */
export class CountDistinctState implements AggState {
	private seen: Set<number | bigint | string> = new Set();
	readonly outputDType = DTypeFactory.int64;

	reset(): void {
		this.seen.clear();
	}

	accumulate(value: number | bigint | null): void {
		if (value === null) return;
		this.seen.add(value as number | bigint | string);
	}

	result(): bigint {
		return BigInt(this.seen.size);
	}
}

/** Factory function to create aggregation state from expression type */
export function createAggState(aggType: AggType, inputDType?: DType): AggState {
	switch (aggType) {
		case AggType.Sum:
			return new SumState();
		case AggType.Avg:
			return new AvgState();
		case AggType.Count:
			return new CountState();
		case AggType.CountAll:
			return new CountAllState();
		case AggType.Min:
			return new MinState(inputDType ?? DTypeFactory.float64);
		case AggType.Max:
			return new MaxState(inputDType ?? DTypeFactory.float64);
		case AggType.First:
			return new FirstState(inputDType ?? DTypeFactory.float64);
		case AggType.Last:
			return new LastState(inputDType ?? DTypeFactory.float64);
		case AggType.Std:
			return new StdState();
		case AggType.Var:
			return new VarState();
		case AggType.Median:
			return new MedianState();
		case AggType.CountDistinct:
			return new CountDistinctState();
	}
}

/** Aggregation types */
export enum AggType {
	Sum = 0,
	Avg = 1,
	Count = 2,
	CountAll = 3,
	Min = 4,
	Max = 5,
	First = 6,
	Last = 7,
	Std = 8,
	Var = 9,
	Median = 10,
	CountDistinct = 11,
}
