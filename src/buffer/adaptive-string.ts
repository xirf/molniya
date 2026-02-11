/**
 * Adaptive string column that automatically chooses between Dictionary and StringView
 * based on cardinality sampling.
 *
 * Key principles:
 * 1. Start with StringView (O(1) insertion, handles any cardinality)
 * 2. Sample first N strings to estimate cardinality
 * 3. If cardinality is low enough, offer dictionary conversion
 * 4. Never automatically switch during live operations (avoids O(N) spikes)
 * 5. Allow user to force mode via DType hints
 */

import {
	createDictionary,
	type Dictionary,
	type DictIndex,
} from "./dictionary.ts";
import { SharedStringBuffer, StringView } from "./string-view.ts";
import { DTypeKind, type DType } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";

/** Sampling configuration */
const DEFAULT_SAMPLE_SIZE = 1000;
const DEFAULT_CARDINALITY_THRESHOLD = 0.1; // 10%
const MIN_STRINGS_FOR_DICT = 100; // Don't consider dict until we have enough data

/** String encoding modes */
export enum StringEncoding {
	/** Auto-adaptive (starts as StringView) */
	Adaptive = "adaptive",
	/** Explicit dictionary encoding */
	Dictionary = "dictionary",
	/** Explicit view encoding */
	View = "view",
}

/** Cardinality analysis result */
interface CardinalityStats {
	totalStrings: number;
	uniqueStrings: number;
	cardinality: number; // ratio of unique to total
	avgLength: number;
	estimatedMemoryDict: number; // estimated memory with dictionary
	estimatedMemoryView: number; // estimated memory with view
}

/**
 * Adaptive string column that chooses optimal encoding based on data characteristics.
 */
export class AdaptiveStringColumn {
	/** Current encoding mode */
	private mode: StringEncoding;
	/** Force a specific encoding (overrides adaptive logic) */
	private forcedMode: StringEncoding | null = null;

	/** Dictionary storage (if using dictionary mode) */
	private dictionary: Dictionary | null = null;
	private dictIndices: Uint32Array | null = null;

	/** StringView storage (if using view mode) */
	private stringView: StringView | null = null;

	/** Shared resources */
	private sharedBuffer: SharedStringBuffer | null = null;
	private sharedDict: Dictionary | null = null;

	/** Sampling state */
	private sampleSet: Set<string>;
	private totalSampleLength: number;
	private sampleSize: number;
	private cardinalityThreshold: number;
	private samplingComplete: boolean;

	/** Column metadata */
	private count: number;
	private capacity: number;
	private nullable: boolean;
	private nullBitmap: Uint8Array | null;

	constructor(
		capacity: number,
		dtype: DType,
		options: {
			sampleSize?: number;
			cardinalityThreshold?: number;
			sharedBuffer?: SharedStringBuffer;
			sharedDict?: Dictionary;
		} = {},
	) {
		this.capacity = capacity;
		this.nullable = dtype.nullable;
		this.count = 0;

		// Initialize sampling
		this.sampleSize = options.sampleSize ?? DEFAULT_SAMPLE_SIZE;
		this.cardinalityThreshold =
			options.cardinalityThreshold ?? DEFAULT_CARDINALITY_THRESHOLD;
		this.sampleSet = new Set();
		this.totalSampleLength = 0;
		this.samplingComplete = false;

		// Set up shared resources
		this.sharedBuffer = options.sharedBuffer ?? null;
		this.sharedDict = options.sharedDict ?? null;

		// Determine initial mode from dtype
		this.mode = this.determineModeFromDType(dtype.kind);
		if (this.mode !== StringEncoding.Adaptive) {
			this.forcedMode = this.mode;
			this.samplingComplete = true; // Skip sampling for forced modes
		}

		// Initialize null bitmap if needed
		this.nullBitmap = this.nullable
			? new Uint8Array(Math.ceil(capacity / 8))
			: null;

		// Initialize storage based on mode
		this.initializeStorage();
	}

	/**
	 * Append a string to the column.
	 */
	append(str: string): Result<void> {
		if (this.count >= this.capacity) {
			return err(ErrorCode.BufferFull);
		}

		// Handle null bitmap
		if (this.nullBitmap) {
			this.setNull(this.count, false);
		}

		// Sample for cardinality analysis (if not complete)
		if (!this.samplingComplete) {
			this.addToSample(str);
		}

		// Delegate to appropriate storage
		const result =
			this.mode === StringEncoding.Dictionary
				? this.appendToDictionary(str)
				: this.appendToStringView(str);

		if (result.error === ErrorCode.None) {
			this.count++;
		}

		return result;
	}

	/**
	 * Append null value.
	 */
	appendNull(): Result<void> {
		if (this.count >= this.capacity) {
			return err(ErrorCode.BufferFull);
		}

		if (!this.nullable) {
			return err(ErrorCode.TypeMismatch);
		}

		this.setNull(this.count, true);

		// Append null to the underlying storage
		const result =
			this.mode === StringEncoding.Dictionary
				? this.appendToDictionary("") // Dummy value, null bitmap takes precedence
				: this.appendNullToStringView(); // Call StringView's appendNull

		if (result.error === ErrorCode.None) {
			this.count++;
		}

		return result;
	}

	/**
	 * Get string at index.
	 */
	getString(index: number): string | undefined {
		if (index >= this.count || this.isNull(index)) {
			return undefined;
		}

		return this.mode === StringEncoding.Dictionary
			? this.getStringFromDictionary(index)
			: this.getStringFromView(index);
	}

	/**
	 * Check if value at index is null.
	 */
	isNull(index: number): boolean {
		if (!this.nullBitmap) return false;

		const byteIdx = Math.floor(index / 8);
		const bitIdx = index % 8;
		return (this.nullBitmap[byteIdx]! & (1 << bitIdx)) !== 0;
	}

	/**
	 * Get current cardinality statistics.
	 * Only available after sampling is complete.
	 */
	getCardinalityStats(): CardinalityStats | null {
		if (!this.samplingComplete) return null;

		const totalStrings = Math.max(this.sampleSet.size, this.count);
		const uniqueStrings = this.sampleSet.size;
		const cardinality = uniqueStrings / totalStrings;
		const avgLength = this.totalSampleLength / uniqueStrings;

		// Rough memory estimates
		const estimatedMemoryDict = uniqueStrings * avgLength + this.count * 4; // dict + indices
		const estimatedMemoryView = this.count * (avgLength + 12); // data + offset/length

		return {
			totalStrings,
			uniqueStrings,
			cardinality,
			avgLength,
			estimatedMemoryDict,
			estimatedMemoryView,
		};
	}

	/**
	 * Check if dictionary encoding would be beneficial.
	 * Returns recommendation based on current sampling data.
	 */
	shouldUseDictionary(): boolean | null {
		const stats = this.getCardinalityStats();
		if (!stats || stats.totalStrings < MIN_STRINGS_FOR_DICT) {
			return null; // Not enough data to decide
		}

		return (
			stats.cardinality < this.cardinalityThreshold &&
			stats.estimatedMemoryDict < stats.estimatedMemoryView
		);
	}

	/**
	 * Convert current StringView to Dictionary encoding.
	 * This is an expensive O(N) operation - use carefully!
	 */
	convertToDictionary(): Result<void> {
		if (this.mode === StringEncoding.Dictionary) {
			return ok(undefined); // Already dictionary
		}

		if (!this.stringView) {
			return err(ErrorCode.InvalidState);
		}

		// Create dictionary and indices array
		const dict = this.sharedDict ?? createDictionary();
		const indices = new Uint32Array(this.capacity);

		// Convert all existing strings
		for (let i = 0; i < this.count; i++) {
			if (!this.isNull(i)) {
				const str = this.stringView.getString(i);
				if (str !== undefined) {
					indices[i] = dict.internString(str);
				}
			}
		}

		// Update state
		this.dictionary = dict;
		this.dictIndices = indices;
		this.stringView = null; // Release StringView
		this.mode = StringEncoding.Dictionary;

		return ok(undefined);
	}

	/**
	 * Current number of strings.
	 */
	get length(): number {
		return this.count;
	}

	/**
	 * Current encoding mode.
	 */
	get encoding(): StringEncoding {
		return this.mode;
	}

	/**
	 * Whether sampling is complete.
	 */
	get isSamplingComplete(): boolean {
		return this.samplingComplete;
	}

	/**
	 * Get access to underlying storage for advanced operations.
	 */
	getInternals(): {
		dictionary?: Dictionary;
		indices?: Uint32Array;
		stringView?: StringView;
	} {
		return {
			dictionary: this.dictionary ?? undefined,
			indices: this.dictIndices ?? undefined,
			stringView: this.stringView ?? undefined,
		};
	}

	/**
	 * Get the underlying StringView (if using View encoding).
	 */
	getView(): StringView | null {
		return this.stringView;
	}

	/**
	 * Get the underlying Dictionary (if using Dictionary encoding).
	 */
	getDictionary(): Dictionary | null {
		return this.dictionary;
	}

	/**
	 * Get the column buffer of indices (if using Dictionary encoding).
	 * Returns a view that looks like a ColumnBuffer for compatibility.
	 */
	getColumnBuffer(): {
		data: Uint32Array;
		length: number;
		kind: DTypeKind;
		isNull: (i: number) => boolean;
	} | null {
		if (!this.dictIndices) return null;

		return {
			data: this.dictIndices,
			length: this.count,
			kind: DTypeKind.String, // Use String for compatibility with existing string-ops.ts
			isNull: (i: number) => this.isNull(i),
		};
	}

	/**
	 * Replace the underlying StringView with a new one.
	 * Used by string operations that transform the data.
	 */
	replaceView(newView: StringView): void {
		if (
			this.mode !== StringEncoding.View &&
			this.mode !== StringEncoding.Adaptive
		) {
			throw new Error("Cannot replace view when not in View mode");
		}
		this.stringView = newView;
		// Update count to match new view
		this.count = newView.length;
	}

	/**
	 * Replace the underlying Dictionary with a new one.
	 * Used by string operations that transform the dictionary.
	 */
	replaceDictionary(newDict: Dictionary): void {
		if (this.mode !== StringEncoding.Dictionary) {
			throw new Error("Cannot replace dictionary when not in Dictionary mode");
		}
		this.dictionary = newDict;
	}

	// Private methods

	private determineModeFromDType(kind: DTypeKind): StringEncoding {
		switch (kind) {
			case DTypeKind.StringDict:
				return StringEncoding.Dictionary;
			case DTypeKind.StringView:
				return StringEncoding.View;
			case DTypeKind.String:
			default:
				return StringEncoding.Adaptive; // Default to adaptive
		}
	}

	private initializeStorage(): void {
		if (this.mode === StringEncoding.Dictionary) {
			this.dictionary = this.sharedDict ?? createDictionary();
			this.dictIndices = new Uint32Array(this.capacity);
		} else {
			// StringView or Adaptive (starts with StringView)
			this.stringView = new StringView(
				this.capacity,
				this.nullable, // Use same nullable setting as the column
				this.sharedBuffer ?? undefined,
			);
		}
	}

	private addToSample(str: string): void {
		if (this.samplingComplete) return;

		// Add to sample set
		if (!this.sampleSet.has(str)) {
			this.sampleSet.add(str);
			this.totalSampleLength += str.length;
		}

		// Check if sampling is complete
		if (
			this.sampleSet.size >= this.sampleSize ||
			this.count >= this.sampleSize
		) {
			this.samplingComplete = true;

			// For adaptive mode, decide whether to suggest dictionary
			if (this.mode === StringEncoding.Adaptive && this.shouldUseDictionary()) {
				// Could emit event/callback here for user to decide
				// For now, we keep using StringView but mark recommendation
			}
		}
	}

	private appendToDictionary(str: string): Result<void> {
		if (!this.dictionary || !this.dictIndices) {
			return err(ErrorCode.InvalidState);
		}

		const index = this.dictionary.internString(str);
		this.dictIndices[this.count] = index;
		return ok(undefined);
	}

	private appendToStringView(str: string): Result<void> {
		if (!this.stringView) {
			return err(ErrorCode.InvalidState);
		}

		const result = this.stringView.append(str);
		if (result.error !== ErrorCode.None) {
			return { value: undefined, error: result.error };
		}
		return ok(undefined);
	}

	private appendNullToStringView(): Result<void> {
		if (!this.stringView) {
			return err(ErrorCode.InvalidState);
		}

		const result = this.stringView.appendNull();
		if (result.error !== ErrorCode.None) {
			return { value: undefined, error: result.error };
		}
		return ok(undefined);
	}

	private getStringFromDictionary(index: number): string | undefined {
		if (!this.dictionary || !this.dictIndices) {
			return undefined;
		}

		const dictIndex = this.dictIndices[index]!;
		return this.dictionary.getString(dictIndex);
	}

	private getStringFromView(index: number): string | undefined {
		return this.stringView?.getString(index);
	}

	private setNull(index: number, isNull: boolean): void {
		if (!this.nullBitmap) return;

		const byteIdx = Math.floor(index / 8);
		const bitIdx = index % 8;

		if (isNull) {
			this.nullBitmap[byteIdx]! |= 1 << bitIdx;
		} else {
			this.nullBitmap[byteIdx]! &= ~(1 << bitIdx);
		}
	}
}

/**
 * Create an adaptive string column with the specified configuration.
 */
export function createAdaptiveStringColumn(
	capacity: number,
	dtype: DType,
	options?: {
		sampleSize?: number;
		cardinalityThreshold?: number;
		sharedBuffer?: SharedStringBuffer;
		sharedDict?: Dictionary;
	},
): AdaptiveStringColumn {
	return new AdaptiveStringColumn(capacity, dtype, options);
}
