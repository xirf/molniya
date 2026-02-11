/**
 * Enhanced column buffer that supports adaptive string storage.
 * 
 * Extends the basic ColumnBuffer to handle:
 * - Adaptive string columns (Dictionary vs StringView)
 * - Shared resource management
 * - String-specific operations
 */

import { ColumnBuffer } from "./column-buffer.ts";
import { AdaptiveStringColumn, StringEncoding } from "./adaptive-string.ts";
import { StringResourceHandle } from "./string-resources.ts";
import { DTypeKind, type DType } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";

/** Enhanced column buffer that can handle any data type including adaptive strings */
export class EnhancedColumnBuffer<K extends DTypeKind = DTypeKind> {
	/** The data type kind */
	readonly kind: K;
	/** Maximum capacity */
	readonly capacity: number;
	/** Whether values can be null */
	readonly nullable: boolean;

	/** Basic column buffer for non-string types */
	private basicBuffer: ColumnBuffer<K> | null = null;
	
	/** Adaptive string column for string types */
	private stringColumn: AdaptiveStringColumn | null = null;
	
	/** Resource handle for shared resources */
	private resourceHandle: StringResourceHandle | null = null;

	constructor(
		kind: K, 
		capacity: number, 
		nullable: boolean = false,
		options: {
			/** For shared dictionaries across similar columns */
			sharedDictionaryId?: string;
			/** For shared string buffers */
			sharedBufferId?: string;
			/** Tags for resource discovery */
			resourceTags?: string[];
			/** String-specific options */
			cardinalityThreshold?: number;
			sampleSize?: number;
		} = {}
	) {
		this.kind = kind;
		this.capacity = capacity;
		this.nullable = nullable;

		// Create appropriate storage based on type
		if (this.isStringType(kind)) {
			this.initializeStringStorage(kind, options);
		} else {
			this.basicBuffer = new ColumnBuffer(kind, capacity, nullable);
		}
	}

	/**
	 * Append a value to the column.
	 */
	append(value: any): Result<void> {
		if (this.stringColumn) {
			// Handle string value
			if (typeof value === "string") {
				return this.stringColumn.append(value);
			} else if (value === null || value === undefined) {
				return this.stringColumn.appendNull();
			} else {
				// Convert to string
				return this.stringColumn.append(String(value));
			}
		} else if (this.basicBuffer) {
			// Handle non-string value using basic buffer
			return this.appendToBasicBuffer(value);
		}

		return err(ErrorCode.InvalidState);
	}

	/**
	 * Get value at index.
	 */
	get(index: number): any {
		if (this.stringColumn) {
			return this.stringColumn.getString(index);
		} else if (this.basicBuffer) {
			return this.getFromBasicBuffer(index);
		}
		return undefined;
	}

	/**
	 * Check if value at index is null.
	 */
	isNull(index: number): boolean {
		if (this.stringColumn) {
			return this.stringColumn.isNull(index);
		} else if (this.basicBuffer) {
			return this.basicBuffer.isNull(index);
		}
		return false;
	}

	/**
	 * Set value at index to null.
	 */
	setNull(index: number, isNull: boolean): void {
		if (this.stringColumn) {
			// AdaptiveStringColumn doesn't support setNull on existing indices
			// This would require extending the interface
			return;
		} else if (this.basicBuffer) {
			this.basicBuffer.setNull(index, isNull);
		}
	}

	/**
	 * Get current length.
	 */
	get length(): number {
		if (this.stringColumn) {
			return this.stringColumn.length;
		} else if (this.basicBuffer) {
			return this.basicBuffer.length;
		}
		return 0;
	}

	/**
	 * Get remaining capacity.
	 */
	get available(): number {
		return this.capacity - this.length;
	}

	/**
	 * For string columns, get the current encoding being used.
	 */
	getStringEncoding(): StringEncoding | null {
		return this.stringColumn?.encoding ?? null;
	}

	/**
	 * For string columns, get cardinality statistics.
	 */
	getStringStats(): any {
		return this.stringColumn?.getCardinalityStats() ?? null;
	}

	/**
	 * For string columns, check if dictionary encoding is recommended.
	 */
	shouldUseDictionary(): boolean | null {
		return this.stringColumn?.shouldUseDictionary() ?? null;
	}

	/**
	 * For string columns, convert to dictionary encoding.
	 * This is an expensive O(N) operation!
	 */
	convertToDictionary(): Result<void> {
		if (!this.stringColumn) {
			return err(ErrorCode.TypeMismatch);
		}
		return this.stringColumn.convertToDictionary();
	}

	/**
	 * Get access to underlying storage for advanced operations.
	 */
	getInternals(): {
		basicBuffer?: ColumnBuffer<K>;
		stringColumn?: AdaptiveStringColumn;
	} {
		return {
			basicBuffer: this.basicBuffer ?? undefined,
			stringColumn: this.stringColumn ?? undefined
		};
	}

	/**
	 * Dispose of resources.
	 */
	dispose(): void {
		this.resourceHandle?.dispose();
	}

	// Private methods

	private isStringType(kind: DTypeKind): boolean {
		return kind === DTypeKind.String || 
		       kind === DTypeKind.StringDict || 
		       kind === DTypeKind.StringView;
	}

	private initializeStringStorage(
		kind: DTypeKind, 
		options: {
			sharedDictionaryId?: string;
			sharedBufferId?: string;
			resourceTags?: string[];
			cardinalityThreshold?: number;
			sampleSize?: number;
		}
	): void {
		this.resourceHandle = new StringResourceHandle();

		const dtype: DType = { kind, nullable: this.nullable };
		
		const stringOptions: any = {
			cardinalityThreshold: options.cardinalityThreshold,
			sampleSize: options.sampleSize
		};

		// Set up shared resources if requested
		if (options.sharedDictionaryId) {
			stringOptions.sharedDict = this.resourceHandle.getDictionary(
				options.sharedDictionaryId,
				options.resourceTags
			);
		}

		if (options.sharedBufferId) {
			stringOptions.sharedBuffer = this.resourceHandle.getBuffer(
				options.sharedBufferId,
				undefined,
				options.resourceTags
			);
		}

		this.stringColumn = new AdaptiveStringColumn(
			this.capacity,
			dtype,
			stringOptions
		);
	}

	private appendToBasicBuffer(value: any): Result<void> {
		if (!this.basicBuffer) {
			return err(ErrorCode.InvalidState);
		}

		const index = this.basicBuffer.length;
		
		if (value === null || value === undefined) {
			if (!this.nullable) {
				return err(ErrorCode.TypeMismatch);
			}
			this.basicBuffer.setLength(index + 1);
			this.basicBuffer.setNull(index, true);
			return ok(undefined);
		}

		// Set the value based on the type
		const data = this.basicBuffer.data;
		
		if (this.kind === DTypeKind.Boolean) {
			(data as Uint8Array)[index] = value ? 1 : 0;
		} else if (this.kind === DTypeKind.Timestamp || this.kind === DTypeKind.Int64 || this.kind === DTypeKind.UInt64) {
			(data as BigInt64Array | BigUint64Array)[index] = BigInt(value);
		} else {
			(data as any)[index] = Number(value);
		}

		this.basicBuffer.setLength(index + 1);
		return ok(undefined);
	}

	private getFromBasicBuffer(index: number): any {
		if (!this.basicBuffer || index >= this.basicBuffer.length) {
			return undefined;
		}

		if (this.basicBuffer.isNull(index)) {
			return null;
		}

		const data = this.basicBuffer.data;
		
		if (this.kind === DTypeKind.Boolean) {
			return (data as Uint8Array)[index] === 1;
		} else if (this.kind === DTypeKind.Timestamp || this.kind === DTypeKind.Int64 || this.kind === DTypeKind.UInt64) {
			return (data as BigInt64Array | BigUint64Array)[index];
		} else {
			return (data as any)[index];
		}
	}
}

/**
 * Create an enhanced column buffer with the specified configuration.
 */
export function createEnhancedColumnBuffer<K extends DTypeKind>(
	kind: K,
	capacity: number,
	nullable: boolean = false,
	options?: {
		sharedDictionaryId?: string;
		sharedBufferId?: string;
		resourceTags?: string[];
		cardinalityThreshold?: number;
		sampleSize?: number;
	}
): EnhancedColumnBuffer<K> {
	return new EnhancedColumnBuffer(kind, capacity, nullable, options);
}