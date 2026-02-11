/** biome-ignore-all lint/style/noNonNullAssertion: Performance optimization */
/**
 * Sort operator.
 *
 * In-memory sorting for bounded datasets.
 * Supports multi-column sorting with ascending/descending order.
 */

import { Chunk } from "../buffer/chunk.ts";
import { ColumnBuffer } from "../buffer/column-buffer.ts";
import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import { getColumnIndex, type Schema } from "../types/schema.ts";
import { type Operator, type OperatorResult, opEmpty } from "./operator.ts";

/** Sort order specification */
export interface SortKey {
	column: string;
	descending?: boolean;
	nullsFirst?: boolean;
}

/**
 * Sort operator that buffers all input and sorts on finish.
 */
export class SortOperator implements Operator {
	readonly name = "Sort";
	readonly outputSchema: Schema;
	private readonly sortKeys: SortKey[];
	private readonly columnIndices: number[];
	private bufferedChunks: Chunk[] = [];

	private constructor(
		schema: Schema,
		sortKeys: SortKey[],
		columnIndices: number[],
	) {
		this.outputSchema = schema;
		this.sortKeys = sortKeys;
		this.columnIndices = columnIndices;
	}

	static create(schema: Schema, sortKeys: SortKey[]): Result<SortOperator> {
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

		return ok(new SortOperator(schema, sortKeys, columnIndices));
	}

	process(chunk: Chunk): Result<OperatorResult> {
		// Buffer all chunks for sorting at the end
		this.bufferedChunks.push(chunk);
		return ok(opEmpty());
	}

	finish(): Result<OperatorResult> {
		if (this.bufferedChunks.length === 0) {
			return ok(opEmpty());
		}

		// Collect all rows into arrays for sorting
		const totalRows = this.bufferedChunks.reduce(
			(sum, c) => sum + c.rowCount,
			0,
		);
		if (totalRows === 0) {
			return ok(opEmpty());
		}

		// Build flat packed arrays instead of object-per-row (3.1 optimization)
		// For N rows: 6 bytes each (Uint16 chunk + Uint32 row) vs 32+ bytes per object
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

		// Indirect sort: sort a permutation array that indexes into the packed arrays
		const perm = new Uint32Array(totalRows);
		for (let i = 0; i < totalRows; i++) perm[i] = i;

		perm.sort((ai, bi) => {
			const chunkA = this.bufferedChunks[chunkIndices[ai]!]!;
			const chunkB = this.bufferedChunks[chunkIndices[bi]!]!;
			const rowA = rowIndices[ai]!;
			const rowB = rowIndices[bi]!;

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
					const strA = chunkA.getStringValue(colIdx, rowA) ?? "";
					const strB = chunkB.getStringValue(colIdx, rowB) ?? "";
					cmp = strA < strB ? -1 : strA > strB ? 1 : 0;
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
		});

		// Build output chunk from sorted permutation
		const outputChunk = this.buildSortedChunk(perm, chunkIndices, rowIndices);

		this.bufferedChunks = []; // Clear buffer
		return ok({
			chunk: outputChunk,
			done: true,
			hasMore: false,
		});
	}

	reset(): void {
		this.bufferedChunks = [];
	}

	private buildSortedChunk(
		perm: Uint32Array,
		chunkIndices: Uint16Array,
		rowIndices: Uint32Array,
	): Chunk {
		const schema = this.outputSchema;
		const dictionary = this.bufferedChunks[0]!.dictionary;

		// Create new column buffers
		const columns: ColumnBuffer[] = [];
		for (const col of schema.columns) {
			columns.push(
				new ColumnBuffer(col.dtype.kind, perm.length, col.dtype.nullable),
			);
		}

		// Copy rows in sorted order
		for (let i = 0; i < perm.length; i++) {
			const p = perm[i]!;
			const srcChunk = this.bufferedChunks[chunkIndices[p]!]!;
			const srcRow = rowIndices[p]!;
			for (let c = 0; c < schema.columnCount; c++) {
				const destCol = columns[c]!;
				if (srcChunk.isNull(c, srcRow)) {
					destCol.appendNull();
				} else {
					const value = srcChunk.getValue(c, srcRow);
					destCol.append(value!);
				}
			}
		}

		return new Chunk(schema, columns, dictionary);
	}
}

/**
 * Create a sort operator.
 */
/**
 * Sort expression builder for configuring sort options.
 */
export class SortExpression {
	constructor(
		public readonly column: string,
		public readonly descending: boolean = false,
		private readonly _nullsFirst?: boolean,
	) {}

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
	const sortKeys: SortKey[] = keys.map(toSortKey);
	return SortOperator.create(schema, sortKeys);
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
