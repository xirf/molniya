/** biome-ignore-all lint/style/noNonNullAssertion: Performance optimization */
/**
 * Join operator.
 *
 * Hash-based join implementation supporting inner, left, and right joins.
 */

import { Chunk } from "../buffer/chunk.ts";
import { ColumnBuffer } from "../buffer/column-buffer.ts";
import { createDictionary, type Dictionary } from "../buffer/dictionary.ts";
import { type DType, DTypeKind, toNullable } from "../types/dtypes.ts";
import { ErrorCode, err, ok, type Result } from "../types/error.ts";
import { createSchema, getColumnIndex, type Schema } from "../types/schema.ts";

/** Join type */
export enum JoinType {
	Inner = "inner",
	Left = "left",
	Right = "right",
	Semi = "semi",
	Anti = "anti",
}

/** Join configuration */
export interface JoinConfig {
	leftKey: string;
	rightKey: string;
	joinType?: JoinType;
	suffix?: string; // Suffix for conflicting column names (default: "_right")
}

/**
 * Perform a hash join between two sets of chunks.
 *
 * For streaming, the right side (build side) is fully materialized.
 * The left side is streamed through.
 */
export function hashJoin(
	leftChunks: Chunk[],
	leftSchema: Schema,
	rightChunks: Chunk[],
	rightSchema: Schema,
	config: JoinConfig,
): Result<{ chunks: Chunk[]; schema: Schema }> {
	const joinType = config.joinType ?? JoinType.Inner;
	const suffix = config.suffix ?? "_right";

	// Validate key columns
	const leftKeyResult = getColumnIndex(leftSchema, config.leftKey);
	if (leftKeyResult.error !== ErrorCode.None) {
		return err(ErrorCode.UnknownColumn);
	}
	const leftKeyIdx = leftKeyResult.value;

	const rightKeyResult = getColumnIndex(rightSchema, config.rightKey);
	if (rightKeyResult.error !== ErrorCode.None) {
		return err(ErrorCode.UnknownColumn);
	}
	const rightKeyIdx = rightKeyResult.value;

	// Build output schema
	const outputSchemaResult = buildJoinSchema(
		leftSchema,
		rightSchema,
		config.leftKey,
		config.rightKey,
		suffix,
		joinType,
	);
	if (outputSchemaResult.error !== ErrorCode.None) {
		return err(outputSchemaResult.error);
	}
	const outputSchema = outputSchemaResult.value;

	// Build hash table from right side
	const hashTable = buildHashTable(rightChunks, rightKeyIdx);

	// Track matched right rows for right/outer joins
	// Only needed for Right join
	const rightMatched =
		joinType === JoinType.Right ? new Set<string>() : undefined;

	// Process left side and produce output
	const outputChunks: Chunk[] = [];
	const leftDict =
		(leftChunks.length > 0 ? leftChunks[0]!.dictionary : null) ??
		createDictionary();

	for (const leftChunk of leftChunks) {
		const result = processLeftChunk(
			leftChunk,
			leftKeyIdx,
			rightChunks,
			rightKeyIdx,
			hashTable,
			outputSchema,
			leftSchema,
			rightSchema,
			joinType,
			rightMatched,
			config.leftKey,
		);

		if (result.rowCount > 0) {
			outputChunks.push(result);
		}
	}

	// For right join, add unmatched right rows
	if (joinType === JoinType.Right && rightMatched) {
		const unmatchedResult = addUnmatchedRight(
			rightChunks,
			rightKeyIdx,
			rightMatched,
			outputSchema,
			leftSchema,
			rightSchema,
			config.leftKey,
			leftDict,
		);

		if (unmatchedResult.rowCount > 0) {
			outputChunks.push(unmatchedResult);
		}
	}

	return ok({ chunks: outputChunks, schema: outputSchema });
}

/**
 * Perform a streaming hash join.
 * Right side (build) is materialized. Left side (probe) is streamed.
 */
export function streamingHashJoin(
	leftSource:  AsyncIterable<Chunk> | Iterable<Chunk>,
	leftSchema: Schema,
	rightChunks: Chunk[],
	rightSchema: Schema,
	config: JoinConfig,
): Result<{ stream: AsyncGenerator<Chunk>; schema: Schema }> {
	const joinType = config.joinType ?? JoinType.Inner;
	const suffix = config.suffix ?? "_right";

	// Validate key columns
	const leftKeyResult = getColumnIndex(leftSchema, config.leftKey);
	if (leftKeyResult.error !== ErrorCode.None) {
		return err(ErrorCode.UnknownColumn);
	}
	const leftKeyIdx = leftKeyResult.value;

	const rightKeyResult = getColumnIndex(rightSchema, config.rightKey);
	if (rightKeyResult.error !== ErrorCode.None) {
		return err(ErrorCode.UnknownColumn);
	}
	const rightKeyIdx = rightKeyResult.value;

	// Build output schema
	const outputSchemaResult = buildJoinSchema(
		leftSchema,
		rightSchema,
		config.leftKey,
		config.rightKey,
		suffix,
		joinType,
	);
	if (outputSchemaResult.error !== ErrorCode.None) {
		return err(outputSchemaResult.error);
	}
	const outputSchema = outputSchemaResult.value;

	// Build hash table from right side
	const hashTable = buildHashTable(rightChunks, rightKeyIdx);

	// Track matched right rows for right/outer joins
	const rightMatched =
		joinType === JoinType.Right ? new Set<string>() : undefined;

	// Generator function
	async function* generator(): AsyncGenerator<Chunk> {
		for await (const leftChunk of leftSource) {
			const result = processLeftChunk(
				leftChunk,
				leftKeyIdx,
				rightChunks,
				rightKeyIdx,
				hashTable,
				outputSchema,
				leftSchema,
				rightSchema,
				joinType,
				rightMatched,
				config.leftKey,
			);

			if (result.rowCount > 0) {
				yield result;
			}
		}

		// For right join, add unmatched right rows
		if (joinType === JoinType.Right && rightMatched) {
			// Issue: We need a left dictionary for string interning if left columns are involved
			// But we don't have access to left chunks anymore.
			// We can create a new temporary dictionary or reuse first chunk's if we captured it?
			// `addUnmatchedRight` needs `leftDict`.
			// We can use an empty dictionary or create one.
			const leftDict = createDictionary();

			const unmatchedResult = addUnmatchedRight(
				rightChunks,
				rightKeyIdx,
				rightMatched,
				outputSchema,
				leftSchema,
				rightSchema,
				config.leftKey,
				leftDict,
			);

			if (unmatchedResult.rowCount > 0) {
				yield unmatchedResult;
			}
		}
	}

	return ok({ stream: generator(), schema: outputSchema });
}

/**
 * Perform a cross join (cartesian product).
 */
export function crossProduct(
	leftChunks: Chunk[],
	leftSchema: Schema,
	rightChunks: Chunk[],
	rightSchema: Schema,
	suffix?: string,
): Result<{ chunks: Chunk[]; schema: Schema }> {
	const outputSchemaResult = buildJoinSchema(
		leftSchema,
		rightSchema,
		"", // No keys
		"",
		suffix || "_right",
		JoinType.Inner, // Schema struct similar to Inner but no keys skipped
	);
	if (outputSchemaResult.error !== ErrorCode.None) {
		return err(outputSchemaResult.error);
	}
	const outputSchema = outputSchemaResult.value;

	const outputChunks: Chunk[] = [];
	// const leftDict = (leftChunks.length > 0 ? leftChunks[0]!.dictionary : null) ?? createDictionary();

	for (const leftChunk of leftChunks) {
		for (const rightChunk of rightChunks) {
			// Create a chunk for every pair of left/right chunks
			// This is N*M
			// For every row in left, repeat for every row in right
			const result = processCrossChunk(
				leftChunk,
				rightChunk,
				outputSchema,
				leftSchema,
				rightSchema,
			);
			if (result.rowCount > 0) {
				outputChunks.push(result);
			}
		}
	}

	return ok({ chunks: outputChunks, schema: outputSchema });
}

function processCrossChunk(
	leftChunk: Chunk,
	rightChunk: Chunk,
	outputSchema: Schema,
	leftSchema: Schema,
	rightSchema: Schema,
): Chunk {
	const rowCount = leftChunk.rowCount * rightChunk.rowCount;
	const columns: ColumnBuffer[] = [];
	for (const col of outputSchema.columns) {
		columns.push(
			new ColumnBuffer(col.dtype.kind, rowCount, col.dtype.nullable),
		);
	}

	// Naive implementation: iterate left, then iterate right
	// Optimize: implement repeat() for column buffers for left values
	// and tile() for right values.

	// For now, simple loop
	for (let l = 0; l < leftChunk.rowCount; l++) {
		for (let r = 0; r < rightChunk.rowCount; r++) {
			// Append left cols
			let outIdx = 0;
			for (let c = 0; c < leftSchema.columnCount; c++) {
				const col = columns[outIdx++]!;
				if (leftChunk.isNull(c, l)) {
					col.appendNull();
				} else {
					col.append(leftChunk.getValue(c, l)!);
				}
			}
			// Append right cols
			for (let c = 0; c < rightSchema.columnCount; c++) {
				const col = columns[outIdx++]!;
				if (rightChunk.isNull(c, r)) {
					col.appendNull();
				} else {
					let val = rightChunk.getValue(c, r);
					// Handle string interning if needed (omitted for brevity, ideally share dict or intern)
					// Assuming simplified handling or shared dict logic
					// If we strictly follow, we need dictionary handling.
					// Reusing logic from appendLeftRow would be better but it's specific.
					// Let's just append.
					if (
						col.kind === DTypeKind.String &&
						rightChunk.dictionary &&
						leftChunk.dictionary
					) {
						const str = rightChunk.dictionary.getString(val as number);
						if (str !== undefined) val = leftChunk.dictionary.internString(str);
					}
					col.append(val!);
				}
			}
		}
	}
	return new Chunk(outputSchema, columns, leftChunk.dictionary);
}

/**
 * Build output schema for join.
 * For left joins, right columns become nullable.
 * For right joins, left columns become nullable.
 */
function buildJoinSchema(
	leftSchema: Schema,
	rightSchema: Schema,
	leftKey: string,
	rightKey: string,
	suffix: string,
	joinType: JoinType,
): Result<Schema> {
	// Semi/Anti joins only return left columns
	if (joinType === JoinType.Semi || joinType === JoinType.Anti) {
		return ok(leftSchema);
	}

	const schemaSpec: Record<string, DType> = {};
	const usedNames = new Set<string>();

	// Add all left columns (nullable for right joins)
	for (const col of leftSchema.columns) {
		const dtype =
			joinType === JoinType.Right ? toNullable(col.dtype) : col.dtype;
		schemaSpec[col.name] = dtype;
		usedNames.add(col.name);
	}

	// Add right columns (except the key if it has the same name)
	// Nullable for left joins
	for (const col of rightSchema.columns) {
		// Skip the join key if same name (except cross product where keys aren't used for skipping)
		if (leftKey && rightKey && col.name === rightKey && leftKey === rightKey) {
			continue;
		}

		let name = col.name;
		if (usedNames.has(name)) {
			name = col.name + suffix;
		}

		const dtype =
			joinType === JoinType.Left ? toNullable(col.dtype) : col.dtype;
		schemaSpec[name] = dtype;
	}

	return createSchema(schemaSpec);
}

/**
 * Build hash table from right chunks.
 * Maps key value (as string) to array of (chunkIdx, rowIdx) pairs.
 */
function buildHashTable(
	rightChunks: Chunk[],
	keyIdx: number,
): Map<string, Array<{ chunkIdx: number; rowIdx: number }>> {
	const table = new Map<string, Array<{ chunkIdx: number; rowIdx: number }>>();

	for (let c = 0; c < rightChunks.length; c++) {
		const chunk = rightChunks[c]!;
		for (let r = 0; r < chunk.rowCount; r++) {
			if (chunk.isNull(keyIdx, r)) continue;

			const keyValue = getKeyAsString(chunk, keyIdx, r);
			let entries = table.get(keyValue);
			if (!entries) {
				entries = [];
				table.set(keyValue, entries);
			}
			entries.push({ chunkIdx: c, rowIdx: r });
		}
	}

	return table;
}

/**
 * Get key value as string for hashing.
 */
function getKeyAsString(chunk: Chunk, colIdx: number, rowIdx: number): string {
	const dtype = chunk.schema.columns[colIdx]!.dtype;

	if (dtype.kind === DTypeKind.String) {
		return chunk.getStringValue(colIdx, rowIdx) ?? "";
	}

	return String(chunk.getValue(colIdx, rowIdx));
}

/**
 * Process left chunk and produce joined output.
 */
function processLeftChunk(
	leftChunk: Chunk,
	leftKeyIdx: number,
	rightChunks: Chunk[],
	rightKeyIdx: number,
	hashTable: Map<string, Array<{ chunkIdx: number; rowIdx: number }>>,
	outputSchema: Schema,
	leftSchema: Schema,
	rightSchema: Schema,
	joinType: JoinType,
	rightMatched: Set<string> | undefined,
	leftKey: string,
): Chunk {
	// Pre-allocate (may need to grow for multi-matches)
	const columns: ColumnBuffer[] = [];
	for (const col of outputSchema.columns) {
		columns.push(
			new ColumnBuffer(
				col.dtype.kind,
				leftChunk.rowCount * (joinType === JoinType.Inner ? 1 : 2), // Heuristic
				col.dtype.nullable,
			),
		);
	}

	for (let r = 0; r < leftChunk.rowCount; r++) {
		const isNullKey = leftChunk.isNull(leftKeyIdx, r);
		let matches: { chunkIdx: number; rowIdx: number }[] | undefined;

		if (!isNullKey) {
			const keyValue = getKeyAsString(leftChunk, leftKeyIdx, r);
			matches = hashTable.get(keyValue);
		}

		if (!matches || matches.length === 0) {
			// No Match
			if (joinType === JoinType.Left) {
				appendLeftRow(
					columns,
					leftChunk,
					r,
					leftSchema,
					rightSchema,
					null,
					null,
					rightKeyIdx,
					leftKey,
					joinType,
				);
			} else if (joinType === JoinType.Anti) {
				// Anti join: include if NO match
				appendLeftRow(
					columns,
					leftChunk,
					r,
					leftSchema,
					rightSchema,
					null,
					null,
					rightKeyIdx,
					leftKey,
					joinType,
				);
			}
			continue;
		}

		// Match Found
		if (joinType === JoinType.Anti) {
			// Anti join: skip if match found
			continue;
		}

		if (joinType === JoinType.Semi) {
			// Semi join: include ONCE if match found, don't include right cols
			appendLeftRow(
				columns,
				leftChunk,
				r,
				leftSchema,
				rightSchema,
				null,
				null,
				rightKeyIdx,
				leftKey,
				joinType,
			);
			continue;
		}

		// Add a row for each match (Inner, Left, Right)
		for (const match of matches) {
			const rightChunk = rightChunks[match.chunkIdx]!;
			appendLeftRow(
				columns,
				leftChunk,
				r,
				leftSchema,
				rightSchema,
				rightChunk,
				match,
				rightKeyIdx,
				leftKey,
				joinType,
			);
			if (rightMatched) rightMatched.add(`${match.chunkIdx}:${match.rowIdx}`);
		}
	}

	// Trim buffers to actual length
	// for(const col of columns) {
	// We need direct access to set length or rely on constructor sizing?
	// ColumnBuffer usually handles size but if we overestimated significantly we might want to trim.
	// For now, assuming standard behavior (length is tracked internally).
	// Actually ColumnBuffer tracks `length` property separately from capacity.
	// }

	return new Chunk(outputSchema, columns, leftChunk.dictionary);
}

/**
 * Append a joined row to output columns.
 */
function appendLeftRow(
	columns: ColumnBuffer[],
	leftChunk: Chunk,
	leftRow: number,
	leftSchema: Schema,
	rightSchema: Schema,
	rightChunk: Chunk | null,
	rightMatch: { chunkIdx: number; rowIdx: number } | null,
	rightKeyIdx: number,
	leftKey: string,
	joinType: JoinType,
): void {
	let outIdx = 0;

	// Add left columns
	for (let c = 0; c < leftSchema.columnCount; c++) {
		const col = columns[outIdx++]!;
		if (leftChunk.isNull(c, leftRow)) {
			col.appendNull();
		} else {
			const value = leftChunk.getValue(c, leftRow);
			col.append(value!);
		}
	}

	// Semi/Anti joins stop here (only left columns)
	if (joinType === JoinType.Semi || joinType === JoinType.Anti) {
		return;
	}

	// Add right columns (except duplicate key)
	for (let c = 0; c < rightSchema.columnCount; c++) {
		const colName = rightSchema.columns[c]!.name;
		// Skip if same key column name
		if (c === rightKeyIdx && colName === leftKey) {
			continue;
		}

		const col = columns[outIdx++]!;
		const rightColDef = rightSchema.columns[c]!;

		if (rightChunk && rightMatch) {
			if (rightChunk.isNull(c, rightMatch.rowIdx)) {
				col.appendNull();
			} else {
				let value = rightChunk.getValue(c, rightMatch.rowIdx);

				if (
					rightColDef.dtype.kind === DTypeKind.String &&
					rightChunk.dictionary &&
					leftChunk.dictionary
				) {
					const str = rightChunk.dictionary.getString(value as number);
					if (str !== undefined) {
						value = leftChunk.dictionary.internString(str);
					}
				}

				col.append(value!);
			}
		} else {
			col.appendNull();
		}
	}
}

/**
 * Add unmatched right rows for right join.
 */
function addUnmatchedRight(
	rightChunks: Chunk[],
	rightKeyIdx: number,
	rightMatched: Set<string>,
	outputSchema: Schema,
	leftSchema: Schema,
	rightSchema: Schema,
	leftKey: string,
	dictionary: Dictionary,
): Chunk {
	const columns: ColumnBuffer[] = [];
	for (const col of outputSchema.columns) {
		columns.push(new ColumnBuffer(col.dtype.kind, 1024, col.dtype.nullable));
	}

	for (let c = 0; c < rightChunks.length; c++) {
		const chunk = rightChunks[c]!;
		for (let r = 0; r < chunk.rowCount; r++) {
			const key = `${c}:${r}`;
			if (rightMatched.has(key)) continue;

			// Add null left columns + right row
			let outIdx = 0;

			// Left columns as null
			for (let lc = 0; lc < leftSchema.columnCount; lc++) {
				columns[outIdx++]!.appendNull();
			}

			// Right columns
			for (let rc = 0; rc < rightSchema.columnCount; rc++) {
				const colName = rightSchema.columns[rc]!.name;
				if (rc === rightKeyIdx && colName === leftKey) {
					continue;
				}

				const col = columns[outIdx++]!;
				if (chunk.isNull(rc, r)) {
					col.appendNull();
				} else {
					let value = chunk.getValue(rc, r);
					if (col.kind === DTypeKind.String && chunk.dictionary && dictionary) {
						const str = chunk.dictionary.getString(value as number);
						if (str !== undefined) {
							value = dictionary.internString(str);
						}
					}
					col.append(value!);
				}
			}
		}
	}

	return new Chunk(outputSchema, columns, dictionary);
}

/**
 * Convenience function for inner join.
 */
export function innerJoin(
	leftChunks: Chunk[],
	leftSchema: Schema,
	rightChunks: Chunk[],
	rightSchema: Schema,
	leftKey: string,
	rightKey: string,
): Result<{ chunks: Chunk[]; schema: Schema }> {
	return hashJoin(leftChunks, leftSchema, rightChunks, rightSchema, {
		leftKey,
		rightKey,
		joinType: JoinType.Inner,
	});
}

/**
 * Convenience function for left join.
 */
export function leftJoin(
	leftChunks: Chunk[],
	leftSchema: Schema,
	rightChunks: Chunk[],
	rightSchema: Schema,
	leftKey: string,
	rightKey: string,
): Result<{ chunks: Chunk[]; schema: Schema }> {
	return hashJoin(leftChunks, leftSchema, rightChunks, rightSchema, {
		leftKey,
		rightKey,
		joinType: JoinType.Left,
	});
}
