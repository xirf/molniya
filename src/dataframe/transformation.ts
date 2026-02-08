/* TRANSFORMATION METHODS
/*-----------------------------------------------------
/* Add computed columns to DataFrame
/* ==================================================== */

import { ColumnBuffer } from "../buffer/column-buffer.ts";
import { Chunk } from "../buffer/chunk.ts";
import { createDictionary, type Dictionary } from "../buffer/dictionary.ts";
import type { Expr } from "../expr/ast.ts";
import { ColumnRef } from "../expr/builders.ts";
import { type ComputedColumn, transform } from "../ops/index.ts";
import { DTypeKind } from "../types/dtypes.ts";
import { ErrorCode } from "../types/error.ts";
import type { Schema } from "../types/schema.ts";
import type { DataFrame } from "./core.ts";

function buildChunkFromRows(
	rows: Record<string, unknown>[],
	schema: Schema,
	dictionary: Dictionary,
): Chunk {
	const rowCount = rows.length;
	const columnBuffers: ColumnBuffer<DTypeKind>[] = [];

	for (const colDef of schema.columns) {
		const kind = colDef.dtype.kind;
		const nullable = colDef.dtype.nullable;
		const buffer = new ColumnBuffer(kind, rowCount, nullable);
		const colName = colDef.name;

		for (let i = 0; i < rowCount; i++) {
			const value = rows[i]?.[colName];

			if (value === null || value === undefined) {
				buffer.setNull(i, true);
				continue;
			}

			switch (kind) {
				case DTypeKind.String: {
					const dictIdx = dictionary.internString(String(value));
					buffer.set(i, dictIdx as never);
					break;
				}
				case DTypeKind.Boolean: {
					buffer.set(i, (value ? 1 : 0) as never);
					break;
				}
				case DTypeKind.Timestamp: {
					if (value instanceof Date) {
						buffer.set(i, BigInt(value.getTime()) as never);
					} else if (typeof value === "bigint") {
						buffer.set(i, value as never);
					} else {
						buffer.set(i, BigInt(value as number) as never);
					}
					break;
				}
				case DTypeKind.Date: {
					if (value instanceof Date) {
						buffer.set(
							i,
							Math.floor(value.getTime() / 86400000) as never,
						);
					} else {
						buffer.set(i, Number(value) as never);
					}
					break;
				}
				default:
					buffer.set(i, value as number | bigint as never);
			}
		}

		buffer.setLength(rowCount);
		columnBuffers.push(buffer);
	}

	return new Chunk(schema, columnBuffers, dictionary);
}

export function addTransformMethods(df: typeof DataFrame.prototype) {
	df.withColumn = function (name: string, expr: Expr | ColumnRef): DataFrame {
		const e = expr instanceof ColumnRef ? expr.toExpr() : expr;
		const result = transform(this.currentSchema(), [{ name, expr: e }]);
		if (result.error !== ErrorCode.None) {
			throw new Error(`WithColumn error: ${result.error}`);
		}
		return this.withOperator(result.value);
	};

	df.withColumns = function (
		columns: ComputedColumn[] | Record<string, Expr | ColumnRef>,
	): DataFrame {
		let cols: ComputedColumn[];
		if (Array.isArray(columns)) {
			cols = columns;
		} else {
			cols = Object.entries(columns).map(([name, expr]) => ({
				name,
				expr: expr instanceof ColumnRef ? expr.toExpr() : expr,
			}));
		}

		const result = transform(this.currentSchema(), cols);
		if (result.error !== ErrorCode.None) {
			throw new Error(`WithColumns error: ${result.error}`);
		}
		return this.withOperator(result.value);
	};

	df.shuffle = async function (): Promise<DataFrame> {
		const rows = await this.toArray();
		if (rows.length <= 1) return this;

		const shuffled = rows.slice();
		for (let i = shuffled.length - 1; i > 0; i--) {
			const j = Math.floor(Math.random() * (i + 1));
			// biome-ignore lint/style/noNonNullAssertion: Index bounds enforced
			[shuffled[i], shuffled[j]] = [shuffled[j]!, shuffled[i]!];
		}

		const dictionary = createDictionary();
		const chunk = buildChunkFromRows(
			shuffled,
			this.currentSchema(),
			dictionary,
		);
		return (this.constructor as typeof DataFrame).fromChunks(
			[chunk],
			this.currentSchema(),
			dictionary,
		);
	};

	df.sample = async function (fraction: number): Promise<DataFrame> {
		if (fraction <= 0) {
			return (this.constructor as typeof DataFrame).empty(
				this.currentSchema(),
				this._dictionary,
			);
		}
		if (fraction >= 1) {
			return this;
		}

		const rows = await this.toArray();
		const sampled = rows.filter(() => Math.random() < fraction);

		const dictionary = createDictionary();
		const chunk = buildChunkFromRows(
			sampled,
			this.currentSchema(),
			dictionary,
		);
		return (this.constructor as typeof DataFrame).fromChunks(
			[chunk],
			this.currentSchema(),
			dictionary,
		);
	};

	df.explode = async function (column: string): Promise<DataFrame> {
		const schema = this.currentSchema();
		if (!schema.columnMap.has(column)) {
			throw new Error(`Explode error: Column '${column}' not found`);
		}

		const rows = await this.toArray();
		const exploded: Record<string, unknown>[] = [];

		for (const row of rows) {
			const raw = row[column];
			let items: unknown[] | null = null;

			if (Array.isArray(raw)) {
				items = raw;
			} else if (typeof raw === "string") {
				try {
					const parsed = JSON.parse(raw);
					if (Array.isArray(parsed)) items = parsed;
				} catch {
					// Ignore parse errors; treat as scalar
				}
			}

			if (items) {
				for (const item of items) {
					exploded.push({ ...row, [column]: item });
				}
			} else {
				exploded.push(row);
			}
		}

		const dictionary = createDictionary();
		const chunk = buildChunkFromRows(exploded, schema, dictionary);
		return (this.constructor as typeof DataFrame).fromChunks(
			[chunk],
			schema,
			dictionary,
		);
	};
}
