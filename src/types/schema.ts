/**
 * Schema definitions for DataFrame columns.
 *
 * A schema maps column names to their DTypes and provides
 * metadata needed for binary layout (offsets, sizes).
 */

import { type DType, DTypeKind, getDTypeSize } from "./dtypes.ts";
import { ErrorCode, err, ok, type Result } from "./error.ts";

/** Single column definition */
export interface ColumnDef {
	readonly name: string;
	readonly dtype: DType;
	readonly offset: number; // Byte offset within a row (for row-based layout)
}

/** Schema describes the structure of a DataFrame */
export interface Schema {
	readonly columns: readonly ColumnDef[];
	readonly columnMap: ReadonlyMap<string, number>; // name -> index
	readonly rowSize: number; // Total bytes per row
	readonly columnCount: number;
}

/** Schema definition input format (user-facing) */
export type SchemaSpec = Record<string, DType>;

/**
 * Create a Schema from a specification object.
 *
 * Example:
 *   const schema = createSchema({
 *     id: DType.int32,
 *     name: DType.string,
 *     amount: DType.float64,
 *   });
 */
export function createSchema(spec: SchemaSpec): Result<Schema> {
	const entries = Object.entries(spec);

	if (entries.length === 0) {
		return err(ErrorCode.EmptySchema);
	}

	const columns: ColumnDef[] = [];
	const columnMap = new Map<string, number>();
	let offset = 0;

	for (const [i, [name, dtype]] of entries.entries()) {
		// Validate column name
		if (!isValidColumnName(name)) {
			return err(ErrorCode.InvalidColumnName);
		}

		// Check for duplicates
		if (columnMap.has(name)) {
			return err(ErrorCode.DuplicateColumn);
		}

		const size = getDTypeSize(dtype);

		columns.push({
			name,
			dtype,
			offset,
		});

		columnMap.set(name, i);
		offset += size;
	}

	return ok({
		columns,
		columnMap,
		rowSize: offset,
		columnCount: columns.length,
	});
}

/** Validate a schema spec without constructing a Schema instance. */
export function validateSchemaSpec(spec: SchemaSpec): Result<void> {
	const result = createSchema(spec);
	return result.error === ErrorCode.None
		? ok(undefined)
		: err(result.error);
}

/** Compare two schemas for exact equality (name, order, dtype). */
export function compareSchemas(a: Schema, b: Schema): Result<void> {
	if (a.columnCount !== b.columnCount) {
		return err(ErrorCode.SchemaMismatch);
	}

	for (let i = 0; i < a.columnCount; i++) {
		const colA = a.columns[i];
		const colB = b.columns[i];
		if (!colA || !colB) return err(ErrorCode.SchemaMismatch);
		if (colA.name !== colB.name) return err(ErrorCode.SchemaMismatch);
		if (colA.dtype.kind !== colB.dtype.kind) return err(ErrorCode.SchemaMismatch);
		if (colA.dtype.nullable !== colB.dtype.nullable) {
			return err(ErrorCode.SchemaMismatch);
		}
	}

	return ok(undefined);
}

/** Convenience boolean equality check for schemas. */
export function isSchemaEqual(a: Schema, b: Schema): boolean {
	return compareSchemas(a, b).error === ErrorCode.None;
}

/** Validate column name (non-empty, no special chars) */
function isValidColumnName(name: string): boolean {
	if (name.length === 0 || name.length > 256) {
		return false;
	}
	// Allow alphanumeric, underscore, and common chars
	return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name);
}

/** Get column definition by name */
export function getColumn(schema: Schema, name: string): Result<ColumnDef> {
	const index = schema.columnMap.get(name);
	if (index === undefined) {
		return err(ErrorCode.UnknownColumn);
	}
	const col = schema.columns[index];
	if (!col) {
		return err(ErrorCode.UnknownColumn);
	}
	return ok(col);
}

/** Get column definition by index */
export function getColumnByIndex(
	schema: Schema,
	index: number,
): Result<ColumnDef> {
	if (index < 0 || index >= schema.columnCount) {
		return err(ErrorCode.InvalidOffset);
	}
	const col = schema.columns[index];
	if (!col) {
		return err(ErrorCode.InvalidOffset);
	}
	return ok(col);
}

/** Get the index of a column by name */
export function getColumnIndex(schema: Schema, name: string): Result<number> {
	const index = schema.columnMap.get(name);
	if (index === undefined) {
		return err(ErrorCode.UnknownColumn);
	}
	return ok(index);
}

/** Check if schema contains a column */
export function hasColumn(schema: Schema, name: string): boolean {
	return schema.columnMap.has(name);
}

/** Get column names as array */
export function getColumnNames(schema: Schema): string[] {
	return schema.columns.map((col) => col.name);
}

/** Create a new schema with only selected columns */
export function selectColumns(schema: Schema, names: string[]): Result<Schema> {
	const spec: SchemaSpec = {};

	for (const name of names) {
		const colResult = getColumn(schema, name);
		if (colResult.error !== ErrorCode.None) {
			return err(colResult.error);
		}
		spec[name] = colResult.value.dtype;
	}

	return createSchema(spec);
}

/** Create a new schema with a column renamed */
export function renameColumn(
	schema: Schema,
	oldName: string,
	newName: string,
): Result<Schema> {
	if (!hasColumn(schema, oldName)) {
		return err(ErrorCode.UnknownColumn);
	}

	if (!isValidColumnName(newName)) {
		return err(ErrorCode.InvalidColumnName);
	}

	if (oldName !== newName && hasColumn(schema, newName)) {
		return err(ErrorCode.DuplicateColumn);
	}

	const spec: SchemaSpec = {};
	for (const col of schema.columns) {
		const name = col.name === oldName ? newName : col.name;
		spec[name] = col.dtype;
	}

	return createSchema(spec);
}

/** Create a new schema with an additional column */
export function addColumn(
	schema: Schema,
	name: string,
	dtype: DType,
): Result<Schema> {
	if (hasColumn(schema, name)) {
		return err(ErrorCode.DuplicateColumn);
	}

	if (!isValidColumnName(name)) {
		return err(ErrorCode.InvalidColumnName);
	}

	const spec: SchemaSpec = {};
	for (const col of schema.columns) {
		spec[col.name] = col.dtype;
	}
	spec[name] = dtype;

	return createSchema(spec);
}

/** Create a new schema without specified columns */
export function dropColumns(schema: Schema, names: string[]): Result<Schema> {
	const dropSet = new Set(names);

	// Verify all columns to drop exist
	for (const name of names) {
		if (!hasColumn(schema, name)) {
			return err(ErrorCode.UnknownColumn);
		}
	}

	const spec: SchemaSpec = {};
	for (const col of schema.columns) {
		if (!dropSet.has(col.name)) {
			spec[col.name] = col.dtype;
		}
	}

	if (Object.keys(spec).length === 0) {
		return err(ErrorCode.EmptySchema);
	}

	return createSchema(spec);
}

/** Map DTypeKind to string name for debugging */
const DTYPE_KIND_NAMES: Record<DTypeKind, string> = {
	[DTypeKind.Int8]: "Int8",
	[DTypeKind.Int16]: "Int16",
	[DTypeKind.Int32]: "Int32",
	[DTypeKind.Int64]: "Int64",
	[DTypeKind.UInt8]: "UInt8",
	[DTypeKind.UInt16]: "UInt16",
	[DTypeKind.UInt32]: "UInt32",
	[DTypeKind.UInt64]: "UInt64",
	[DTypeKind.Float32]: "Float32",
	[DTypeKind.Float64]: "Float64",
	[DTypeKind.Boolean]: "Boolean",
	[DTypeKind.String]: "String",
	[DTypeKind.Date]: "Date",
	[DTypeKind.Timestamp]: "Timestamp",
};

/** Format schema as string for debugging */
export function formatSchema(schema: Schema): string {
	const lines = schema.columns.map((col) => {
		const nullStr = col.dtype.nullable ? "?" : "";
		const kindName = DTYPE_KIND_NAMES[col.dtype.kind];
		return `  ${col.name}: ${kindName}${nullStr}`;
	});
	return `Schema(${schema.columnCount} columns, ${schema.rowSize} bytes/row):\n${lines.join("\n")}`;
}
