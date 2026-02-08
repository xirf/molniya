/**
 * Public exports for the types module.
 */

export {
	DTYPE_ARRAY_CONSTRUCTORS,
	DTYPE_SIZES,
	DType,
	DTypeKind,
	type DTypeToTS,
	getDTypeSize,
	isBigIntDType,
	isIntegerDType,
	isNumericDType,
	toNullable,
} from "./dtypes.ts";

// Error handling
export {
	andThen,
	ERROR_MESSAGES,
	ErrorCode,
	err,
	getErrorMessage,
	isErr,
	isOk,
	mapResult,
	ok,
	type Result,
	unwrap,
	unwrapOr,
} from "./error.ts";

// Schema
export {
	addColumn,
	compareSchemas,
	type ColumnDef,
	createSchema,
	dropColumns,
	formatSchema,
	getColumn,
	getColumnByIndex,
	getColumnIndex,
	getColumnNames,
	hasColumn,
	isSchemaEqual,
	renameColumn,
	type Schema,
	type SchemaSpec,
	selectColumns,
	validateSchemaSpec,
} from "./schema.ts";
