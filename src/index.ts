/**
 * Molniya - High-performance, stream-only, binary-level data manipulation for Bun
 *
 * Main entry point for the library.
 */

// Re-export buffer
export {
	Chunk,
	ColumnBuffer,
	columnBufferFromArray,
	createChunkFromArrays,
	createColumnBuffer,
	createDictionary,
	createEmptyChunk,
	Dictionary,
	NULL_INDEX,
	type DictIndex,
	type TypedArray,
} from "./buffer/index.ts";
// Re-export DataFrame
export {
	DataFrame,
	fromArrays,
	fromColumns,
	fromCsvString,
	fromRecords,
	range,
	type CsvReadOptions,
	type ParquetReadOptions,
	readCsv,
	readParquet,
} from "./dataframe/index.ts";
// Re-export expressions
export {
	add,
	and,
	applyPredicate,
	applyValue,
	avg,
	between,
	coalesce,
	// Compiler
	type CompiledPredicate,
	type CompiledValue,
	col,
	compilePredicate,
	compileValue,
	count,
	countDistinct,
	div,
	type Expr,
	ExprType,
	first,
	formatExpr,
	// Type inference
	type InferredType,
	inferExprType,
	isPredicateExpr,
	last,
	lit,
	max,
	median,
	min,
	mod,
	mul,
	neg,
	not,
	or,
	std,
	sub,
	sum,
	validateExpr,
	variance,
	when,
	WhenBuilder,
} from "./expr/index.ts";
// Re-export I/O
export {
	type CsvOptions,
	CsvParser,
	type CsvSchemaSpec,
	CsvSource,
	createCsvParser,
	ParquetReader,
	readCsvFile,
	readCsvString,
	MbfSource,
	readMbf,
} from "./io/index.ts";

// Re-export operators
export {
	AggregateOperator,
	type AggSpec,
	// Aggregation
	type AggState,
	AggType,
	aggregate,
	asc,
	type ComputedColumn,
	// Concat
	concatChunks,
	createAggState,
	desc,
	// Filter
	FilterOperator,
	filter,
	from,
	GroupByOperator,
	groupBy,
	hashJoin,
	innerJoin,
	type JoinConfig,
	// Join
	JoinType,
	// Limit
	LimitOperator,
	leftJoin,
	limit,
	// Base
	type Operator,
	type OperatorResult,
	opDone,
	opEmpty,
	opResult,
	PassthroughOperator,
	// Pipeline
	Pipeline,
	PipelineBuilder,
	type PipelineResult,
	// Project
	ProjectOperator,
	type ProjectSpec,
	pipeline,
	project,
	projectWithRename,
	SimpleOperator,
	type SortKey,
	// Sort
	SortOperator,
	sort,
	// Transform
	TransformOperator,
	transform,
	validateConcatSchemas,
	withColumn,
} from "./ops/index.ts";
// Re-export types
export {
	addColumn,
	compareSchemas,
	type ColumnDef,
	createSchema,
	DType,
	DTypeKind,
	type DTypeToTS,
	dropColumns,
	ErrorCode,
	err,
	formatSchema,
	getColumn,
	getColumnByIndex,
	getColumnIndex,
	getColumnNames,
	getDTypeSize,
	getErrorMessage,
	hasColumn,
	isSchemaEqual,
	isBigIntDType,
	isErr,
	isIntegerDType,
	isNumericDType,
	isOk,
	ok,
	type Result,
	renameColumn,
	type Schema,
	type SchemaSpec,
	selectColumns,
	validateSchemaSpec,
	unwrap,
	unwrapOr,
} from "./types/index.ts";
