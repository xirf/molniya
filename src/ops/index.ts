/**
 * Public exports for the ops module.
 */

// Aggregation state
export {
	createAggState,
	AggType,
	type AggState,
	AvgState,
	CountAllState,
	CountState,
	FirstState,
	LastState,
	MaxState,
	MinState,
	SumState,
} from "./agg-state.ts";

// Aggregate operator
export {
	AggregateOperator,
	type AggSpec,
	aggregate,
} from "./aggregate.ts";
// Builder
export { from, PipelineBuilder } from "./builder.ts";
// Cast
export {
	type CastResult,
	castColumn,
} from "./cast.ts";
// Concat
export {
	concatChunks,
	validateConcatSchemas,
} from "./concat.ts";
export {
	countNulls,
	dropNullSelection,
	hasAnyNull,
} from "./drop-null.ts";
// Null handling
export {
	fillNullBackward,
	fillNullColumn,
	fillNullForward,
} from "./fill-null.ts";
// Filter
export { FilterOperator, filter } from "./filter.ts";
// GroupBy operator
export { GroupByOperator, type GroupByOptions, groupBy } from "./groupby.ts";
// Join
export {
	hashJoin,
	innerJoin,
	type JoinConfig,
	JoinType,
	leftJoin,
	crossProduct,
	streamingHashJoin,
} from "./join.ts";
// Limit
export { LimitOperator, limit } from "./limit.ts";
// Operator base
export {
	type Operator,
	type OperatorResult,
	opDone,
	opEmpty,
	opResult,
	PassthroughOperator,
	SimpleOperator,
} from "./operator.ts";
// Pipeline
export {
	Pipeline,
	type PipelineResult,
	pipeline,
} from "./pipeline.ts";
// Project
export {
	ProjectOperator,
	type ProjectSpec,
	project,
	projectWithRename,
} from "./project.ts";
// Sort
export {
	asc,
	desc,
	type SortKey,
	type SortOptions,
	SortExpression,
	SortOperator,
	sort,
} from "./sort.ts";
// String operations
export {
	padColumn,
	replaceColumn,
	substringColumn,
	toLowerColumn,
	toUpperColumn,
	trimColumn,
} from "./string-ops.ts";
// Transform
export {
	type ComputedColumn,
	TransformOperator,
	transform,
	withColumn,
} from "./transform.ts";
// Pushdown
export { compilePushdownFilter } from "./pushdown.ts";
// Chunk sizing
export { computeChunkSize, type ChunkSizingOptions } from "./chunk-sizing.ts";
// Unique/Deduplication
export {
	countUnique,
	uniqueSelection,
} from "./unique.ts";
