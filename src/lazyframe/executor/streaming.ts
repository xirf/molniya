import * as fs from 'node:fs';
import type { DataFrame } from '../../dataframe/dataframe';
import { readBinaryBlocks } from '../../io/binary-format';
import type { ColumnarBatchIterator } from '../../io/columnar-batch';
import { streamCsvBatches } from '../../io/csv-streamer';
import { createMemoryBudget } from '../../memory/budget';
import type { Result } from '../../types/result';
import { analyzeRequiredColumns } from '../column-analyzer';
import type {
  DistinctPlan,
  FilterPlan,
  GroupByPlan,
  JoinPlan,
  PlanNode,
  ScanPlan,
  SelectPlan,
  SortPlan,
} from '../plan';
import { QueryPlan } from '../plan';
import {
  aggregateBatchesToDataFrame,
  blocksToBatchIterator,
  collectBatchesToDataFrame,
  distinctBatchesToDataFrame,
  externalMergeSortBatchesToDataFrame,
  filterBatchIterator,
  innerJoinBatchIterators,
  selectBatchIterator,
} from '../streaming';
import {
  buildSchemaSignature,
  cleanupStaleCacheFiles,
  createCacheKey,
  getCachePath,
  normalizeNullValues,
} from './cache';

export function flattenLinearPlan(plan: PlanNode): {
  scan: ScanPlan;
  operations: Array<FilterPlan | SelectPlan | GroupByPlan | DistinctPlan | SortPlan>;
} | null {
  const operations: Array<FilterPlan | SelectPlan | GroupByPlan | DistinctPlan | SortPlan> = [];
  let current: PlanNode = plan;

  while (current.type !== 'scan') {
    if (
      current.type === 'filter' ||
      current.type === 'select' ||
      current.type === 'groupby' ||
      current.type === 'distinct' ||
      current.type === 'sort'
    ) {
      operations.push(current);
      current = current.input;
      continue;
    }
    return null;
  }

  return {
    scan: current,
    operations: operations.reverse(),
  };
}

export async function executeStreamingPlan(
  plan: PlanNode,
): Promise<Result<DataFrame, Error> | null> {
  if (plan.type === 'join') {
    return await executeStreamingJoin(plan);
  }

  const flat = flattenLinearPlan(plan);
  if (!flat) return null;

  const { scan, operations } = flat;
  const schemaCount = Object.keys(scan.schema).length;
  const requiredColumns = analyzeRequiredColumns(plan);
  const shouldPrune = requiredColumns.size > 0 && requiredColumns.size < schemaCount;

  const cacheKey = shouldPrune
    ? createCacheKey({
        mode: 'prune',
        requiredColumns: Array.from(requiredColumns).sort(),
        schema: buildSchemaSignature(scan.schema, requiredColumns),
        delimiter: scan.delimiter ?? ',',
        hasHeader: scan.hasHeader ?? true,
        nullValues: normalizeNullValues(scan.nullValues),
      })
    : undefined;
  const cachePath = getCachePath(scan.path, cacheKey);
  cleanupStaleCacheFiles(scan.path);

  const baseIterator = await buildStreamingIterator(scan, requiredColumns, shouldPrune, cacheKey);
  if (!baseIterator) return null;

  let iterator: ColumnarBatchIterator = baseIterator;
  const pendingFilters: Array<{
    column: string;
    operator: FilterPlan['operator'];
    value: FilterPlan['value'];
  }> = [];

  for (let i = 0; i < operations.length; i++) {
    const op = operations[i];
    if (!op) continue;

    switch (op.type) {
      case 'filter':
        pendingFilters.push({
          column: op.column,
          operator: op.operator,
          value: op.value,
        });
        break;
      case 'select':
        if (pendingFilters.length > 0) {
          iterator = filterBatchIterator(iterator, pendingFilters);
          pendingFilters.length = 0;
        }
        iterator = selectBatchIterator(iterator, op.columns);
        break;
      case 'groupby':
        if (pendingFilters.length > 0) {
          iterator = filterBatchIterator(iterator, pendingFilters);
          pendingFilters.length = 0;
        }
        if (i !== operations.length - 1) {
          return null;
        }
        return await aggregateBatchesToDataFrame(iterator, op.groupKeys, op.aggregations);

      case 'distinct':
        if (pendingFilters.length > 0) {
          iterator = filterBatchIterator(iterator, pendingFilters);
          pendingFilters.length = 0;
        }
        if (i !== operations.length - 1) {
          return null;
        }
        return await distinctBatchesToDataFrame(iterator, op.subset);

      case 'sort':
        if (pendingFilters.length > 0) {
          iterator = filterBatchIterator(iterator, pendingFilters);
          pendingFilters.length = 0;
        }
        if (i !== operations.length - 1) {
          return null;
        }
        return await externalMergeSortBatchesToDataFrame(iterator, op.columns, op.directions, {
          runBytes: op.runBytes,
          tempDir: op.tempDir,
        });
    }
  }

  if (pendingFilters.length > 0) {
    iterator = filterBatchIterator(iterator, pendingFilters);
  }

  return await collectBatchesToDataFrame(iterator);
}

async function buildStreamingIterator(
  scan: ScanPlan,
  requiredColumns: Set<string>,
  shouldPrune: boolean,
  cacheKey?: string,
): Promise<ColumnarBatchIterator | null> {
  const cachePath = getCachePath(scan.path, cacheKey);
  cleanupStaleCacheFiles(scan.path);

  if (cachePath && fs.existsSync(cachePath)) {
    const cached = await readBinaryBlocks(cachePath);
    if (cached.ok) {
      return blocksToBatchIterator(cached.data.blocks, cached.data.schema);
    }
  }

  const streamResult = await streamCsvBatches(scan.path, {
    schema: scan.schema,
    delimiter: scan.delimiter ?? ',',
    hasHeader: scan.hasHeader ?? true,
    nullValues: scan.nullValues,
    dropInvalidRows: true,
    requiredColumns: shouldPrune ? requiredColumns : undefined,
    cachePath: cachePath ?? undefined,
    deleteCacheOnAbort: true,
    deleteCacheOnComplete: Boolean(cacheKey),
    memoryBudget: createMemoryBudget(),
  });

  if (!streamResult.ok) {
    return null;
  }

  return streamResult.data;
}

async function buildIteratorForPlan(
  plan: PlanNode,
  extraRequiredColumns: string[] = [],
): Promise<ColumnarBatchIterator | null> {
  const flat = flattenLinearPlan(plan);
  if (!flat) return null;

  const { scan, operations } = flat;
  if (
    operations.some((op) => op.type === 'groupby' || op.type === 'distinct' || op.type === 'sort')
  ) {
    return null;
  }

  const schemaCount = Object.keys(scan.schema).length;
  const requiredColumns = analyzeRequiredColumns(plan);
  for (const col of extraRequiredColumns) {
    requiredColumns.add(col);
  }
  const shouldPrune = requiredColumns.size > 0 && requiredColumns.size < schemaCount;

  const cacheKey = shouldPrune
    ? createCacheKey({
        mode: 'prune',
        requiredColumns: Array.from(requiredColumns).sort(),
        schema: buildSchemaSignature(scan.schema, requiredColumns),
        delimiter: scan.delimiter ?? ',',
        hasHeader: scan.hasHeader ?? true,
        nullValues: normalizeNullValues(scan.nullValues),
      })
    : undefined;

  let iterator = await buildStreamingIterator(scan, requiredColumns, shouldPrune, cacheKey);
  if (!iterator) return null;

  const pendingFilters: Array<{
    column: string;
    operator: FilterPlan['operator'];
    value: FilterPlan['value'];
  }> = [];

  for (const op of operations) {
    if (op.type === 'filter') {
      pendingFilters.push({ column: op.column, operator: op.operator, value: op.value });
      continue;
    }
    if (op.type === 'select') {
      if (pendingFilters.length > 0) {
        iterator = filterBatchIterator(iterator, pendingFilters);
        pendingFilters.length = 0;
      }
      iterator = selectBatchIterator(iterator, op.columns);
    }
  }

  if (pendingFilters.length > 0) {
    iterator = filterBatchIterator(iterator, pendingFilters);
  }

  return iterator;
}

async function executeStreamingJoin(plan: JoinPlan): Promise<Result<DataFrame, Error> | null> {
  const leftRequired = QueryPlan.getColumnOrder(plan.left);
  const rightRequired = QueryPlan.getColumnOrder(plan.right);

  const leftIterator = await buildIteratorForPlan(plan.left, [
    ...leftRequired,
    ...(plan.on ?? plan.leftOn ?? []),
  ]);
  const rightIterator = await buildIteratorForPlan(plan.right, [
    ...rightRequired,
    ...(plan.on ?? plan.rightOn ?? []),
  ]);
  if (!leftIterator || !rightIterator) return null;

  return await innerJoinBatchIterators(leftIterator, rightIterator, {
    on: plan.on,
    leftOn: plan.leftOn,
    rightOn: plan.rightOn,
    suffixes: plan.suffixes,
    how: plan.how,
  });
}
