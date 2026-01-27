import type { DataFrame } from '../../dataframe/dataframe';
import { groupby } from '../../dataframe/groupby';
import { filter, select } from '../../dataframe/operations';
import { unique } from '../../dataframe/row-ops';
import { sortDataFrame } from '../../dataframe/sort';
import type { Result } from '../../types/result';
import { err, ok } from '../../types/result';
import { type OptimizedPlan, explainPlan, optimizePlan as optimizeQueryPlan } from '../optimizer';
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
import { applyCacheRetention } from './cache';
import { executeScan, executeScanWithPruning } from './scan';

/**
 * Find the scan node in a plan tree
 */
function findScanNode(plan: PlanNode): ScanPlan | null {
  if (plan.type === 'scan') {
    return plan as ScanPlan;
  }

  switch (plan.type) {
    case 'filter':
    case 'select':
    case 'groupby':
    case 'distinct':
    case 'sort':
      return findScanNode(
        (plan as FilterPlan | SelectPlan | GroupByPlan | DistinctPlan | SortPlan).input,
      );
    case 'join':
      return null;
    default:
      return null;
  }
}

/**
 * Execute a query plan and return a DataFrame
 * Automatically applies optimizations (query optimization, column pruning, predicate pushdown)
 */
export async function executePlan(
  plan: PlanNode,
  enableOptimizations = true,
): Promise<Result<DataFrame, Error>> {
  applyCacheRetention();
  // Step 1: Optimize the query plan (reorder operations, combine filters, etc.)
  let optimizedPlan = plan;
  if (enableOptimizations) {
    const optimizationResult = optimizeQueryPlan(plan, 1000000);
    optimizedPlan = optimizationResult.plan;
  }

  // Step 2: Try specialized execution paths (predicate pushdown, column pruning)
  if (enableOptimizations) {
    // Find the scan node in the plan (might not be at the root after optimization)
    const scanNode = findScanNode(optimizedPlan);
    if (scanNode) {
      const pruningResult = await executeScanWithPruning(scanNode, optimizedPlan);
      if (pruningResult) {
        return pruningResult;
      }
    }
  }

  // Step 3: Execute the optimized plan using standard execution path
  switch (optimizedPlan.type) {
    case 'scan':
      return executeScan(optimizedPlan);

    case 'filter':
      return executeFilter(optimizedPlan);

    case 'select':
      return executeSelect(optimizedPlan);

    case 'groupby':
      return executeGroupBy(optimizedPlan);

    case 'distinct':
      return executeDistinct(optimizedPlan);

    case 'sort':
      return executeSort(optimizedPlan);

    case 'join':
      return executeJoin(optimizedPlan);

    default: {
      // TypeScript's exhaustive checking ensures this never happens
      const _exhaustiveCheck: never = optimizedPlan;
      return err(new Error(`Unknown plan node type: ${(_exhaustiveCheck as PlanNode).type}`));
    }
  }
}

/**
 * Execute filter operation
 */
async function executeFilter(plan: FilterPlan): Promise<Result<DataFrame, Error>> {
  // First execute the input plan
  const inputResult = await executePlan(plan.input);
  if (!inputResult.ok) {
    return inputResult;
  }

  // Apply filter
  try {
    // @ts-ignore - Dynamic execution
    const filtered = filter(inputResult.data, plan.column, plan.operator, plan.value);
    return ok(filtered);
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}

/**
 * Execute a select operation
 */
async function executeSelect(plan: SelectPlan): Promise<Result<DataFrame, Error>> {
  // First execute the input plan
  const inputResult = await executePlan(plan.input);
  if (!inputResult.ok) {
    return inputResult;
  }

  // Apply select
  try {
    // @ts-ignore - Dynamic execution
    const selected = select(inputResult.data, plan.columns);
    return ok(selected);
  } catch (error) {
    return err(error instanceof Error ? error : new Error(String(error)));
  }
}

/**
 * Execute a groupby operation
 */
async function executeGroupBy(plan: GroupByPlan): Promise<Result<DataFrame, Error>> {
  // First execute the input plan
  const inputResult = await executePlan(plan.input);
  if (!inputResult.ok) {
    return inputResult;
  }

  const df = inputResult.data;

  // Perform groupby operation (use correct property names from plan)
  return groupby(df, plan.groupKeys, plan.aggregations);
}

/**
 * Execute a distinct operation
 */
async function executeDistinct(plan: DistinctPlan): Promise<Result<DataFrame, Error>> {
  const inputResult = await executePlan(plan.input);
  if (!inputResult.ok) return inputResult;

  return unique(inputResult.data);
}

/**
 * Execute a sort operation
 */
async function executeSort(plan: SortPlan): Promise<Result<DataFrame, Error>> {
  const inputResult = await executePlan(plan.input);
  if (!inputResult.ok) return inputResult;

  return sortDataFrame(inputResult.data, {
    columns: plan.columns,
    directions: plan.directions,
  });
}

/**
 * Execute a join operation (fallback for non-streaming paths)
 */
async function executeJoin(plan: JoinPlan): Promise<Result<DataFrame, Error>> {
  const leftResult = await executePlan(plan.left);
  if (!leftResult.ok) return leftResult;
  const rightResult = await executePlan(plan.right);
  if (!rightResult.ok) return rightResult;

  const { merge } = await import('../../dataframe/joins/merge');
  return merge(leftResult.data, rightResult.data, {
    on: plan.on,
    leftOn: plan.leftOn,
    rightOn: plan.rightOn,
    how: plan.how,
    suffixes: plan.suffixes,
  });
}

/**
 * Optimize a query plan and return detailed optimization information
 *
 * @param plan - The query plan to optimize
 * @param estimatedRows - Estimated number of input rows (for cost calculations)
 * @returns Optimized plan with statistics and applied optimizations
 */
export function optimizePlan(plan: PlanNode, estimatedRows = 1000000): OptimizedPlan {
  return optimizeQueryPlan(plan, estimatedRows);
}

/**
 * Explain how a query plan will be executed with optimizations
 *
 * @param plan - The query plan to explain
 * @param estimatedRows - Estimated number of input rows
 * @returns Human-readable explanation of the optimized plan
 */
export function explainQueryPlan(plan: PlanNode, estimatedRows = 1000000): string {
  const optimized = optimizeQueryPlan(plan, estimatedRows);
  return explainPlan(optimized);
}
