import type { FilterPlan, PlanNode, ScanPlan, SelectPlan } from '../plan';
import { canPushdownFilter, estimateFilterSelectivity, sortFiltersBySelectivity } from './filters';
import type { OptimizationStats } from './types';

/**
 * Reorder operations for optimal performance
 */
export function reorderOperations(plan: PlanNode, optimizations: string[]): PlanNode {
  if (plan.type === 'join') {
    return {
      ...plan,
      left: reorderOperations(plan.left, optimizations),
      right: reorderOperations(plan.right, optimizations),
    };
  }

  if (plan.type === 'distinct') {
    return {
      ...plan,
      input: reorderOperations(plan.input, optimizations),
    };
  }

  if (plan.type === 'sort') {
    return {
      ...plan,
      input: reorderOperations(plan.input, optimizations),
    };
  }

  const operations: PlanNode[] = [];
  let current: PlanNode = plan;
  let scanNode: ScanPlan | null = null;

  while (current.type !== 'scan') {
    operations.push(current);
    current = (current as FilterPlan | SelectPlan).input;
  }
  scanNode = current as ScanPlan;

  if (operations.length === 0) {
    return plan;
  }

  const filters: FilterPlan[] = [];
  const selects: SelectPlan[] = [];
  const others: PlanNode[] = [];

  for (const op of operations) {
    if (op.type === 'filter') {
      filters.push(op as FilterPlan);
    } else if (op.type === 'select') {
      selects.push(op as SelectPlan);
    } else {
      others.push(op);
    }
  }

  const sortedFilters = sortFiltersBySelectivity(filters);

  let result: PlanNode = scanNode;

  for (const filter of sortedFilters) {
    result = {
      ...filter,
      input: result,
    };
  }

  for (const selectOp of selects) {
    result = {
      ...selectOp,
      input: result,
    };
  }

  for (const op of others) {
    result = {
      ...op,
      input: result,
    } as PlanNode;
  }

  if (sortedFilters.length > 1 && JSON.stringify(sortedFilters) !== JSON.stringify(filters)) {
    optimizations.push('Reordered filters by selectivity');
  }

  if (selects.length > 0 && operations[0] && operations[0].type !== 'select') {
    optimizations.push('Moved select operations after filters');
  }

  return result;
}

/**
 * Detect and mark operations eligible for pushdown optimization
 */
export function detectPushdownOpportunities(plan: PlanNode, optimizations: string[]): PlanNode {
  if (plan.type === 'scan') {
    return plan;
  }

  if (plan.type === 'join') {
    return {
      ...plan,
      left: detectPushdownOpportunities(plan.left, optimizations),
      right: detectPushdownOpportunities(plan.right, optimizations),
    };
  }

  if (plan.type === 'distinct') {
    return {
      ...plan,
      input: detectPushdownOpportunities(plan.input, optimizations),
    };
  }

  if (plan.type === 'sort') {
    return {
      ...plan,
      input: detectPushdownOpportunities(plan.input, optimizations),
    };
  }

  if (plan.type === 'filter') {
    const filterPlan = plan as FilterPlan;
    const isPushdownEligible = canPushdownFilter(filterPlan);

    if (isPushdownEligible) {
      let current: PlanNode = filterPlan.input;
      while (current.type !== 'scan') {
        if (current.type === 'filter' || current.type === 'select') {
          current = (current as FilterPlan | SelectPlan).input;
        } else {
          break;
        }
      }
      if (current.type === 'scan') {
        optimizations.push(`Detected pushdown opportunity for filter on '${filterPlan.column}'`);
      }
    }

    return {
      ...filterPlan,
      input: detectPushdownOpportunities(filterPlan.input, optimizations),
    };
  }

  if (plan.type === 'select') {
    const selectPlan = plan as SelectPlan;

    let current: PlanNode = selectPlan.input;
    while (current.type !== 'scan') {
      if (current.type === 'filter' || current.type === 'select') {
        current = (current as FilterPlan | SelectPlan).input;
      } else {
        break;
      }
    }

    if (current.type === 'scan') {
      optimizations.push(
        `Detected column pruning opportunity (selecting ${selectPlan.columns.length} columns)`,
      );
    }

    return {
      ...selectPlan,
      input: detectPushdownOpportunities(selectPlan.input, optimizations),
    };
  }

  if (plan.type === 'groupby') {
    return {
      ...plan,
      input: detectPushdownOpportunities(plan.input, optimizations),
    };
  }

  return plan;
}

/**
 * Calculate statistics for the optimized plan
 */
export function calculateStats(plan: PlanNode, inputRows: number): OptimizationStats {
  let estimatedRows = inputRows;
  let selectivity = 1.0;
  let pushdownEligible = false;
  let cost = 0;

  const traverse = (node: PlanNode, rows: number): number => {
    switch (node.type) {
      case 'scan':
        cost += rows * 1;
        return rows;

      case 'filter': {
        const filterPlan = node as FilterPlan;
        const filterSelectivity = estimateFilterSelectivity(filterPlan);
        const outputRows = rows * filterSelectivity;

        if (canPushdownFilter(filterPlan) && filterPlan.input.type === 'scan') {
          pushdownEligible = true;
          cost += rows * 0.5;
        } else {
          cost += rows * 1.5;
        }

        return traverse(filterPlan.input, outputRows);
      }

      case 'select': {
        const selectPlan = node as SelectPlan;
        const columnRatio = selectPlan.columns.length / 10;
        cost += rows * columnRatio * 0.3;
        return traverse(selectPlan.input, rows);
      }

      case 'groupby':
        cost += rows * 2;
        return traverse(node.input, rows * 0.1);

      case 'distinct':
        cost += rows * 1.8;
        return traverse(node.input, rows * 0.7);

      case 'sort':
        cost += rows * 2.2;
        return traverse(node.input, rows);

      case 'join': {
        const leftRows = traverse(node.left, rows);
        const rightRows = traverse(node.right, rows);
        const outputRows = Math.min(leftRows, rightRows);
        cost += (leftRows + rightRows) * 1.2;
        return outputRows;
      }
    }

    return rows;
  };

  estimatedRows = traverse(plan, inputRows);
  selectivity = estimatedRows / inputRows;

  return {
    estimatedRows,
    selectivity,
    pushdownEligible,
    cost,
  };
}
