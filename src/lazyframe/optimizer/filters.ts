import type { FilterOperator } from '../../types/operators';
import type { FilterPlan, PlanNode, SelectPlan } from '../plan';

/**
 * Sort filters by estimated selectivity (most selective first)
 */
export function sortFiltersBySelectivity(filters: FilterPlan[]): FilterPlan[] {
  return filters.slice().sort((a, b) => {
    const selectivityA = estimateFilterSelectivity(a);
    const selectivityB = estimateFilterSelectivity(b);
    return selectivityA - selectivityB; // Lower selectivity (more selective) first
  });
}

/**
 * Estimate selectivity of a filter operation (0.0 = filters all, 1.0 = keeps all)
 */
export function estimateFilterSelectivity(filter: FilterPlan): number {
  const { operator, value } = filter;

  if (operator === '==') {
    if (typeof value === 'string') {
      return 0.05;
    }
    return 0.1;
  }

  if (operator === '!=') {
    return 0.9;
  }

  if (operator === '>' || operator === '<') {
    return 0.5;
  }

  if (operator === '>=' || operator === '<=') {
    return 0.5;
  }

  return 0.5;
}

/**
 * Check if a filter can be pushed down to the scan operation
 */
export function canPushdownFilter(filter: FilterPlan): boolean {
  const pushdownOperators: FilterOperator[] = ['==', '!=', '>', '<', '>=', '<='];
  if (!pushdownOperators.includes(filter.operator)) {
    return false;
  }

  if (Array.isArray(filter.value)) {
    return false;
  }

  return true;
}

/**
 * Combine consecutive filter operations into multi-predicate filters
 */
export function combineFilters(plan: PlanNode, optimizations: string[]): PlanNode {
  if (plan.type !== 'filter') {
    switch (plan.type) {
      case 'select':
        return {
          ...plan,
          input: combineFilters(plan.input, optimizations),
        };
      case 'groupby':
        return {
          ...plan,
          input: combineFilters(plan.input, optimizations),
        };
      case 'join':
        return {
          ...plan,
          left: combineFilters(plan.left, optimizations),
          right: combineFilters(plan.right, optimizations),
        };
      case 'distinct':
        return {
          ...plan,
          input: combineFilters(plan.input, optimizations),
        };
      case 'sort':
        return {
          ...plan,
          input: combineFilters(plan.input, optimizations),
        };
      default:
        return plan;
    }
  }

  const filters: FilterPlan[] = [];
  let current: PlanNode = plan;

  while (current.type === 'filter') {
    filters.push(current);
    current = current.input;
  }

  if (filters.length > 1) {
    optimizations.push(`Combined ${filters.length} consecutive filters`);

    const sorted = sortFiltersBySelectivity(filters);
    filters.length = 0;
    filters.push(...sorted);
  }

  const baseInput = combineFilters(current, optimizations);

  let result: PlanNode = baseInput;
  for (let i = filters.length - 1; i >= 0; i--) {
    const filter = filters[i];
    if (!filter) continue;
    result = {
      type: 'filter' as const,
      input: result,
      column: filter.column,
      operator: filter.operator,
      value: filter.value,
      id: filter.id,
    };
  }

  return result;
}

/**
 * Deduplicate identical consecutive filters
 */
export function deduplicateFilters(plan: PlanNode): PlanNode {
  if (plan.type !== 'filter') {
    switch (plan.type) {
      case 'select':
        return {
          ...plan,
          input: deduplicateFilters(plan.input),
        } as SelectPlan;
      case 'groupby':
        return {
          ...plan,
          input: deduplicateFilters(plan.input),
        };
      case 'join':
        return {
          ...plan,
          left: deduplicateFilters(plan.left),
          right: deduplicateFilters(plan.right),
        };
      case 'distinct':
        return {
          ...plan,
          input: deduplicateFilters(plan.input),
        };
      case 'sort':
        return {
          ...plan,
          input: deduplicateFilters(plan.input),
        };
      default:
        return plan;
    }
  }

  const filterPlan = plan as FilterPlan;
  const processedInput = deduplicateFilters(filterPlan.input);

  if (
    processedInput.type === 'filter' &&
    processedInput.column === filterPlan.column &&
    processedInput.operator === filterPlan.operator &&
    processedInput.value === filterPlan.value
  ) {
    return processedInput;
  }

  return {
    ...filterPlan,
    input: processedInput,
  };
}
