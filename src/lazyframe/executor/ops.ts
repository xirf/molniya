import type { DataFrame } from '../../dataframe/dataframe';
import { groupby } from '../../dataframe/groupby';
import { filter, select } from '../../dataframe/operations';
import { unique } from '../../dataframe/row-ops';
import { sortDataFrame } from '../../dataframe/sort';
import type { Result } from '../../types/result';
import { err, ok } from '../../types/result';
import type { PlanNode } from '../plan';
import type { ScanPredicate } from '../predicate-pushdown';

/**
 * Execute plan on data, skipping filters that were already applied during pushdown
 */
export async function executePlanOnDataSkippingPushedFilters(
  plan: PlanNode,
  data: DataFrame,
  pushedPredicates: ScanPredicate[],
): Promise<Result<DataFrame, Error>> {
  switch (plan.type) {
    case 'scan':
      return ok(data);

    case 'filter': {
      // First execute the input plan
      const inputResult = await executePlanOnDataSkippingPushedFilters(
        plan.input,
        data,
        pushedPredicates,
      );
      if (!inputResult.ok) return inputResult;

      // Check if this filter was pushed down
      const wasPushed = pushedPredicates.some(
        (p) =>
          p.columnName === plan.column && p.operator === plan.operator && p.value === plan.value,
      );

      if (wasPushed) {
        // Skip this filter, already applied
        return inputResult;
      }

      // Apply filter normally
      try {
        // @ts-ignore - Dynamic execution
        const filtered = filter(inputResult.data, plan.column, plan.operator, plan.value);
        return ok(filtered);
      } catch (error) {
        return err(error instanceof Error ? error : new Error(String(error)));
      }
    }

    case 'select': {
      // First execute the input plan
      const inputResult = await executePlanOnDataSkippingPushedFilters(
        plan.input,
        data,
        pushedPredicates,
      );
      if (!inputResult.ok) return inputResult;

      // Apply select to the result
      try {
        // @ts-ignore - Dynamic execution
        const selected = select(inputResult.data, plan.columns);
        return ok(selected);
      } catch (error) {
        return err(error instanceof Error ? error : new Error(String(error)));
      }
    }

    case 'groupby': {
      // First execute the input plan
      const inputResult = await executePlanOnDataSkippingPushedFilters(
        plan.input,
        data,
        pushedPredicates,
      );
      if (!inputResult.ok) return inputResult;

      // Apply groupby to the result
      return groupby(inputResult.data, plan.groupKeys, plan.aggregations);
    }

    case 'distinct': {
      const inputResult = await executePlanOnDataSkippingPushedFilters(
        plan.input,
        data,
        pushedPredicates,
      );
      if (!inputResult.ok) return inputResult;
      return unique(inputResult.data);
    }

    case 'sort': {
      const inputResult = await executePlanOnDataSkippingPushedFilters(
        plan.input,
        data,
        pushedPredicates,
      );
      if (!inputResult.ok) return inputResult;
      return sortDataFrame(inputResult.data, {
        columns: plan.columns,
        directions: plan.directions,
      });
    }

    case 'join':
      return err(new Error('Join plans cannot be executed with pruned scan path'));

    default: {
      // TypeScript's exhaustive checking ensures this never happens
      {
        const _exhaustiveCheck: never = plan;
        return err(new Error(`Unknown plan node type: ${(_exhaustiveCheck as PlanNode).type}`));
      }
    }
  }
}

/**
 * Execute plan operations after the scan (used for pruned execution)
 */
export async function executePlanOnData(
  plan: PlanNode,
  data: DataFrame,
): Promise<Result<DataFrame, Error>> {
  switch (plan.type) {
    case 'scan':
      // Already have data from pruned scan
      return ok(data);

    case 'filter': {
      // First execute the input plan
      const inputResult = await executePlanOnData(plan.input, data);
      if (!inputResult.ok) return inputResult;

      // Apply filter to the result
      try {
        // @ts-ignore - Dynamic execution
        const filtered = filter(inputResult.data, plan.column, plan.operator, plan.value);
        return ok(filtered);
      } catch (error) {
        return err(error instanceof Error ? error : new Error(String(error)));
      }
    }

    case 'select': {
      // First execute the input plan
      const inputResult = await executePlanOnData(plan.input, data);
      if (!inputResult.ok) return inputResult;

      // Apply select to the result
      try {
        // @ts-ignore - Dynamic execution
        const selected = select(inputResult.data, plan.columns);
        return ok(selected);
      } catch (error) {
        return err(error instanceof Error ? error : new Error(String(error)));
      }
    }

    case 'groupby': {
      // First execute the input plan
      const inputResult = await executePlanOnData(plan.input, data);
      if (!inputResult.ok) return inputResult;

      // Apply groupby to the result
      return groupby(inputResult.data, plan.groupKeys, plan.aggregations);
    }

    case 'distinct': {
      const inputResult = await executePlanOnData(plan.input, data);
      if (!inputResult.ok) return inputResult;
      return unique(inputResult.data);
    }

    case 'sort': {
      const inputResult = await executePlanOnData(plan.input, data);
      if (!inputResult.ok) return inputResult;
      return sortDataFrame(inputResult.data, {
        columns: plan.columns,
        directions: plan.directions,
      });
    }

    case 'join':
      return err(new Error('Join plans cannot be executed with pruned scan path'));

    default: {
      // TypeScript's exhaustive checking ensures this never happens
      {
        const _exhaustiveCheck: never = plan;
        return err(new Error(`Unknown plan node type: ${(_exhaustiveCheck as PlanNode).type}`));
      }
    }
  }
}
