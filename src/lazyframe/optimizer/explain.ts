import type { FilterPlan, JoinPlan, PlanNode, SelectPlan } from '../plan';
import type { OptimizedPlan } from './types';

/**
 * Explain the optimization decisions made
 */
export function explainPlan(optimized: OptimizedPlan): string {
  const lines: string[] = [];

  lines.push('=== Query Plan Optimization ===\n');

  if (optimized.optimizationsApplied.length === 0) {
    lines.push('No optimizations applied (plan is already optimal)\n');
  } else {
    lines.push('Optimizations Applied:');
    for (const opt of optimized.optimizationsApplied) {
      lines.push(`  • ${opt}`);
    }
    lines.push('');
  }

  lines.push('Statistics:');
  lines.push(
    `  Estimated output rows: ${Math.round(optimized.stats.estimatedRows).toLocaleString()}`,
  );
  lines.push(`  Selectivity: ${(optimized.stats.selectivity * 100).toFixed(1)}%`);
  lines.push(`  Pushdown eligible: ${optimized.stats.pushdownEligible ? 'Yes' : 'No'}`);
  lines.push(`  Estimated cost: ${optimized.stats.cost.toFixed(2)}`);
  lines.push('');

  lines.push('Optimized Plan:');
  lines.push(formatPlanTree(optimized.plan, 1));

  return lines.join('\n');
}

/**
 * Format plan tree for display
 */
function formatPlanTree(plan: PlanNode, depth: number): string {
  const indent = '  '.repeat(depth);
  const lines: string[] = [];

  switch (plan.type) {
    case 'scan':
      lines.push(`${indent}Scan: ${plan.path}`);
      break;

    case 'filter': {
      const filterPlan = plan as FilterPlan;
      lines.push(
        `${indent}Filter: ${filterPlan.column} ${filterPlan.operator} ${filterPlan.value}`,
      );
      lines.push(formatPlanTree(filterPlan.input, depth + 1));
      break;
    }

    case 'select': {
      const selectPlan = plan as SelectPlan;
      lines.push(`${indent}Select: [${selectPlan.columns.join(', ')}]`);
      lines.push(formatPlanTree(selectPlan.input, depth + 1));
      break;
    }

    case 'groupby':
      lines.push(`${indent}GroupBy: ${plan.groupKeys.join(', ')}`);
      lines.push(formatPlanTree(plan.input, depth + 1));
      break;

    case 'distinct': {
      const subset = plan.subset && plan.subset.length > 0 ? plan.subset.join(', ') : 'all columns';
      lines.push(`${indent}Distinct: ${subset}`);
      lines.push(formatPlanTree(plan.input, depth + 1));
      break;
    }

    case 'sort': {
      const cols = plan.columns.join(', ');
      const dirs =
        plan.directions && plan.directions.length > 0 ? plan.directions.join(', ') : 'asc';
      lines.push(`${indent}Sort: ${cols} (${dirs})`);
      lines.push(formatPlanTree(plan.input, depth + 1));
      break;
    }

    case 'join': {
      const joinPlan = plan as JoinPlan;
      const how = joinPlan.how ?? 'inner';
      const leftKeys = joinPlan.on ?? joinPlan.leftOn ?? [];
      const rightKeys = joinPlan.on ?? joinPlan.rightOn ?? [];
      lines.push(
        `${indent}Join (${how}): left[${leftKeys.join(', ')}] = right[${rightKeys.join(', ')}]`,
      );
      lines.push(formatPlanTree(joinPlan.left, depth + 1));
      lines.push(formatPlanTree(joinPlan.right, depth + 1));
      break;
    }
  }

  return lines.join('\n');
}
