import type { PlanNode } from '../plan';
import { combineFilters } from './filters';
import { calculateStats, detectPushdownOpportunities, reorderOperations } from './operations';
import type { OptimizedPlan } from './types';

/**
 * Main entry point: optimize a query plan
 */
export function optimizePlan(plan: PlanNode, estimatedInputRows = 1000000): OptimizedPlan {
  const optimizationsApplied: string[] = [];

  let optimized = combineFilters(plan, optimizationsApplied);
  optimized = reorderOperations(optimized, optimizationsApplied);
  optimized = detectPushdownOpportunities(optimized, optimizationsApplied);

  const stats = calculateStats(optimized, estimatedInputRows);

  return {
    plan: optimized,
    stats,
    optimizationsApplied,
  };
}
