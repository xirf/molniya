import type { PlanNode } from '../plan';

/**
 * Optimization statistics for cost-based decisions
 */
export interface OptimizationStats {
  /** Estimated number of rows after this operation */
  estimatedRows: number;
  /** Estimated selectivity (0.0 - 1.0) */
  selectivity: number;
  /** Can this operation be pushed down to scan? */
  pushdownEligible: boolean;
  /** Estimated cost (lower is better) */
  cost: number;
}

/**
 * Optimized query plan with metadata
 */
export interface OptimizedPlan {
  plan: PlanNode;
  stats: OptimizationStats;
  optimizationsApplied: string[];
}
