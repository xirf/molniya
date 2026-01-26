/**
 * Analyzes query plans to determine which columns are actually needed
 * Enables column pruning during CSV scanning
 */

import type { PlanNode } from './plan';
import { QueryPlan } from './plan';

/**
 * Analyzes a query plan to determine which columns are required
 * Returns the minimal set of columns needed to execute the plan
 */
export function analyzeRequiredColumns(plan: PlanNode): Set<string> {
  const required = new Set<string>();

  // Traverse the plan tree bottom-up
  collectRequiredColumns(plan, required);

  return required;
}

/**
 * Recursively collect required columns from a query plan
 */
function collectRequiredColumns(plan: PlanNode, required: Set<string>): void {
  switch (plan.type) {
    case 'scan':
      // Scan doesn't add requirements - it's the data source
      // The parent operations determine what columns we need
      break;

    case 'filter':
      // Filter needs the column being filtered
      required.add(plan.column);
      // Recursively check the input
      collectRequiredColumns(plan.input, required);
      break;

    case 'select':
      // Select explicitly specifies which columns to keep
      for (const col of plan.columns) {
        required.add(col);
      }
      // IMPORTANT: We must recurse to collect columns needed by operations
      // that come before this select (filter, etc.)
      collectRequiredColumns(plan.input, required);
      break;

    case 'groupby':
      // GroupBy needs the grouping column(s) and aggregation columns
      for (const col of plan.groupKeys) {
        required.add(col);
      }
      // Add aggregation columns
      for (const agg of plan.aggregations) {
        required.add(agg.col);
      }
      // Recursively check the input
      collectRequiredColumns(plan.input, required);
      break;

    case 'distinct': {
      if (plan.subset && plan.subset.length > 0) {
        for (const col of plan.subset) required.add(col);
      } else {
        for (const col of QueryPlan.getColumnOrder(plan.input)) {
          required.add(col);
        }
      }
      collectRequiredColumns(plan.input, required);
      break;
    }

    case 'sort':
      for (const col of plan.columns) {
        required.add(col);
      }
      collectRequiredColumns(plan.input, required);
      break;

    case 'join': {
      const leftKeys = plan.on ?? plan.leftOn ?? [];
      const rightKeys = plan.on ?? plan.rightOn ?? [];
      for (const key of leftKeys) required.add(key);
      for (const key of rightKeys) required.add(key);
      collectRequiredColumns(plan.left, required);
      collectRequiredColumns(plan.right, required);
      break;
    }
  }
}

/**
 * Checks if column pruning would be beneficial for this plan
 * Returns true if:
 * - Plan contains a Select operation (explicit column filtering)
 * - Required columns < total available columns
 */
export function shouldPruneColumns(plan: PlanNode, totalColumns: number): boolean {
  // Check if there's a Select in the plan
  if (!hasSelectOperation(plan)) {
    // No select means we need all columns - pruning won't help
    return false;
  }

  // Calculate required columns
  const required = analyzeRequiredColumns(plan);

  // Only prune if we can skip at least 30% of columns
  const pruningRatio = required.size / totalColumns;
  return pruningRatio < 0.7;
}

/**
 * Check if the plan contains a Select operation
 */
function hasSelectOperation(plan: PlanNode): boolean {
  if (plan.type === 'select') {
    return true;
  }

  if (plan.type === 'join') {
    return hasSelectOperation(plan.left) || hasSelectOperation(plan.right);
  }

  // Check input plans recursively (all plans except ScanPlan have input)
  if (plan.type !== 'scan') {
    return hasSelectOperation(plan.input);
  }

  return false;
}

/**
 * Gets the column indices for required columns from a header row
 * This is used by the CSV scanner to know which column positions to parse
 */
export function getRequiredColumnIndices(
  headers: string[],
  requiredColumns: Set<string>,
): Map<string, number> {
  const indices = new Map<string, number>();

  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    if (header && requiredColumns.has(header)) {
      indices.set(header, i);
    }
  }

  return indices;
}

/**
 * Analyzes the plan to determine the earliest point where column pruning can occur
 * Returns the columns needed at each stage
 */
export interface ColumnRequirements {
  // Columns needed from the scan (minimal set)
  scanColumns: Set<string>;
  // Columns needed after filters
  afterFilterColumns: Set<string>;
  // Final output columns
  outputColumns: Set<string>;
}

export function analyzeColumnRequirements(plan: PlanNode): ColumnRequirements {
  const scanColumns = new Set<string>();
  const afterFilterColumns = new Set<string>();
  const outputColumns = new Set<string>();

  analyzeStage(plan, scanColumns, afterFilterColumns, outputColumns);

  return {
    scanColumns,
    afterFilterColumns,
    outputColumns,
  };
}

function analyzeStage(
  plan: PlanNode,
  scanColumns: Set<string>,
  afterFilterColumns: Set<string>,
  outputColumns: Set<string>,
): void {
  switch (plan.type) {
    case 'scan':
      // Base case - all accumulated columns are scan columns
      break;

    case 'filter':
      // Filter needs its column at scan time
      scanColumns.add(plan.column);
      afterFilterColumns.add(plan.column);
      analyzeStage(plan.input, scanColumns, afterFilterColumns, outputColumns);
      break;

    case 'select':
      // Select defines output columns
      for (const col of plan.columns) {
        scanColumns.add(col);
        afterFilterColumns.add(col);
        outputColumns.add(col);
      }
      // Don't recurse - select cuts off upstream
      break;

    case 'groupby':
      // GroupBy needs its columns
      for (const col of plan.groupKeys) {
        scanColumns.add(col);
        afterFilterColumns.add(col);
        outputColumns.add(col);
      }
      for (const agg of plan.aggregations) {
        scanColumns.add(agg.col);
        afterFilterColumns.add(agg.col);
      }
      analyzeStage(plan.input, scanColumns, afterFilterColumns, outputColumns);
      break;

    case 'distinct': {
      const columns =
        plan.subset && plan.subset.length > 0 ? plan.subset : QueryPlan.getColumnOrder(plan.input);
      for (const col of columns) {
        scanColumns.add(col);
        afterFilterColumns.add(col);
        outputColumns.add(col);
      }
      analyzeStage(plan.input, scanColumns, afterFilterColumns, outputColumns);
      break;
    }

    case 'sort':
      for (const col of plan.columns) {
        scanColumns.add(col);
        afterFilterColumns.add(col);
        outputColumns.add(col);
      }
      analyzeStage(plan.input, scanColumns, afterFilterColumns, outputColumns);
      break;

    case 'join': {
      const leftKeys = plan.on ?? plan.leftOn ?? [];
      const rightKeys = plan.on ?? plan.rightOn ?? [];
      for (const key of leftKeys) {
        scanColumns.add(key);
        afterFilterColumns.add(key);
        outputColumns.add(key);
      }
      for (const key of rightKeys) {
        scanColumns.add(key);
        afterFilterColumns.add(key);
        outputColumns.add(key);
      }
      analyzeStage(plan.left, scanColumns, afterFilterColumns, outputColumns);
      analyzeStage(plan.right, scanColumns, afterFilterColumns, outputColumns);
      break;
    }
  }
}
