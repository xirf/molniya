import type { DataFrame } from '../../dataframe/dataframe';
import { readCsv } from '../../io/csv-reader';
import { scanCsv } from '../../io/csv-scanner';
import type { Result } from '../../types/result';
import { analyzeRequiredColumns, shouldPruneColumns } from '../column-analyzer';
import { scanCsvWithPruning } from '../csv-pruning';
import type { PlanNode, ScanPlan } from '../plan';
import {
  type ScanPredicate,
  extractPushdownPredicates,
  scanCsvWithPredicates,
} from '../predicate-pushdown';
import { cleanupStaleCacheFiles } from './cache';
import { executePlanOnData, executePlanOnDataSkippingPushedFilters } from './ops';

/**
 * Execute a scan operation (load CSV)
 */
export async function executeScan(plan: ScanPlan): Promise<Result<DataFrame, Error>> {
  cleanupStaleCacheFiles(plan.path);

  const scanResult = await scanCsv(plan.path, {
    schema: plan.schema,
    delimiter: plan.delimiter ?? ',',
    hasHeader: plan.hasHeader ?? true,
    nullValues: plan.nullValues,
    chunkSize: plan.chunkSize,
  });

  if (scanResult.ok) {
    return scanResult;
  }

  // Fallback to eager loading
  return await readCsv(plan.path, {
    schema: plan.schema,
    delimiter: plan.delimiter ?? ',',
    hasHeader: plan.hasHeader ?? true,
    nullValues: plan.nullValues,
  });
}

/**
 * Execute scan with column pruning if beneficial
 * Returns null if pruning is not applicable, otherwise returns the result
 */
export async function executeScanWithPruning(
  scanPlan: ScanPlan,
  fullPlan: PlanNode,
): Promise<Result<DataFrame, Error> | null> {
  cleanupStaleCacheFiles(scanPlan.path);
  // First, try predicate pushdown (more impactful than column pruning)
  const predicates = extractPushdownPredicates(fullPlan);
  if (predicates.length > 0) {
    return executeScanWithPredicates(scanPlan, fullPlan, predicates);
  }

  // Analyze the full plan to see what columns are needed
  const requiredColumns = analyzeRequiredColumns(fullPlan);

  // If we need all columns or pruning won't help, skip
  if (requiredColumns.size === 0) {
    return null; // Can't determine columns, use normal path
  }

  // Read first line to get total column count
  try {
    const file = Bun.file(scanPlan.path);
    const fileContent = await file.text();
    const firstLine = fileContent.split('\n')[0];

    if (!firstLine) {
      return null; // Empty file
    }

    const headers = firstLine
      .split(scanPlan.delimiter ?? ',')
      .map((h) => h.trim().replace(/^"|"$/g, ''));

    // Check if pruning is beneficial
    if (!shouldPruneColumns(fullPlan, headers.length)) {
      return null; // Not worth pruning
    }

    const result = await scanCsvWithPruning(scanPlan.path, {
      requiredColumns,
      schema: new Map(Object.entries(scanPlan.schema)),
    });

    if (!result.ok) {
      return null; // Fall back to normal scan on error
    }

    return await executePlanOnData(fullPlan, result.data);
  } catch {
    return null; // Fall back to normal execution
  }
}

/**
 * Execute scan with predicate pushdown
 * Applies filters during CSV parsing to avoid loading filtered-out rows
 */
export async function executeScanWithPredicates(
  scanPlan: ScanPlan,
  fullPlan: PlanNode,
  predicates: ScanPredicate[],
): Promise<Result<DataFrame, Error> | null> {
  cleanupStaleCacheFiles(scanPlan.path);
  try {
    // Also get required columns for combined optimization
    const requiredColumns = analyzeRequiredColumns(fullPlan);

    const result = await scanCsvWithPredicates(scanPlan.path, {
      predicates,
      schema: new Map(Object.entries(scanPlan.schema)),
      requiredColumns: requiredColumns.size > 0 ? requiredColumns : undefined,
    });

    if (!result.ok) {
      return null; // Fall back to normal scan
    }

    return await executePlanOnDataSkippingPushedFilters(fullPlan, result.data, predicates);
  } catch {
    return null;
  }
}
