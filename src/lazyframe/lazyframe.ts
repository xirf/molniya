import type { DataFrame } from '../dataframe/dataframe';
import type { AggSpec } from '../dataframe/groupby';
import type { JoinType } from '../dataframe/joins/types';
import type { DType } from '../types/dtypes';
import type { FilterOperator } from '../types/operators';
import type { Result } from '../types/result';
import type { SortDirection } from '../utils/sort';
import { type PlanNode, QueryPlan } from './plan';
import type { InferSchemaType, Select } from './types';

/**
 * LazyFrame - Builds a query plan without executing it.
 * Operations are chained and only executed when collect() is called.
 * T represents the row shape (key -> value type)
 */
export class LazyFrame<T = unknown> {
  private readonly plan: PlanNode;

  /**
   * Private constructor - use scanCsv() to create a LazyFrame
   */
  private constructor(plan: PlanNode) {
    this.plan = plan;
  }

  /**
   * Create a LazyFrame from a CSV file path (scan operation)
   */
  static scanCsv<S extends Record<string, DType>>(
    path: string,
    schema: S,
    options?: {
      chunkSize?: number;
      delimiter?: string;
      hasHeader?: boolean;
      nullValues?: string[];
    },
  ): LazyFrame<InferSchemaType<S>> {
    const columnOrder = Object.keys(schema);
    const plan = QueryPlan.scan(path, schema, columnOrder, options);
    return new LazyFrame<InferSchemaType<S>>(plan);
  }

  /**
   * Add a filter operation to the plan
   */
  filter<K extends keyof T>(
    column: K,
    operator: FilterOperator,
    value: number | bigint | string | boolean | Array<number | bigint | string | boolean>,
  ): LazyFrame<T> {
    const newPlan = QueryPlan.filter(this.plan, String(column), operator, value);
    return new LazyFrame<T>(newPlan);
  }

  /**
   * Add a select operation to the plan
   */
  select<K extends keyof T>(columns: K[]): LazyFrame<Select<T, K>> {
    const newPlan = QueryPlan.select(this.plan, columns as string[]);
    return new LazyFrame<Select<T, K>>(newPlan);
  }

  /**
   * Add a groupby operation to the plan
   * Note: groupKeys should ideally be K extends keyof T, but avoiding complexity for now
   */
  groupby(groupKeys: string[], aggregations: AggSpec[]): LazyFrame<T> {
    const newPlan = QueryPlan.groupby(this.plan, groupKeys, aggregations);
    return new LazyFrame<T>(newPlan);
  }

  /**
   * Drop duplicate rows (distinct)
   */
  unique(subset?: string[]): LazyFrame<T> {
    const newPlan = QueryPlan.distinct(this.plan, subset);
    return new LazyFrame<T>(newPlan);
  }

  /**
   * Alias for unique()
   */
  distinct(subset?: string[]): LazyFrame<T> {
    return this.unique(subset);
  }

  /**
   * Sort rows by one or more columns
   */
  sort(
    columns: string[] | string,
    directions?: SortDirection[] | SortDirection,
    options?: { runBytes?: number; tempDir?: string },
  ): LazyFrame<T> {
    const cols = Array.isArray(columns) ? columns : [columns];
    const dirs = Array.isArray(directions) ? directions : directions ? [directions] : undefined;
    const newPlan = QueryPlan.sort(this.plan, cols, dirs, options);
    return new LazyFrame<T>(newPlan);
  }

  /**
   * Merge (join) with another LazyFrame
   */
  merge(
    right: LazyFrame<T>,
    options: {
      on?: string | string[];
      leftOn?: string | string[];
      rightOn?: string | string[];
      how?: JoinType;
      suffixes?: [string, string];
    },
  ): LazyFrame<T> {
    const newPlan = QueryPlan.join(this.plan, right.getPlan(), options);
    return new LazyFrame<T>(newPlan);
  }

  /**
   * Get the query plan (for debugging/inspection)
   */
  getPlan(): PlanNode {
    return this.plan;
  }

  /**
   * Get the expected output schema
   */
  getSchema(): Record<string, DType> {
    return QueryPlan.getOutputSchema(this.plan);
  }

  /**
   * Get the expected column order
   */
  getColumnOrder(): string[] {
    return QueryPlan.getColumnOrder(this.plan);
  }

  /**
   * Pretty-print the query plan
   */
  explain(): string {
    return QueryPlan.explain(this.plan);
  }

  /**
   * Execute the query plan and return a DataFrame
   * This is a placeholder - actual execution will be implemented in executor.ts
   */
  async collect(): Promise<Result<DataFrame<T>, Error>> {
    // Import executor dynamically to avoid circular dependency
    const { executePlan } = await import('./executor');
    // Cast result to generic DataFrame<T> since executor returns DataFrame<unknown>
    return executePlan(this.plan) as Promise<Result<DataFrame<T>, Error>>;
  }
}
