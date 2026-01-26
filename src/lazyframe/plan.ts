import type { Column } from '../core/column';
import type { AggFunc, AggSpec } from '../dataframe/groupby';
import type { JoinType } from '../dataframe/joins/types';
import { DType } from '../types/dtypes';
import type { FilterOperator } from '../types/operators';
import type { SortDirection } from '../utils/sort';

/**
 * Query plan node types for lazy evaluation
 */
export type PlanNode =
  | ScanPlan
  | FilterPlan
  | SelectPlan
  | GroupByPlan
  | JoinPlan
  | DistinctPlan
  | SortPlan;

/**
 * Base plan node
 */
interface BasePlan {
  id: number; // Unique node identifier for caching
}

/**
 * CSV scan operation (source node)
 */
export interface ScanPlan extends BasePlan {
  type: 'scan';
  path: string;
  schema: Record<string, DType>;
  columnOrder: string[];
  chunkSize?: number;
  delimiter?: string;
  hasHeader?: boolean;
  nullValues?: string[];
}

/**
 * Filter operation
 */
export interface FilterPlan extends BasePlan {
  type: 'filter';
  input: PlanNode;
  column: string;
  operator: FilterOperator;
  value: number | bigint | string | boolean | Array<number | bigint | string | boolean>;
}

/**
 * Select (column projection) operation
 */
export interface SelectPlan extends BasePlan {
  type: 'select';
  input: PlanNode;
  columns: string[];
}

/**
 * GroupBy aggregation operation
 */
export interface GroupByPlan extends BasePlan {
  type: 'groupby';
  input: PlanNode;
  groupKeys: string[];
  aggregations: AggSpec[];
}

/**
 * Join operation
 */
export interface JoinPlan extends BasePlan {
  type: 'join';
  left: PlanNode;
  right: PlanNode;
  on?: string[];
  leftOn?: string[];
  rightOn?: string[];
  how?: JoinType;
  suffixes?: [string, string];
}

/**
 * Distinct/unique operation
 */
export interface DistinctPlan extends BasePlan {
  type: 'distinct';
  input: PlanNode;
  subset?: string[];
}

/**
 * Sort operation
 */
export interface SortPlan extends BasePlan {
  type: 'sort';
  input: PlanNode;
  columns: string[];
  directions?: SortDirection[];
  runBytes?: number;
  tempDir?: string;
}

/**
 * Plan builder for constructing query plans
 */
export class QueryPlan {
  private static nextId = 0;

  /**
   * Create a scan plan node
   */
  static scan(
    path: string,
    schema: Record<string, DType>,
    columnOrder: string[],
    options?: {
      chunkSize?: number;
      delimiter?: string;
      hasHeader?: boolean;
      nullValues?: string[];
    },
  ): ScanPlan {
    return {
      type: 'scan',
      id: QueryPlan.nextId++,
      path,
      schema,
      columnOrder,
      chunkSize: options?.chunkSize,
      delimiter: options?.delimiter,
      hasHeader: options?.hasHeader,
      nullValues: options?.nullValues,
    };
  }

  /**
   * Create a filter plan node
   */
  static filter(
    input: PlanNode,
    column: string,
    operator: FilterOperator,
    value: number | bigint | string | boolean | Array<number | bigint | string | boolean>,
  ): FilterPlan {
    return {
      type: 'filter',
      id: QueryPlan.nextId++,
      input,
      column,
      operator,
      value,
    };
  }

  /**
   * Create a select plan node
   */
  static select(input: PlanNode, columns: string[]): SelectPlan {
    return {
      type: 'select',
      id: QueryPlan.nextId++,
      input,
      columns,
    };
  }

  /**
   * Create a groupby plan node
   */
  static groupby(input: PlanNode, groupKeys: string[], aggregations: AggSpec[]): GroupByPlan {
    return {
      type: 'groupby',
      id: QueryPlan.nextId++,
      input,
      groupKeys,
      aggregations,
    };
  }

  /**
   * Create a join plan node
   */
  static join(
    left: PlanNode,
    right: PlanNode,
    options: {
      on?: string | string[];
      leftOn?: string | string[];
      rightOn?: string | string[];
      how?: JoinType;
      suffixes?: [string, string];
    },
  ): JoinPlan {
    return {
      type: 'join',
      id: QueryPlan.nextId++,
      left,
      right,
      on: options.on ? (Array.isArray(options.on) ? options.on : [options.on]) : undefined,
      leftOn: options.leftOn
        ? Array.isArray(options.leftOn)
          ? options.leftOn
          : [options.leftOn]
        : undefined,
      rightOn: options.rightOn
        ? Array.isArray(options.rightOn)
          ? options.rightOn
          : [options.rightOn]
        : undefined,
      how: options.how,
      suffixes: options.suffixes,
    };
  }

  /**
   * Create a distinct plan node
   */
  static distinct(input: PlanNode, subset?: string[]): DistinctPlan {
    return {
      type: 'distinct',
      id: QueryPlan.nextId++,
      input,
      subset,
    };
  }

  /**
   * Create a sort plan node
   */
  static sort(
    input: PlanNode,
    columns: string[],
    directions?: SortDirection[],
    options?: { runBytes?: number; tempDir?: string },
  ): SortPlan {
    return {
      type: 'sort',
      id: QueryPlan.nextId++,
      input,
      columns,
      directions,
      runBytes: options?.runBytes,
      tempDir: options?.tempDir,
    };
  }

  private static resolveJoinKeys(plan: JoinPlan): { leftKeys: string[]; rightKeys: string[] } {
    if (plan.on && plan.on.length > 0) {
      return { leftKeys: plan.on, rightKeys: plan.on };
    }
    const leftKeys = plan.leftOn ?? [];
    const rightKeys = plan.rightOn ?? [];
    return { leftKeys, rightKeys };
  }

  private static buildJoinOutputSchema(plan: JoinPlan): Record<string, DType> {
    const leftSchema = QueryPlan.getOutputSchema(plan.left);
    const rightSchema = QueryPlan.getOutputSchema(plan.right);
    const leftOrder = QueryPlan.getColumnOrder(plan.left);
    const rightOrder = QueryPlan.getColumnOrder(plan.right);
    const suffixes = plan.suffixes ?? ['_x', '_y'];
    const { leftKeys, rightKeys } = QueryPlan.resolveJoinKeys(plan);
    const rightKeySet = new Set(rightKeys);
    const leftNameSet = new Set(leftOrder);
    const overlapping = rightOrder.filter(
      (name) => !rightKeySet.has(name) && leftNameSet.has(name),
    );

    const output: Record<string, DType> = {};
    for (const name of leftOrder) {
      const dtype = leftSchema[name];
      if (!dtype) continue;
      const finalName = overlapping.includes(name) ? `${name}${suffixes[0]}` : name;
      output[finalName] = dtype;
    }

    for (const name of rightOrder) {
      if (rightKeySet.has(name)) continue;
      const dtype = rightSchema[name];
      if (!dtype) continue;
      const finalName = overlapping.includes(name) ? `${name}${suffixes[1]}` : name;
      output[finalName] = dtype;
    }

    return output;
  }

  /**
   * Get the output schema for a plan node
   */
  static getOutputSchema(node: PlanNode): Record<string, DType> {
    switch (node.type) {
      case 'scan':
        return node.schema;

      case 'filter':
        return QueryPlan.getOutputSchema(node.input);

      case 'select': {
        const inputSchema = QueryPlan.getOutputSchema(node.input);
        const outputSchema: Record<string, DType> = {};
        for (const col of node.columns) {
          if (inputSchema[col]) {
            outputSchema[col] = inputSchema[col];
          }
        }
        return outputSchema;
      }

      case 'groupby': {
        const inputSchema = QueryPlan.getOutputSchema(node.input);
        const outputSchema: Record<string, DType> = {};

        // Add group key columns
        for (const key of node.groupKeys) {
          if (inputSchema[key]) {
            outputSchema[key] = inputSchema[key];
          }
        }

        // Add aggregation columns with their output dtypes
        for (const agg of node.aggregations) {
          const srcDtype = inputSchema[agg.col];
          if (!srcDtype) continue;

          // Determine output dtype based on aggregation function
          let outDtype: DType;
          if (agg.func === 'count') {
            outDtype = DType.Int32;
          } else if (agg.func === 'mean') {
            outDtype = DType.Float64;
          } else {
            outDtype = srcDtype; // Preserve source dtype for sum/min/max/first/last
          }

          outputSchema[agg.outName] = outDtype;
        }

        return outputSchema;
      }

      case 'distinct':
        return QueryPlan.getOutputSchema(node.input);

      case 'sort':
        return QueryPlan.getOutputSchema(node.input);

      case 'join':
        return QueryPlan.buildJoinOutputSchema(node);
    }
  }

  /**
   * Get column order for a plan node
   */
  static getColumnOrder(node: PlanNode): string[] {
    switch (node.type) {
      case 'scan':
        return node.columnOrder;

      case 'filter':
        return QueryPlan.getColumnOrder(node.input);

      case 'select':
        return node.columns;

      case 'groupby': {
        const cols: string[] = [];
        cols.push(...node.groupKeys);
        for (const agg of node.aggregations) {
          cols.push(agg.outName);
        }
        return cols;
      }

      case 'distinct':
        return QueryPlan.getColumnOrder(node.input);

      case 'sort':
        return QueryPlan.getColumnOrder(node.input);

      case 'join': {
        const leftOrder = QueryPlan.getColumnOrder(node.left);
        const rightOrder = QueryPlan.getColumnOrder(node.right);
        const suffixes = node.suffixes ?? ['_x', '_y'];
        const { leftKeys, rightKeys } = QueryPlan.resolveJoinKeys(node);
        const rightKeySet = new Set(rightKeys);
        const leftNameSet = new Set(leftOrder);
        const overlapping = rightOrder.filter(
          (name) => !rightKeySet.has(name) && leftNameSet.has(name),
        );

        const cols: string[] = [];
        for (const name of leftOrder) {
          cols.push(overlapping.includes(name) ? `${name}${suffixes[0]}` : name);
        }
        for (const name of rightOrder) {
          if (rightKeySet.has(name)) continue;
          cols.push(overlapping.includes(name) ? `${name}${suffixes[1]}` : name);
        }
        return cols;
      }
    }
  }

  /**
   * Pretty-print a plan for debugging
   */
  static explain(node: PlanNode, indent = 0): string {
    const prefix = '  '.repeat(indent);
    let result = '';

    switch (node.type) {
      case 'scan':
        result += `${prefix}Scan: ${node.path}\n`;
        result += `${prefix}  Schema: ${Object.keys(node.schema).join(', ')}\n`;
        result += `${prefix}  Rows: unknown (streaming)\n`;
        break;

      case 'filter':
        result += `${prefix}Filter: ${node.column} ${node.operator} ${JSON.stringify(node.value)}\n`;
        result += QueryPlan.explain(node.input, indent + 1);
        break;

      case 'select':
        result += `${prefix}Select: ${node.columns.join(', ')}\n`;
        result += QueryPlan.explain(node.input, indent + 1);
        break;

      case 'groupby':
        result += `${prefix}GroupBy: ${node.groupKeys.join(', ')}\n`;
        result += `${prefix}  Aggregations:\n`;
        for (const agg of node.aggregations) {
          result += `${prefix}    ${agg.func}(${agg.col}) AS ${agg.outName}\n`;
        }
        result += QueryPlan.explain(node.input, indent + 1);
        break;

      case 'distinct': {
        const subset =
          node.subset && node.subset.length > 0 ? node.subset.join(', ') : 'all columns';
        result += `${prefix}Distinct: ${subset}\n`;
        result += QueryPlan.explain(node.input, indent + 1);
        break;
      }

      case 'sort': {
        const cols = node.columns.join(', ');
        const dirs =
          node.directions && node.directions.length > 0 ? node.directions.join(', ') : 'asc';
        result += `${prefix}Sort: ${cols} (${dirs})\n`;
        result += QueryPlan.explain(node.input, indent + 1);
        break;
      }

      case 'join': {
        const { leftKeys, rightKeys } = QueryPlan.resolveJoinKeys(node);
        const how = node.how ?? 'inner';
        result += `${prefix}Join (${how}): left[${leftKeys.join(', ')}] = right[${rightKeys.join(', ')}]\n`;
        result += `${prefix}  Left:\n`;
        result += QueryPlan.explain(node.left, indent + 2);
        result += `${prefix}  Right:\n`;
        result += QueryPlan.explain(node.right, indent + 2);
        break;
      }
    }

    return result;
  }
}
