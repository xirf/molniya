import type { Batch } from "./column.ts";
import type { Context } from "./context.ts";
import { and, bind, references, type Expr } from "./expr.ts";
import type { Schema } from "./schema.ts";

export interface Statistics {
  readonly rows?: number;
  readonly bytes?: number;
  readonly distinct?: Readonly<Record<string, number>>;
  readonly ordering?: readonly SortKey[];
}
export interface Source {
  readonly name: string;
  readonly schema: Schema;
  readonly statistics?: Statistics;
  /** Each invocation must return a fresh cursor. Ownership of each batch transfers to the executor. */
  open(context: Context, columns: readonly string[]): AsyncIterable<Batch>;
}
export interface SortKey {
  readonly column: string;
  readonly descending?: boolean;
  readonly nulls?: "first" | "last";
}
export interface Aggregate {
  readonly op: "sum" | "avg" | "count" | "min" | "max" | "first" | "last";
  readonly expr?: Expr;
}
export type JoinHow = "inner" | "left" | "full" | "semi" | "anti" | "cross";
export interface JoinSpec {
  readonly on: readonly (readonly [string, string])[];
  readonly how: JoinHow;
  readonly suffix: string;
}
interface Base {
  readonly schema: Schema;
}
export type Plan = Readonly<
  Base &
    (
      | { kind: "scan"; source: string; statistics: Statistics }
      | { kind: "filter"; input: Plan; predicate: Expr }
      | { kind: "project"; input: Plan; expressions: Readonly<Record<string, Expr>> }
      | { kind: "limit"; input: Plan; count: number; offset: number }
      | { kind: "sort"; input: Plan; keys: readonly SortKey[] }
      | {
          kind: "aggregate";
          input: Plan;
          keys: readonly string[];
          aggregates: Readonly<Record<string, Aggregate>>;
        }
      | { kind: "join"; left: Plan; right: Plan; spec: JoinSpec }
      | { kind: "concat"; left: Plan; right: Plan }
    )
>;

export interface PhysicalPlan {
  readonly logical: Plan;
  readonly strategy: string;
  readonly reason: string;
  readonly statistics: Statistics;
  readonly children: readonly PhysicalPlan[];
}

/** Conservative rewrites: preserve expression order and only remove unused scan columns. */
export function optimize(plan: Plan): Plan {
  if (plan.kind === "scan") return plan;
  if (plan.kind === "join" || plan.kind === "concat")
    return Object.freeze({ ...plan, left: optimize(plan.left), right: optimize(plan.right) });
  let input = optimize(plan.input);
  if (plan.kind === "filter" && input.kind === "filter") {
    return optimize(
      Object.freeze({
        ...plan,
        input: input.input,
        predicate: and(input.predicate, plan.predicate),
      }),
    );
  }
  if (plan.kind === "project") {
    const needed = new Set(Object.values(plan.expressions).flatMap((expr) => references(expr)));
    const filter = input.kind === "filter" ? input : undefined;
    if (filter) for (const name of references(filter.predicate)) needed.add(name);
    const scan = filter ? filter.input : input;
    if (scan.kind === "scan" && needed.size > 0) {
      const fields = Object.freeze(scan.schema.filter((field) => needed.has(field.name)));
      const pruned = Object.freeze({ ...scan, schema: fields });
      input = filter ? Object.freeze({ ...filter, input: pruned, schema: fields }) : pruned;
    }
  }
  return Object.freeze({ ...plan, input });
}

export function physical(plan: Plan, memoryBytes: number): PhysicalPlan {
  const children =
    plan.kind === "scan"
      ? []
      : plan.kind === "join" || plan.kind === "concat"
        ? [physical(plan.left, memoryBytes), physical(plan.right, memoryBytes)]
        : [physical(plan.input, memoryBytes)];
  const child = children[0]?.statistics ?? {};
  let statistics: Statistics = {};
  let strategy: string = plan.kind;
  let reason = "Preserves the declared query semantics";
  switch (plan.kind) {
    case "scan":
      statistics = plan.statistics;
      strategy = "column-scan";
      reason = "Source provides only the requested columns";
      break;
    case "filter":
      statistics = { ...child, rows: undefined, bytes: undefined };
      reason = "Selection view; null predicates are rejected";
      break;
    case "project":
      statistics = { rows: child.rows };
      strategy = Object.values(plan.expressions).every((expr) => expr.kind === "column")
        ? "column-view"
        : "evaluate";
      break;
    case "limit":
      statistics = {
        ...child,
        rows:
          child.rows === undefined
            ? undefined
            : Math.min(plan.count, Math.max(0, child.rows - plan.offset)),
      };
      reason = "Stops upstream consumption when the requested range is complete";
      break;
    case "sort":
      statistics = { ...child, ordering: plan.keys };
      strategy = "budgeted-sort";
      reason = "Runs use at most one quarter of the query budget; two-way merge bounds fan-in";
      break;
    case "aggregate": {
      let groups = plan.keys.length === 0 ? 1 : 1;
      for (const key of plan.keys) groups *= child.distinct?.[key] ?? Infinity;
      if (child.rows !== undefined) groups = Math.min(groups, child.rows);
      const ordered =
        plan.keys.length > 0 && plan.keys.every((key, i) => child.ordering?.[i]?.column === key);
      const estimate =
        groups * (128 + plan.keys.length * 32 + Object.keys(plan.aggregates).length * 64);
      strategy = ordered
        ? "ordered-aggregate"
        : estimate <= memoryBytes / 4
          ? "hash-aggregate"
          : "sort-aggregate";
      reason = ordered
        ? "Input already groups equal keys contiguously"
        : strategy === "hash-aggregate"
          ? `Estimated state ${estimate} bytes fits one quarter of the budget; allocations remain checked`
          : "Unknown or large group state uses external sorting and one active group";
      statistics = { rows: Number.isFinite(groups) ? groups : undefined };
      break;
    }
    case "join": {
      const leftBytes = child.bytes ?? Infinity;
      const rightBytes = children[1]!.statistics.bytes ?? Infinity;
      strategy = plan.spec.how === "inner" && leftBytes < rightBytes ? "hash-left" : "hash-right";
      reason =
        strategy === "hash-left"
          ? "Measured left input is smaller"
          : "Preserves outer/semi/anti semantics; build allocations are budget checked";
      break;
    }
    case "concat": {
      const rightRows = children[1]!.statistics.rows;
      statistics = {
        rows:
          child.rows === undefined || rightRows === undefined ? undefined : child.rows + rightRows,
      };
      break;
    }
  }
  return Object.freeze({
    logical: plan,
    strategy,
    reason,
    statistics: Object.freeze(statistics),
    children: Object.freeze(children),
  });
}

export function explain(plan: PhysicalPlan): unknown {
  return {
    operator: plan.logical.kind,
    strategy: plan.strategy,
    reason: plan.reason,
    columns: plan.logical.schema.map((field) => field.name),
    statistics: plan.statistics,
    children: plan.children.map(explain),
  };
}

export function aggregateField(aggregate: Aggregate, input: Schema, name: string) {
  const expression = aggregate.expr ? bind(aggregate.expr, input) : undefined;
  if (aggregate.op === "count")
    return Object.freeze({ name, type: "f64" as const, nullable: false });
  return Object.freeze({
    name,
    type:
      aggregate.op === "avg" || (expression!.type === "i32" && aggregate.op === "sum")
        ? ("f64" as const)
        : expression!.type,
    nullable: true,
  });
}
