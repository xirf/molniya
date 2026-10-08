import type { Batch } from "./column.ts";
import { Context, type ExecutionOptions } from "./context.ts";
import { integer, requireThat } from "./errors.ts";
import { execute } from "./execute.ts";
import { bind, col, snapshotExpr, type Expr } from "./expr.ts";
import {
  aggregateField,
  explain,
  optimize,
  physical,
  type Aggregate,
  type JoinHow,
  type Plan,
  type SortKey,
  type Source,
} from "./plan.ts";
import { checkSource, memorySource } from "./source.ts";
import { indexOf, rowBytes, schema, type Row, type Schema, type SchemaSpec } from "./schema.ts";
import type { Lease } from "./memory.ts";

export interface Sink {
  start?(schema: Schema, context: Context): Promise<void> | void;
  write(batch: Batch, context: Context): Promise<void> | void;
  close?(): Promise<void> | void;
}

const agg = (op: Aggregate["op"], expr?: Expr): Aggregate =>
  Object.freeze({ op, expr: expr ? snapshotExpr(expr) : undefined });
export const sum = (expr: Expr) => agg("sum", expr);
export const avg = (expr: Expr) => agg("avg", expr);
export const count = (expr?: Expr) => agg("count", expr);
export const min = (expr: Expr) => agg("min", expr);
export const max = (expr: Expr) => agg("max", expr);
export const first = (expr: Expr) => agg("first", expr);
export const last = (expr: Expr) => agg("last", expr);
let sourceId = 0;

async function close(context: Context): Promise<void> {
  try {
    await context.close();
  } finally {
    context.report();
  }
}

export class Query {
  private constructor(
    readonly plan: Plan,
    private readonly sources: ReadonlyMap<string, Source>,
  ) {
    Object.freeze(this);
  }
  static fromSource(source: Source): Query {
    checkSource(source);
    const fields = schema(source.schema);
    const statistics = structuredClone(source.statistics ?? {});
    if (statistics.rows !== undefined) integer(statistics.rows, "source row count");
    if (statistics.bytes !== undefined) integer(statistics.bytes, "source bytes");
    if (statistics.distinct) {
      for (const n of Object.values(statistics.distinct)) integer(n, "distinct count");
      Object.freeze(statistics.distinct);
    }
    if (statistics.ordering) {
      statistics.ordering.forEach(Object.freeze);
      Object.freeze(statistics.ordering);
    }
    Object.freeze(statistics);
    const id = `${source.name}:${sourceId++}`;
    const binding = Object.freeze({
      name: source.name,
      schema: fields,
      statistics,
      open: source.open.bind(source),
    });
    return new Query(
      Object.freeze({ kind: "scan", source: id, schema: fields, statistics }),
      new Map([[id, binding]]),
    );
  }
  get schema(): Schema {
    return this.plan.schema;
  }
  private next(plan: Plan): Query {
    return new Query(Object.freeze(plan), this.sources);
  }

  where(predicate: Expr): Query {
    const expr = snapshotExpr(predicate);
    requireThat(
      bind(expr, this.schema).type === "bool",
      "TYPE",
      "Filter predicate must be Boolean",
    );
    return this.next({ kind: "filter", schema: this.schema, input: this.plan, predicate: expr });
  }
  select(expressions: Readonly<Record<string, Expr>>): Query {
    const entries = Object.entries(expressions).map(
      ([name, expr]) => [name, snapshotExpr(expr)] as const,
    );
    const fields = schema(
      entries.map(([name, expr]) => {
        const bound = bind(expr, this.schema);
        return { name, type: bound.type, nullable: bound.nullable };
      }),
    );
    return this.next({
      kind: "project",
      schema: fields,
      input: this.plan,
      expressions: Object.freeze(Object.fromEntries(entries)),
    });
  }
  limit(count: number, offset = 0): Query {
    integer(count, "limit");
    integer(offset, "offset");
    return this.next({ kind: "limit", schema: this.schema, input: this.plan, count, offset });
  }
  sort(keys: readonly SortKey[]): Query {
    requireThat(keys.length > 0, "PLAN", "Sort needs at least one key");
    const copied = keys.map((key) => {
      indexOf(this.schema, key.column);
      requireThat(
        key.descending === undefined || typeof key.descending === "boolean",
        "PLAN",
        "Invalid sort direction",
      );
      requireThat(
        key.nulls === undefined || key.nulls === "first" || key.nulls === "last",
        "PLAN",
        "Invalid null order",
      );
      return Object.freeze({ ...key });
    });
    return this.next({
      kind: "sort",
      schema: this.schema,
      input: this.plan,
      keys: Object.freeze(copied),
    });
  }
  aggregate(keys: readonly string[], aggregates: Readonly<Record<string, Aggregate>>): Query {
    const entries = Object.entries(aggregates).map(([name, spec]) => {
      requireThat(
        ["sum", "avg", "count", "min", "max", "first", "last"].includes(spec.op),
        "PLAN",
        "Unknown aggregate",
      );
      requireThat(
        spec.expr !== undefined || spec.op === "count",
        "PLAN",
        `${spec.op} needs an expression`,
      );
      const copied = agg(spec.op, spec.expr);
      const expr = copied.expr ? bind(copied.expr, this.schema) : undefined;
      if (spec.op === "sum" || spec.op === "avg")
        requireThat(
          expr!.type === "i32" ||
            expr!.type === "f64" ||
            (spec.op === "sum" && expr!.type === "i64"),
          "TYPE",
          `${spec.op} requires a compatible numeric expression`,
        );
      return [name, copied] as const;
    });
    const fields = schema([
      ...keys.map((key) => this.schema[indexOf(this.schema, key)]!),
      ...entries.map(([name, spec]) => aggregateField(spec, this.schema, name)),
    ]);
    return this.next({
      kind: "aggregate",
      schema: fields,
      input: this.plan,
      keys: Object.freeze(keys.slice()),
      aggregates: Object.freeze(Object.fromEntries(entries)),
    });
  }
  distinct(keys: readonly string[] = this.schema.map((field) => field.name)): Query {
    requireThat(keys.length > 0, "PLAN", "Distinct needs at least one key");
    const others = this.schema.filter((field) => !keys.includes(field.name));
    return this.aggregate(
      keys,
      Object.fromEntries(others.map((field) => [field.name, first(col(field.name))])),
    ).select(Object.fromEntries(this.schema.map((field) => [field.name, col(field.name)])));
  }
  concat(other: Query): Query {
    requireThat(
      this.schema.length === other.schema.length &&
        this.schema.every(
          (field, i) =>
            field.name === other.schema[i]!.name &&
            field.type === other.schema[i]!.type &&
            field.nullable === other.schema[i]!.nullable,
        ),
      "SCHEMA",
      "Concat schemas differ",
    );
    return new Query(
      Object.freeze({ kind: "concat", schema: this.schema, left: this.plan, right: other.plan }),
      new Map([...this.sources, ...other.sources]),
    );
  }
  join(
    other: Query,
    options: {
      readonly on?: readonly (readonly [string, string])[];
      readonly how?: JoinHow;
      readonly suffix?: string;
    } = {},
  ): Query {
    const how = options.how ?? "inner";
    const pairs = options.on ?? [];
    requireThat(
      ["inner", "left", "full", "semi", "anti", "cross"].includes(how),
      "PLAN",
      "Unknown join type",
    );
    requireThat(
      how === "cross" ? pairs.length === 0 : pairs.length > 0,
      "PLAN",
      "Join keys do not match join type",
    );
    const on = pairs.map((pair) => {
      const left = this.schema[indexOf(this.schema, pair[0])]!;
      const right = other.schema[indexOf(other.schema, pair[1])]!;
      requireThat(left.type === right.type, "TYPE", "Join key types differ");
      return Object.freeze([pair[0], pair[1]] as const);
    });
    const suffix = options.suffix ?? "_right";
    const left = this.schema.map((field) => ({
      ...field,
      nullable: field.nullable || how === "full",
    }));
    const right =
      how === "semi" || how === "anti"
        ? []
        : other.schema.map((field) => ({
            ...field,
            name: this.schema.some((left) => left.name === field.name)
              ? field.name + suffix
              : field.name,
            nullable: field.nullable || how === "left" || how === "full",
          }));
    const fields = schema([...left, ...right]);
    const spec = Object.freeze({ on: Object.freeze(on), how, suffix });
    return new Query(
      Object.freeze({ kind: "join", schema: fields, left: this.plan, right: other.plan, spec }),
      new Map([...this.sources, ...other.sources]),
    );
  }

  explain(options: ExecutionOptions = {}): unknown {
    const context = new Context(options);
    return explain(physical(optimize(this.plan), context.memory.limit));
  }
  private async *owned(context: Context): AsyncGenerator<Batch> {
    for await (const batch of execute(
      physical(optimize(this.plan), context.memory.limit),
      this.sources,
      context,
    )) {
      context.outputRows += batch.length;
      yield batch;
    }
  }
  /** Each batch is borrowed until the next iteration; retain detached rows if needed. */
  async *stream(options: ExecutionOptions = {}): AsyncGenerator<Batch> {
    const context = new Context(options);
    try {
      for await (const batch of this.owned(context)) {
        try {
          yield batch;
        } finally {
          batch.dispose();
        }
      }
    } finally {
      await close(context);
    }
  }
  async *rows(options: ExecutionOptions = {}): AsyncGenerator<Row> {
    for await (const batch of this.stream(options))
      for (let row = 0; row < batch.length; row++) yield batch.row(row);
  }
  async pipe(sink: Sink, options: ExecutionOptions = {}): Promise<void> {
    const context = new Context(options);
    try {
      context.check();
      await sink.start?.(this.schema, context);
      for await (const batch of this.owned(context)) {
        try {
          await sink.write(batch, context);
        } finally {
          batch.dispose();
        }
      }
    } finally {
      try {
        await sink.close?.();
      } finally {
        await close(context);
      }
    }
  }
  async collect(options: ExecutionOptions = {}): Promise<Row[]> {
    let lease: Lease | undefined;
    let bytes = 0;
    const result: Row[] = [];
    await this.pipe(
      {
        start: (_, context) => {
          lease = context.memory.reserve(0, "collected output");
        },
        write(batch) {
          for (let row = 0; row < batch.length; row++) {
            bytes += rowBytes(batch.values(row));
            lease!.resize(bytes);
            result.push(batch.row(row));
          }
        },
        close: () => lease?.release(),
      },
      options,
    );
    return result;
  }
  async count(options: ExecutionOptions = {}): Promise<number> {
    let count = 0;
    await this.pipe(
      {
        write(batch) {
          count += batch.length;
          integer(count, "row count");
        },
      },
      options,
    );
    return count;
  }
}

export function fromRows(spec: SchemaSpec | Schema, rows: readonly Row[]): Query {
  return Query.fromSource(memorySource(spec, rows));
}
