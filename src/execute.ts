import { Batch } from "./column.ts";
import type { Context } from "./context.ts";
import { EngineError, requireThat } from "./errors.ts";
import { bind, type BoundExpr } from "./expr.ts";
import type { Aggregate, PhysicalPlan, Plan, SortKey, Source } from "./plan.ts";
import { compare, indexOf, keyOf, rowBytes, type Scalar, type Schema, validate } from "./schema.ts";
import { batches } from "./source.ts";

async function* values(input: AsyncIterable<Batch>): AsyncGenerator<readonly Scalar[]> {
  for await (const batch of input) {
    try {
      for (let row = 0; row < batch.length; row++) yield batch.values(row);
    } finally {
      batch.dispose();
    }
  }
}

function comparator(fields: Schema, keys: readonly SortKey[]) {
  const indices = keys.map((key) => indexOf(fields, key.column));
  return (a: readonly Scalar[], b: readonly Scalar[]) => {
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i]!;
      const x = a[indices[i]!]!;
      const y = b[indices[i]!]!;
      if (x === y) continue;
      if (x === null || y === null) return key.nulls === "first" ? -compare(x, y) : compare(x, y);
      const order = compare(x, y);
      if (order !== 0) return key.descending ? -order : order;
    }
    return 0;
  };
}

async function* readRun(
  id: string,
  fields: Schema,
  context: Context,
): AsyncGenerator<readonly Scalar[]> {
  const store = await context.store();
  const lease = context.memory.reserve(0, "spill cursor");
  try {
    for await (const row of store.read(id)) {
      context.check();
      requireThat(row.length === fields.length, "IO", "Spill schema mismatch");
      try {
        row.forEach((value, i) => validate(value, fields[i]!));
      } catch (cause) {
        throw new EngineError("IO", "Invalid spill value", { cause });
      }
      lease.resize(rowBytes(row));
      yield row;
    }
  } finally {
    lease.release();
  }
}

async function* merge(
  left: AsyncIterable<readonly Scalar[]>,
  right: AsyncIterable<readonly Scalar[]>,
  order: ReturnType<typeof comparator>,
  context: Context,
): AsyncGenerator<readonly Scalar[]> {
  const a = left[Symbol.asyncIterator]();
  const b = right[Symbol.asyncIterator]();
  try {
    let x = await a.next();
    let y = await b.next();
    while (!x.done || !y.done) {
      context.check();
      if (y.done || (!x.done && order(x.value, y.value) <= 0)) {
        yield x.value!;
        x = await a.next();
      } else {
        yield y.value;
        y = await b.next();
      }
    }
  } finally {
    const cleanup = await Promise.allSettled([a.return?.(), b.return?.()]);
    const failure = cleanup.find((result) => result.status === "rejected");
    if (failure?.status === "rejected") throw failure.reason;
  }
}

async function* sorted(
  input: AsyncIterable<readonly Scalar[]>,
  fields: Schema,
  keys: readonly SortKey[],
  context: Context,
): AsyncGenerator<readonly Scalar[]> {
  const order = comparator(fields, keys);
  const lease = context.memory.reserve(0, "sort run");
  const metadata = context.memory.reserve(0, "spill run directory");
  let rows: (readonly Scalar[])[] = [];
  let bytes = 0;
  const levels: (string | undefined)[] = [];
  const write = async (input: AsyncIterable<readonly Scalar[]>) => {
    const id = await (await context.store()).write(input);
    context.spillRuns++;
    return id;
  };
  const combine = async (left: string, right: string) => {
    const result = await write(
      merge(readRun(left, fields, context), readRun(right, fields, context), order, context),
    );
    const store = await context.store();
    await store.remove(left);
    await store.remove(right);
    return result;
  };
  const flush = async () => {
    rows.sort(order);
    let run = await write(
      (async function* () {
        yield* rows;
      })(),
    );
    rows = [];
    bytes = 0;
    lease.resize(0);
    let level = 0;
    while (levels[level] !== undefined) {
      run = await combine(levels[level]!, run);
      levels[level] = undefined;
      level++;
    }
    metadata.resize(Math.max(levels.length, level + 1) * 128);
    levels[level] = run;
  };
  try {
    for await (const row of input) {
      context.check();
      const size = rowBytes(row);
      if (rows.length > 0 && bytes + size > context.memory.limit / 4) await flush();
      lease.resize(bytes + size);
      bytes += size;
      rows.push(row);
    }
    if (levels.length === 0) {
      rows.sort(order);
      yield* rows;
      return;
    }
    if (rows.length > 0) await flush();
    let final: string | undefined;
    for (let i = levels.length - 1; i >= 0; i--)
      if (levels[i] !== undefined)
        final = final === undefined ? levels[i] : await combine(final, levels[i]!);
    if (final !== undefined) {
      yield* readRun(final, fields, context);
      await (await context.store()).remove(final);
    }
  } finally {
    lease.release();
    metadata.release();
  }
}

interface State {
  count: number;
  value: Scalar;
  seen: boolean;
}
interface Group {
  keys: readonly Scalar[];
  states: State[];
}
function group(keys: readonly Scalar[], aggregates: readonly Aggregate[]): Group {
  return { keys, states: aggregates.map(() => ({ count: 0, value: null, seen: false })) };
}
function groupBytes(value: Group, key: string): number {
  return (
    rowBytes(value.keys) +
    key.length * 2 +
    value.states.reduce(
      (bytes, state) => bytes + 64 + (typeof state.value === "string" ? state.value.length * 2 : 0),
      0,
    )
  );
}
function accumulate(
  value: Group,
  row: readonly Scalar[],
  aggregates: readonly Aggregate[],
  expressions: readonly (BoundExpr | undefined)[],
): void {
  aggregates.forEach((aggregate, i) => {
    const state = value.states[i]!;
    const item = expressions[i]?.evaluate((column) => row[column]!) ?? null;
    if (aggregate.op === "first") {
      if (!state.seen) state.value = item;
      state.seen = true;
      return;
    }
    if (aggregate.op === "last") {
      state.value = item;
      state.seen = true;
      return;
    }
    if (item === null && aggregate.expr) return;
    state.count++;
    requireThat(Number.isSafeInteger(state.count), "RANGE", "Aggregate count overflow");
    if (aggregate.op === "count") return;
    if (aggregate.op === "min" || aggregate.op === "max") {
      if (
        !state.seen ||
        (aggregate.op === "min" ? compare(item, state.value) < 0 : compare(item, state.value) > 0)
      )
        state.value = item;
    } else {
      state.value = !state.seen
        ? item
        : typeof item === "bigint"
          ? (state.value as bigint) + item
          : (state.value as number) + (item as number);
      validate(state.value, {
        name: aggregate.op,
        type: typeof state.value === "bigint" ? "i64" : "f64",
        nullable: false,
      });
    }
    state.seen = true;
  });
}
function finish(value: Group, aggregates: readonly Aggregate[]): readonly Scalar[] {
  return [
    ...value.keys,
    ...aggregates.map((aggregate, i) => {
      const state = value.states[i]!;
      return aggregate.op === "count"
        ? state.count
        : aggregate.op === "avg"
          ? state.count === 0
            ? null
            : (state.value as number) / state.count
          : state.value;
    }),
  ];
}

async function* aggregated(
  input: AsyncIterable<readonly Scalar[]>,
  plan: Extract<Plan, { kind: "aggregate" }>,
  strategy: string,
  context: Context,
): AsyncGenerator<readonly Scalar[]> {
  const indices = plan.keys.map((key) => indexOf(plan.input.schema, key));
  const specs = Object.values(plan.aggregates);
  const expressions = specs.map((spec) =>
    spec.expr ? bind(spec.expr, plan.input.schema) : undefined,
  );
  const lease = context.memory.reserve(0, "aggregate state");
  try {
    if (strategy === "hash-aggregate") {
      const groups = new Map<string, Group>();
      let bytes = 0;
      if (indices.length === 0) {
        const value = group([], specs);
        bytes = groupBytes(value, "[]");
        lease.resize(bytes);
        groups.set("[]", value);
      }
      for await (const row of input) {
        context.check();
        const keys = indices.map((index) => row[index]!);
        const key = keyOf(keys);
        let value = groups.get(key);
        const previous = value ? groupBytes(value, key) : 0;
        if (!value) value = group(keys, specs);
        accumulate(value, row, specs, expressions);
        bytes += groupBytes(value, key) - previous;
        lease.resize(bytes);
        groups.set(key, value);
      }
      for (const value of groups.values()) yield finish(value, specs);
      return;
    }
    const ordered =
      strategy === "sort-aggregate"
        ? sorted(
            input,
            plan.input.schema,
            plan.keys.map((column) => ({ column })),
            context,
          )
        : input;
    let active: Group | undefined;
    let previous: string | undefined;
    for await (const row of ordered) {
      const keys = indices.map((index) => row[index]!);
      const key = keyOf(keys);
      if (key !== previous) {
        if (active) yield finish(active, specs);
        active = group(keys, specs);
        previous = key;
      }
      accumulate(active!, row, specs, expressions);
      lease.resize(groupBytes(active!, key));
    }
    if (active) yield finish(active, specs);
  } finally {
    lease.release();
  }
}

async function* joined(
  plan: Extract<Plan, { kind: "join" }>,
  physical: PhysicalPlan,
  sources: ReadonlyMap<string, Source>,
  context: Context,
): AsyncGenerator<readonly Scalar[]> {
  const buildLeft = physical.strategy === "hash-left";
  const build = physical.children[buildLeft ? 0 : 1]!;
  const probe = physical.children[buildLeft ? 1 : 0]!;
  const buildIndices = plan.spec.on.map((pair) =>
    indexOf(build.logical.schema, pair[buildLeft ? 0 : 1]),
  );
  const probeIndices = plan.spec.on.map((pair) =>
    indexOf(probe.logical.schema, pair[buildLeft ? 1 : 0]),
  );
  const table = new Map<string, { row: readonly Scalar[]; matched: boolean }[]>();
  const entries: { row: readonly Scalar[]; matched: boolean }[] = [];
  const lease = context.memory.reserve(0, "join build");
  let bytes = 0;
  try {
    for await (const row of values(execute(build, sources, context))) {
      const keys = buildIndices.map((index) => row[index]!);
      const key = keyOf(keys);
      bytes += rowBytes(row) + key.length * 2 + 64;
      lease.resize(bytes);
      const entry = { row, matched: false };
      entries.push(entry);
      if (keys.some((value) => value === null)) continue;
      const bucket = table.get(key);
      if (bucket) bucket.push(entry);
      else table.set(key, [entry]);
    }
    for await (const row of values(execute(probe, sources, context))) {
      context.check();
      const keys = probeIndices.map((index) => row[index]!);
      const matches = keys.some((value) => value === null) ? [] : (table.get(keyOf(keys)) ?? []);
      if (plan.spec.how === "semi") {
        if (matches.length > 0) yield row;
        continue;
      }
      if (plan.spec.how === "anti") {
        if (matches.length === 0) yield row;
        continue;
      }
      for (const match of matches) {
        match.matched = true;
        yield buildLeft ? [...match.row, ...row] : [...row, ...match.row];
      }
      if (matches.length === 0 && (plan.spec.how === "left" || plan.spec.how === "full"))
        yield [...row, ...plan.right.schema.map(() => null)];
    }
    if (plan.spec.how === "full")
      for (const entry of entries)
        if (!entry.matched) yield [...plan.left.schema.map(() => null), ...entry.row];
  } finally {
    lease.release();
  }
}

export async function* execute(
  physical: PhysicalPlan,
  sources: ReadonlyMap<string, Source>,
  context: Context,
): AsyncGenerator<Batch> {
  context.check();
  const plan = physical.logical;
  if (plan.kind === "scan") {
    const source = sources.get(plan.source)!;
    for await (const batch of source.open(
      context,
      plan.schema.map((field) => field.name),
    )) {
      try {
        context.check();
        requireThat(
          batch.schema.length === plan.schema.length &&
            batch.schema.every(
              (field, i) =>
                field.name === plan.schema[i]!.name &&
                field.type === plan.schema[i]!.type &&
                field.nullable === plan.schema[i]!.nullable,
            ),
          "SCHEMA",
          "Source returned an unexpected schema",
        );
      } catch (error) {
        batch.dispose();
        throw error;
      }
      context.scannedRows += batch.length;
      yield batch;
    }
    return;
  }
  if (plan.kind === "concat") {
    yield* execute(physical.children[0]!, sources, context);
    yield* execute(physical.children[1]!, sources, context);
    return;
  }
  if (plan.kind === "join") {
    yield* batches(plan.schema, joined(plan, physical, sources, context), context);
    return;
  }
  if (plan.kind === "limit" && plan.count === 0) return;
  const input = execute(physical.children[0]!, sources, context);
  if (plan.kind === "sort") {
    yield* batches(
      plan.schema,
      sorted(values(input), plan.input.schema, plan.keys, context),
      context,
    );
    return;
  }
  if (plan.kind === "aggregate") {
    yield* batches(
      plan.schema,
      aggregated(values(input), plan, physical.strategy, context),
      context,
    );
    return;
  }
  if (plan.kind === "project" && physical.strategy === "evaluate") {
    const expressions = Object.values(plan.expressions).map((expr) =>
      bind(expr, plan.input.schema),
    );
    yield* batches(
      plan.schema,
      (async function* () {
        for await (const row of values(input))
          yield expressions.map((expr) => expr.evaluate((column) => row[column]!));
      })(),
      context,
    );
    return;
  }
  const predicate = plan.kind === "filter" ? bind(plan.predicate, plan.input.schema) : undefined;
  const indices =
    plan.kind === "project"
      ? Object.values(plan.expressions).map((expr) =>
          indexOf(plan.input.schema, (expr as { name: string }).name),
        )
      : [];
  let skipped = 0;
  let emitted = 0;
  for await (const batch of input) {
    try {
      context.check();
      if (plan.kind === "project") {
        yield batch.project(indices, plan.schema);
        continue;
      }
      const scratch = context.memory.reserve(batch.length * 8, "selection staging");
      try {
        const selected: number[] = [];
        for (let row = 0; row < batch.length; row++) {
          if (plan.kind === "filter") {
            if (predicate!.evaluate((column) => batch.get(row, column)) === true)
              selected.push(row);
          } else {
            if (skipped < plan.offset) {
              skipped++;
              continue;
            }
            if (emitted >= plan.count) break;
            selected.push(row);
            emitted++;
          }
        }
        if (selected.length > 0) yield batch.take(selected);
      } finally {
        scratch.release();
      }
      if (plan.kind === "limit" && emitted >= plan.count) return;
    } finally {
      batch.dispose();
    }
  }
}
