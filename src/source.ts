import { Batch } from "./column.ts";
import type { Context } from "./context.ts";
import { requireThat } from "./errors.ts";
import type { Source } from "./plan.ts";
import {
  indexOf,
  keyOf,
  rowBytes,
  schema,
  type Row,
  type Scalar,
  type Schema,
  type SchemaSpec,
  valuesOf,
} from "./schema.ts";

/** All row staging uses the same accounting and target size, irrespective of source. */
export async function* batches(
  fields: Schema,
  rows: AsyncIterable<readonly Scalar[]> | Iterable<readonly Scalar[]>,
  context: Context,
): AsyncGenerator<Batch> {
  const lease = context.memory.reserve(0, "row staging");
  let pending: (readonly Scalar[])[] = [];
  let bytes = 0;
  try {
    for await (const row of rows) {
      context.check();
      const size = rowBytes(row);
      if (pending.length > 0 && bytes + size > context.batchBytes) {
        const batch = Batch.fromValues(fields, pending, context.memory);
        pending = [];
        bytes = 0;
        lease.resize(0);
        yield batch;
      }
      lease.resize(bytes + size);
      bytes += size;
      pending.push(row);
    }
    if (pending.length > 0) {
      const batch = Batch.fromValues(fields, pending, context.memory);
      pending = [];
      lease.resize(0);
      yield batch;
    }
  } finally {
    lease.release();
  }
}

export function memorySource(
  spec: SchemaSpec | Schema,
  input: readonly Row[],
  name = "memory",
): Source {
  const fields = schema(spec);
  const rows = input.map((row) => Object.freeze(valuesOf(fields, row)));
  const distinct = Object.fromEntries(
    fields.map((field, c) => [field.name, new Set(rows.map((row) => keyOf([row[c]!]))).size]),
  );
  const statistics = Object.freeze({
    rows: rows.length,
    bytes: rows.reduce((n, row) => n + rowBytes(row), 0),
    distinct: Object.freeze(distinct),
  });
  return Object.freeze({
    name,
    schema: fields,
    statistics,
    async *open(context: Context, columns: readonly string[]) {
      const indices = columns.map((column) => indexOf(fields, column));
      const selectedSchema = Object.freeze(indices.map((index) => fields[index]!));
      yield* batches(
        selectedSchema,
        (function* () {
          for (const row of rows) yield indices.map((index) => row[index]!);
        })(),
        context,
      );
    },
  });
}

export function checkSource(source: Source): void {
  requireThat(
    typeof source.name === "string" && typeof source.open === "function",
    "PLAN",
    "Invalid source",
  );
  schema(source.schema);
}
