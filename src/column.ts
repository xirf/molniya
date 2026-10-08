import { integer, requireThat } from "./errors.ts";
import { type Lease, MemoryBudget } from "./memory.ts";
import {
  compare,
  schema as normalizeSchema,
  type Field,
  type Scalar,
  type Schema,
  validate,
  rowOf,
  type Row,
} from "./schema.ts";

export type Encoding = "fixed" | "utf8" | "dictionary";
export interface ColumnStats {
  readonly rows: number;
  readonly nulls: number;
  readonly distinct?: number;
  readonly min: Scalar;
  readonly max: Scalar;
  readonly bytes: number;
  readonly encoding: Encoding;
}
type Numeric = Int32Array | BigInt64Array | Float64Array | Uint8Array;
interface Strings {
  bytes: Uint8Array;
  offsets: Uint32Array;
  indices?: Uint32Array;
}
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

/** Storage never escapes this object. Batches own references; callers borrow columns. */
export class Column {
  #refs = 1;
  #data: Numeric | Strings;
  #valid: Uint8Array;
  #lease: Lease;
  private constructor(
    readonly field: Field,
    readonly stats: ColumnStats,
    data: Numeric | Strings,
    valid: Uint8Array,
    lease: Lease,
  ) {
    this.#data = data;
    this.#valid = valid;
    this.#lease = lease;
    Object.freeze(this);
  }

  static build(
    field: Field,
    values: readonly Scalar[],
    budget: MemoryBudget,
    force?: "utf8" | "dictionary",
  ): Column {
    field = normalizeSchema([field])[0]!;
    for (const value of values) validate(value, field);
    const bitmapBytes = Math.ceil(values.length / 8);
    let nulls = 0;
    let min: Scalar = null;
    let max: Scalar = null;
    for (const value of values) {
      if (value === null) {
        nulls++;
        continue;
      }
      if (min === null || compare(value, min) < 0) min = value;
      if (max === null || compare(value, max) > 0) max = value;
    }
    const scratch = budget.reserve(
      field.type === "utf8" ? values.length * 80 : 0,
      "encoding statistics",
    );
    let lease: Lease | undefined;
    try {
      let data: Numeric | Strings;
      let encoding: Encoding = "fixed";
      let distinct: number | undefined;
      let bytes: number;
      if (field.type === "utf8") {
        const dictionary = new Map<string, number>();
        let plainBytes = 0;
        let uniqueBytes = 0;
        for (const value of values) {
          if (value === null) continue;
          const text = value as string;
          const length = Buffer.byteLength(text, "utf8");
          plainBytes += length;
          if (!dictionary.has(text)) {
            dictionary.set(text, dictionary.size);
            uniqueBytes += length;
          }
        }
        const plainSize = plainBytes + 4 * (values.length + 1);
        const dictionarySize = uniqueBytes + 4 * (dictionary.size + 1) + 4 * values.length;
        encoding = force ?? (dictionarySize < plainSize ? "dictionary" : "utf8");
        distinct = dictionary.size;
        bytes = (encoding === "dictionary" ? dictionarySize : plainSize) + bitmapBytes;
        requireThat(bytes <= 0xffffffff, "RANGE", "String column exceeds 32-bit offset capacity");
        lease = budget.reserve(bytes, "column utf8");
        const texts = encoding === "dictionary" ? [...dictionary.keys()] : values;
        const buffer = new Uint8Array(encoding === "dictionary" ? uniqueBytes : plainBytes);
        const offsets = new Uint32Array(texts.length + 1);
        let position = 0;
        for (let i = 0; i < texts.length; i++) {
          offsets[i] = position;
          if (texts[i] !== null)
            position += encoder.encodeInto(texts[i] as string, buffer.subarray(position)).written;
        }
        offsets[texts.length] = position;
        let indices: Uint32Array | undefined;
        if (encoding === "dictionary") {
          indices = new Uint32Array(values.length);
          for (let i = 0; i < values.length; i++)
            if (values[i] !== null) indices[i] = dictionary.get(values[i] as string)!;
        }
        data = { bytes: buffer, offsets, indices };
      } else {
        const width = field.type === "bool" ? 1 : field.type === "i32" ? 4 : 8;
        bytes = width * values.length + bitmapBytes;
        lease = budget.reserve(bytes, `column ${field.type}`);
        data =
          field.type === "i64"
            ? new BigInt64Array(values.length)
            : field.type === "f64"
              ? new Float64Array(values.length)
              : field.type === "i32"
                ? new Int32Array(values.length)
                : new Uint8Array(values.length);
        for (let i = 0; i < values.length; i++) {
          const value = values[i];
          if (value === null) continue;
          if (data instanceof BigInt64Array) data[i] = value as bigint;
          else data[i] = typeof value === "boolean" ? Number(value) : (value as number);
        }
      }
      const valid = new Uint8Array(bitmapBytes);
      for (let i = 0; i < values.length; i++)
        if (values[i] !== null) valid[i >>> 3]! |= 1 << (i & 7);
      return new Column(
        field,
        Object.freeze({ rows: values.length, nulls, min, max, bytes, encoding, distinct }),
        data,
        valid,
        lease,
      );
    } catch (error) {
      lease?.release();
      throw error;
    } finally {
      scratch.release();
    }
  }

  get(index: number): Scalar {
    requireThat(this.#refs > 0, "DISPOSED", "Column has been released");
    integer(index, "row index");
    requireThat(index < this.stats.rows, "RANGE", "Row index out of bounds");
    if ((this.#valid[index >>> 3]! & (1 << (index & 7))) === 0) return null;
    if ("offsets" in this.#data) {
      const entry = this.#data.indices ? this.#data.indices[index]! : index;
      return decoder.decode(
        this.#data.bytes.subarray(this.#data.offsets[entry]!, this.#data.offsets[entry + 1]!),
      );
    }
    const value = this.#data[index]!;
    return this.field.type === "bool" ? value === 1 : value;
  }
  /** @internal */ retain(): void {
    requireThat(this.#refs > 0, "DISPOSED", "Column has been released");
    this.#refs++;
  }
  /** @internal */ release(): void {
    if (--this.#refs === 0) this.#lease.release();
  }
}

export class Batch {
  #disposed = false;
  private constructor(
    readonly schema: Schema,
    private readonly columns: readonly Column[],
    readonly length: number,
    private readonly budget: MemoryBudget,
    private readonly lease: Lease,
    private readonly selection: Uint32Array | null = null,
  ) {
    Object.freeze(this);
  }

  static fromValues(
    fields: Schema,
    rows: readonly (readonly Scalar[])[],
    budget: MemoryBudget,
    encoding?: "utf8" | "dictionary",
  ): Batch {
    fields = normalizeSchema(fields);
    for (const row of rows)
      requireThat(row.length === fields.length, "SCHEMA", "Row width differs from schema");
    const lease = budget.reserve(64 + fields.length * 16, "batch metadata");
    const columns: Column[] = [];
    try {
      for (let c = 0; c < fields.length; c++) {
        const scratch = budget.reserve(rows.length * 8, "column references");
        try {
          columns.push(
            Column.build(
              fields[c]!,
              rows.map((row) => row[c]!),
              budget,
              encoding,
            ),
          );
        } finally {
          scratch.release();
        }
      }
      return new Batch(fields, columns, rows.length, budget, lease);
    } catch (error) {
      for (const column of columns) column.release();
      lease.release();
      throw error;
    }
  }

  private active(): void {
    requireThat(!this.#disposed, "DISPOSED", "Batch has been released");
  }
  column(index: number): Column {
    this.active();
    integer(index, "column index");
    requireThat(index < this.columns.length, "RANGE", "Column index out of bounds");
    return this.columns[index]!;
  }
  get(row: number, column: number): Scalar {
    this.active();
    integer(row, "row index");
    requireThat(row < this.length, "RANGE", "Row index out of bounds");
    return this.column(column).get(this.selection ? this.selection[row]! : row);
  }
  values(row: number): Scalar[] {
    return this.schema.map((_, c) => this.get(row, c));
  }
  row(row: number): Row {
    return rowOf(this.schema, this.values(row));
  }
  take(indices: readonly number[]): Batch {
    this.active();
    const lease = this.budget.reserve(
      64 + this.columns.length * 16 + indices.length * 4,
      "selection",
    );
    try {
      const selection = new Uint32Array(indices.length);
      indices.forEach((index, i) => {
        integer(index, "selection index");
        requireThat(index < this.length, "RANGE", "Selection index out of bounds");
        selection[i] = this.selection ? this.selection[index]! : index;
      });
      for (const column of this.columns) column.retain();
      return new Batch(this.schema, this.columns, indices.length, this.budget, lease, selection);
    } catch (error) {
      lease.release();
      throw error;
    }
  }
  project(indices: readonly number[], fields: Schema): Batch {
    this.active();
    const columns = indices.map((index) => this.column(index));
    const lease = this.budget.reserve(
      64 + columns.length * 16 + (this.selection?.byteLength ?? 0),
      "projection",
    );
    try {
      const selection = this.selection?.slice() ?? null;
      for (const column of columns) column.retain();
      return new Batch(fields, columns, this.length, this.budget, lease, selection);
    } catch (error) {
      lease.release();
      throw error;
    }
  }
  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    for (const column of this.columns) column.release();
    this.lease.release();
  }
  [Symbol.dispose](): void {
    this.dispose();
  }
}
