import { open, rename, rm } from "node:fs/promises";
import type { Context, ExecutionOptions } from "./context.ts";
import { EngineError, integer, requireThat } from "./errors.ts";
import { Query } from "./query.ts";
import { schema, validate, type Scalar, type Schema, type SchemaSpec } from "./schema.ts";
import { batches } from "./source.ts";

export interface CsvOptions {
  readonly delimiter?: string;
  readonly header?: boolean;
  readonly nullToken?: string;
  readonly maxFieldChars?: number;
}
interface Cell {
  text: string;
  quoted: boolean;
}
function settings(options: CsvOptions) {
  const delimiter = options.delimiter ?? ",";
  requireThat(
    delimiter.length === 1 &&
      !["\r", "\n", '"', "\0"].includes(delimiter) &&
      delimiter.isWellFormed(),
    "CSV",
    "Invalid CSV delimiter",
  );
  const maxFieldChars = options.maxFieldChars ?? 1024 * 1024;
  integer(maxFieldChars, "maxFieldChars", 1);
  const nullToken = options.nullToken ?? "\\N";
  requireThat(
    nullToken.length > 0 && !nullToken.includes(delimiter) && !/["\r\n]/.test(nullToken),
    "CSV",
    "Null token must be a nonempty unquoted field",
  );
  return Object.freeze({ delimiter, header: options.header ?? true, nullToken, maxFieldChars });
}

async function* records(
  input: AsyncIterable<Uint8Array>,
  options: ReturnType<typeof settings>,
  context: Context,
): AsyncGenerator<Cell[]> {
  const lease = context.memory.reserve(0, "CSV record");
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  let state: "start" | "field" | "quoted" | "closed" = "start";
  let text = "";
  let quoted = false;
  let cells: Cell[] = [];
  let recordBytes = 0;
  let skipLF = false;
  let initial = true;
  let touched = false;
  const append = (char: string) => {
    requireThat(
      text.length + char.length <= options.maxFieldChars,
      "CSV",
      "CSV field exceeds maxFieldChars",
    );
    lease.resize(recordBytes + (text.length + char.length) * 2 + 32);
    text += char;
  };
  const field = () => {
    lease.resize(recordBytes + text.length * 2 + 32);
    recordBytes += text.length * 2 + 32;
    cells.push({ text, quoted });
    text = "";
    quoted = false;
    state = "start";
  };
  const record = () => {
    field();
    const result = cells;
    cells = [];
    recordBytes = 0;
    touched = false;
    lease.resize(0);
    return result;
  };
  const decode = (chunk?: Uint8Array) => {
    try {
      return chunk ? decoder.decode(chunk, { stream: true }) : decoder.decode();
    } catch (cause) {
      throw new EngineError("CSV", "Invalid UTF-8 input", { cause });
    }
  };
  async function* fragments() {
    for await (const chunk of input) yield { text: decode(chunk), bytes: chunk.byteLength };
    yield { text: decode(), bytes: 0 };
  }
  try {
    for await (const fragment of fragments()) {
      context.check();
      const scratch = context.memory.reserve(
        fragment.bytes + fragment.text.length * 2,
        "CSV decoding",
      );
      try {
        for (const char of fragment.text) {
          if (initial) {
            initial = false;
            if (char === "\ufeff") continue;
          }
          if (skipLF) {
            skipLF = false;
            if (char === "\n") continue;
          }
          if (state === "quoted") {
            if (char === '"') state = "closed";
            else append(char);
            continue;
          }
          if (state === "closed" && char === '"') {
            append(char);
            state = "quoted";
            continue;
          }
          if (char === options.delimiter) {
            field();
            touched = true;
            continue;
          }
          if (char === "\r" || char === "\n") {
            yield record();
            skipLF = char === "\r";
            continue;
          }
          requireThat(state !== "closed", "CSV", "Unexpected character after closing quote");
          if (char === '"') {
            requireThat(state === "start", "CSV", "Quote inside an unquoted field");
            state = "quoted";
            quoted = true;
            touched = true;
            continue;
          }
          append(char);
          state = "field";
          touched = true;
        }
      } finally {
        scratch.release();
      }
    }
    requireThat(state !== "quoted", "CSV", "Unterminated quoted field");
    if (touched || cells.length > 0) yield record();
  } finally {
    lease.release();
  }
}

function convert(cell: Cell, field: Schema[number], nullToken: string): Scalar {
  if (!cell.quoted && cell.text === nullToken) {
    validate(null, field);
    return null;
  }
  let value: Scalar;
  if (field.type === "utf8") value = cell.text;
  else if (field.type === "bool") {
    requireThat(
      cell.text === "true" || cell.text === "false",
      "CSV",
      `Invalid Boolean in ${field.name}`,
    );
    value = cell.text === "true";
  } else {
    const pattern =
      field.type === "f64" ? /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/ : /^[+-]?\d+$/;
    requireThat(pattern.test(cell.text), "CSV", `Invalid numeric input in ${field.name}`);
    value = field.type === "i64" ? BigInt(cell.text) : Number(cell.text);
  }
  validate(value, field);
  return value;
}

export function csvChunks(
  factory: (context: Context) => AsyncIterable<Uint8Array>,
  spec: SchemaSpec | Schema,
  options: CsvOptions = {},
): Query {
  const fields = schema(spec);
  const config = settings(options);
  return Query.fromSource({
    name: "csv",
    schema: fields,
    async *open(context, columns) {
      const indices = columns.map((name) => fields.findIndex((field) => field.name === name));
      const projected = Object.freeze(indices.map((index) => fields[index]!));
      async function* rows() {
        let header = !config.header;
        for await (const cells of records(factory(context), config, context)) {
          requireThat(
            cells.length === fields.length,
            "CSV",
            "CSV record width differs from schema",
          );
          if (!header) {
            requireThat(
              cells.every((cell, index) => cell.text === fields[index]!.name),
              "CSV",
              "CSV header differs from schema",
            );
            header = true;
            continue;
          }
          const values = cells.map((cell, index) =>
            convert(cell, fields[index]!, config.nullToken),
          );
          yield indices.map((index) => values[index]!);
        }
        requireThat(header, "CSV", "Missing CSV header");
      }
      yield* batches(projected, rows(), context);
    },
  });
}

export function fromCsv(text: string, spec: SchemaSpec | Schema, options: CsvOptions = {}): Query {
  requireThat(text.isWellFormed(), "CSV", "Invalid Unicode input");
  const bytes = new TextEncoder().encode(text);
  return csvChunks(
    async function* (context) {
      for (let offset = 0; offset < bytes.length; offset += context.batchBytes)
        yield bytes.subarray(offset, offset + context.batchBytes);
    },
    spec,
    options,
  );
}

export function readCsv(path: string, spec: SchemaSpec | Schema, options: CsvOptions = {}): Query {
  return csvChunks(
    async function* (context) {
      const file = await open(path, "r");
      let lease;
      try {
        const size = Math.min(64 * 1024, context.batchBytes);
        lease = context.memory.reserve(size, "CSV file buffer");
        const buffer = new Uint8Array(size);
        while (true) {
          context.check();
          const { bytesRead } = await file.read(buffer);
          if (bytesRead === 0) return;
          yield buffer.subarray(0, bytesRead);
        }
      } finally {
        lease?.release();
        await file.close();
      }
    },
    spec,
    options,
  );
}

export interface AtomicWriter {
  write(bytes: Uint8Array): Promise<number>;
  sync(): Promise<void>;
  close(): Promise<void>;
}
export interface AtomicIO {
  create(path: string): Promise<AtomicWriter>;
  rename(from: string, to: string): Promise<void>;
  remove(path: string): Promise<void>;
}
export const fileIO: AtomicIO = Object.freeze({
  async create(path: string) {
    const handle = await open(path, "wx");
    return {
      async write(bytes: Uint8Array) {
        return (await handle.write(bytes)).bytesWritten;
      },
      sync: () => handle.sync(),
      close: () => handle.close(),
    };
  },
  rename,
  remove: (path: string) => rm(path, { force: true }),
});

function format(value: Scalar, config: ReturnType<typeof settings>): string {
  if (value === null) return config.nullToken;
  const text = typeof value === "number" && Object.is(value, -0) ? "-0" : String(value);
  return text === config.nullToken || text.includes(config.delimiter) || /["\r\n]/.test(text)
    ? `"${text.replaceAll('"', '""')}"`
    : text;
}

export async function writeCsv(
  path: string,
  query: Query,
  options: CsvOptions & ExecutionOptions & { readonly io?: AtomicIO } = {},
): Promise<void> {
  const config = settings(options);
  const io = options.io ?? fileIO;
  const temporary = `${path}.molniya-${crypto.randomUUID()}.tmp`;
  let writer: AtomicWriter | undefined;
  const write = async (values: readonly Scalar[], context: Context) => {
    const size = values.reduce<number>(
      (bytes, value) => bytes + (typeof value === "string" ? value.length * 12 + 16 : 128),
      16,
    );
    const lease = context.memory.reserve(size, "CSV output encoding");
    try {
      const bytes = new TextEncoder().encode(
        values.map((value) => format(value, config)).join(config.delimiter) + "\n",
      );
      let offset = 0;
      while (offset < bytes.length) {
        const written = await writer!.write(bytes.subarray(offset));
        requireThat(
          Number.isSafeInteger(written) && written > 0 && written <= bytes.length - offset,
          "IO",
          "CSV write made invalid progress",
        );
        offset += written;
      }
    } finally {
      lease.release();
    }
  };
  try {
    writer = await io.create(temporary);
    await query.pipe(
      {
        async start(fields, context) {
          if (config.header)
            await write(
              fields.map((field) => field.name),
              context,
            );
        },
        async write(batch, context) {
          for (let row = 0; row < batch.length; row++) await write(batch.values(row), context);
        },
      },
      options,
    );
    await writer.sync();
    await writer.close();
    writer = undefined;
    await io.rename(temporary, path);
  } finally {
    try {
      await writer?.close();
    } finally {
      try {
        await io.remove(temporary);
      } catch (error) {
        await io.remove(temporary);
        throw error;
      }
    }
  }
}
