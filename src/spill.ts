import { mkdtemp, open, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { EngineError, requireThat } from "./errors.ts";
import type { Scalar } from "./schema.ts";
import { MemoryBudget } from "./memory.ts";

export interface SpillStore {
  write(rows: AsyncIterable<readonly Scalar[]>): Promise<string>;
  read(id: string): AsyncIterable<readonly Scalar[]>;
  remove(id: string): Promise<void>;
  close(): Promise<void>;
}

/** Query-private files. A run is visible only after its writer closes successfully. */
export class DiskSpillStore implements SpillStore {
  #next = 0;
  #closed = false;
  private constructor(
    private readonly directory: string,
    private readonly memory: MemoryBudget,
  ) {}
  static async create(
    parent = tmpdir(),
    memory = new MemoryBudget(64 * 1024 * 1024),
  ): Promise<DiskSpillStore> {
    return new DiskSpillStore(await mkdtemp(join(parent, "molniya-")), memory);
  }
  private path(id: string): string {
    requireThat(!this.#closed && /^run-\d+$/.test(id), "IO", "Invalid or closed spill handle");
    return join(this.directory, id);
  }
  async write(rows: AsyncIterable<readonly Scalar[]>): Promise<string> {
    const id = `run-${this.#next++}`;
    const path = this.path(id);
    const file = await open(path, "wx");
    try {
      for await (const row of rows) {
        const lease = this.memory.reserve(
          row.reduce<number>(
            (bytes, value) => bytes + (typeof value === "string" ? value.length * 18 + 64 : 128),
            256,
          ),
          "spill encoding",
        );
        try {
          const payload = JSON.stringify(row, (_, value) =>
            typeof value === "bigint"
              ? { i64: value.toString() }
              : Object.is(value, -0)
                ? { f64: "-0" }
                : value,
          );
          const bytes = Buffer.from(`M1\t${Bun.hash(payload).toString(16)}\t${payload}\n`);
          let offset = 0;
          while (offset < bytes.length) {
            const { bytesWritten } = await file.write(bytes, offset, bytes.length - offset);
            requireThat(bytesWritten > 0, "IO", "Spill write made no progress");
            offset += bytesWritten;
          }
        } finally {
          lease.release();
        }
      }
    } finally {
      await file.close();
    }
    return id;
  }
  async *read(id: string): AsyncGenerator<readonly Scalar[]> {
    const file = await open(this.path(id), "r");
    const size = Math.max(1, Math.min(16 * 1024, Math.floor(this.memory.limit / 64)));
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let bufferLease;
    const pendingLease = this.memory.reserve(0, "spill record text");
    let pending = "";
    try {
      bufferLease = this.memory.reserve(size * 3, "spill read buffer");
      const buffer = new Uint8Array(size);
      while (true) {
        const { bytesRead } = await file.read(buffer);
        let text: string;
        try {
          text =
            bytesRead === 0
              ? decoder.decode()
              : decoder.decode(buffer.subarray(0, bytesRead), { stream: true });
        } catch (cause) {
          throw new EngineError("IO", "Invalid spill UTF-8", { cause });
        }
        pendingLease.resize((pending.length + text.length) * 2);
        pending += text;
        let end: number;
        while ((end = pending.indexOf("\n")) >= 0) {
          const line = pending.slice(0, end);
          pending = pending.slice(end + 1);
          const prefix = /^M1\t([0-9a-f]+)\t/.exec(line);
          requireThat(prefix, "IO", "Invalid spill record header");
          const payload = line.slice(prefix[0].length);
          requireThat(
            Bun.hash(payload).toString(16) === prefix[1],
            "IO",
            "Spill record checksum mismatch",
          );
          let row: unknown;
          try {
            row = JSON.parse(payload, (_, value) => {
              if (
                value &&
                typeof value === "object" &&
                !Array.isArray(value) &&
                Object.keys(value).length === 1
              ) {
                if (typeof value.i64 === "string") return BigInt(value.i64);
                if (value.f64 === "-0") return -0;
              }
              return value;
            });
          } catch (cause) {
            throw new EngineError("IO", "Corrupt spill record", { cause });
          }
          requireThat(
            Array.isArray(row) &&
              row.every(
                (value) =>
                  value === null ||
                  ["string", "number", "boolean", "bigint"].includes(typeof value),
              ),
            "IO",
            "Corrupt spill row",
          );
          yield row as Scalar[];
          pendingLease.resize(pending.length * 2);
        }
        if (bytesRead === 0) break;
      }
      requireThat(pending.length === 0, "IO", "Truncated spill record");
    } finally {
      pendingLease.release();
      bufferLease?.release();
      await file.close();
    }
  }
  async remove(id: string): Promise<void> {
    await rm(this.path(id), { force: true });
  }
  async close(): Promise<void> {
    if (this.#closed) return;
    await rm(this.directory, { recursive: true, force: true });
    this.#closed = true;
  }
}
