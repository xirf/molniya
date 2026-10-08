import { expect } from "bun:test";
import {
  Batch,
  type ExecutionMetrics,
  type ExecutionOptions,
  type Query,
  type Scalar,
  type Schema,
  type SpillStore,
} from "../src/index.ts";

export function assertCode(run: () => unknown, code: string): void {
  try {
    run();
    throw new Error("Expected operation to fail");
  } catch (error) {
    expect((error as { code: string }).code).toBe(code);
  }
}
export async function rejectsCode(run: () => Promise<unknown>, code: string): Promise<void> {
  try {
    await run();
    throw new Error("Expected operation to fail");
  } catch (error) {
    expect((error as { code: string }).code).toBe(code);
  }
}
export async function collect(query: Query, options: ExecutionOptions = {}) {
  let metrics: ExecutionMetrics | undefined;
  try {
    return await query.collect({
      ...options,
      onFinish: (value) => {
        metrics = value;
        options.onFinish?.(value);
      },
    });
  } finally {
    if (metrics) expect(metrics.memory.used).toBe(0);
  }
}
export function seeded(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}
export class TestSpill implements SpillStore {
  files = new Map<string, readonly (readonly Scalar[])[]>();
  calls = 0;
  closed = false;
  next = 0;
  constructor(
    readonly failAt = Infinity,
    readonly corrupt?: (row: readonly Scalar[]) => readonly Scalar[],
  ) {}
  step(): void {
    if (++this.calls === this.failAt) throw new Error("injected I/O failure");
  }
  async write(rows: AsyncIterable<readonly Scalar[]>): Promise<string> {
    this.step();
    const id = String(this.next++);
    this.files.set(id, []);
    const stored: (readonly Scalar[])[] = [];
    for await (const row of rows) {
      this.step();
      stored.push([...row]);
      this.files.set(id, stored);
    }
    return id;
  }
  async *read(id: string): AsyncGenerator<readonly Scalar[]> {
    this.step();
    for (const row of this.files.get(id)!) {
      this.step();
      yield this.corrupt?.(row) ?? row;
    }
  }
  async remove(id: string): Promise<void> {
    this.step();
    this.files.delete(id);
  }
  async close(): Promise<void> {
    this.step();
    this.closed = true;
    this.files.clear();
  }
}
export function scalarRows(batch: Batch, fields: Schema) {
  return Array.from({ length: batch.length }, (_, row) =>
    fields.map((_, column) => batch.get(row, column)),
  );
}
export async function streamRows(query: Query, options: ExecutionOptions = {}) {
  const result = [];
  let metrics: ExecutionMetrics | undefined;
  try {
    for await (const row of query.rows({
      ...options,
      onFinish: (value) => {
        metrics = value;
        options.onFinish?.(value);
      },
    }))
      result.push(row);
    return result;
  } finally {
    if (metrics) expect(metrics.memory.used).toBe(0);
  }
}
