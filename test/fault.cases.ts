import { expect } from "bun:test";
import { mkdtemp, rm, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  Batch,
  Query,
  fromRows,
  fromCsv,
  readCsv,
  writeCsv,
  fileIO,
  schema,
  col,
  gt,
  lit,
  count,
  sum,
  type AtomicIO,
  type ExecutionMetrics,
  type Query as QueryType,
} from "../src/index.ts";
import { tc } from "./registry.ts";
import { collect, streamRows, rejectsCode, TestSpill } from "./helpers.ts";

const allocationQueries: [string, () => QueryType][] = [
  [
    "scan",
    () =>
      fromRows({ x: "i32", s: "utf8" }, [
        { x: 1, s: "a" },
        { x: 2, s: "b" },
      ]),
  ],
  [
    "dictionary",
    () =>
      fromRows(
        { s: "utf8" },
        Array.from({ length: 12 }, () => ({ s: "repeated-text" })),
      ),
  ],
  ["csv", () => fromCsv('x,s\n1,"a,b"\n2,c\n', { x: "i32", s: "utf8" })],
  [
    "filter-view",
    () =>
      fromRows({ x: "i32" }, [{ x: 0 }, { x: 1 }, { x: 2 }])
        .where(gt(col("x"), lit(0)))
        .select({ renamed: col("x") }),
  ],
  [
    "aggregate",
    () =>
      fromRows({ x: "i32" }, [{ x: 0 }, { x: 1 }, { x: 1 }]).aggregate(["x"], {
        n: count(),
        s: sum(col("x")),
      }),
  ],
  [
    "join",
    () =>
      fromRows({ x: "i32" }, [{ x: 1 }]).join(
        fromRows({ x: "i32", s: "utf8" }, [
          { x: 1, s: "a" },
          { x: 1, s: "b" },
        ]),
        { on: [["x", "x"]] },
      ),
  ],
  ["sort", () => fromRows({ x: "i32" }, [{ x: 2 }, { x: 1 }]).sort([{ column: "x" }])],
];
for (const [name, make] of allocationQueries)
  tc(
    {
      id: `TC-FAULT-ALLOC-${name}`,
      requirements: ["REQ-MEM-002", "REQ-LIFE-001"],
      category: "fault",
      objective: `Every allocation failure cleans ${name} execution`,
      steps:
        "Run a baseline; fail allocation ordinals1..baseline count individually; retry with a fresh execution",
      expected:
        "Every injected exception propagates; used bytes0 after each failure; unmodified plan still returns baseline rows",
    },
    async () => {
      const q = make();
      let total = 0;
      const expected = await collect(q, {
        onFinish: (m) => {
          total = m.memory.allocations;
        },
      });
      expect(total).toBeGreaterThan(0);
      for (let failAt = 1; failAt <= total; failAt++) {
        let metrics: ExecutionMetrics | undefined;
        let failed = false;
        try {
          await q.collect({
            beforeAllocate: (event) => {
              if (event.ordinal === failAt) throw new Error(`allocation ${failAt}`);
            },
            onFinish: (m) => {
              metrics = m;
            },
          });
        } catch (error) {
          failed = true;
          expect(String(error)).toContain(`allocation ${failAt}`);
        }
        expect(failed).toBe(true);
        expect(metrics!.memory.used).toBe(0);
      }
      expect(await collect(q)).toEqual(expected);
    },
  );

tc(
  {
    id: "TC-FAULT-BUDGET",
    requirements: ["REQ-MEM-001", "REQ-LIFE-001"],
    category: "boundary",
    objective: "Insufficient budgets fail without leaking partial columns or collected output",
    steps:
      "Collect100 rows with long strings using budget1024/batch64; separately attempt a join with excessive build state",
    expected: "MEMORY error; peak never exceeds configured limit; used0",
  },
  async () => {
    const q = fromRows(
      { x: "i32", s: "utf8" },
      Array.from({ length: 100 }, (_, x) => ({ x, s: "long text".repeat(4) })),
    );
    for (const query of [q, q.join(q, { on: [["x", "x"]] })])
      await rejectsCode(
        () =>
          collect(query, {
            memoryBytes: 1024,
            batchBytes: 64,
            onFinish: (m) => {
              expect(m.memory.peak).toBeLessThanOrEqual(1024);
              expect(m.memory.used).toBe(0);
            },
          }),
        "MEMORY",
      );
  },
);

tc(
  {
    id: "TC-LIFE-EARLY",
    requirements: ["REQ-LIFE-001", "REQ-REL-001"],
    category: "normal",
    objective: "Early consumer exit and limit close upstream cursors",
    steps:
      "Source yields20 one-row batches in try/finally; break after first row, then execute limit2",
    expected: "Cursor finally runs on each execution; only1 then2 batches opened; memory0",
  },
  async () => {
    const fields = schema({ x: "i32" });
    let produced = 0,
      closed = 0;
    const q = Query.fromSource({
      name: "cursor",
      schema: fields,
      async *open(ctx) {
        try {
          for (let x = 0; x < 20; x++) {
            produced++;
            yield Batch.fromValues(fields, [[x]], ctx.memory);
          }
        } finally {
          closed++;
        }
      },
    });
    for await (const row of q.rows({ onFinish: (m) => expect(m.memory.used).toBe(0) })) {
      expect(row.x).toBe(0);
      break;
    }
    expect(produced).toBe(1);
    expect(closed).toBe(1);
    expect(await collect(q.limit(2))).toEqual([{ x: 0 }, { x: 1 }]);
    expect(produced).toBe(3);
    expect(closed).toBe(2);
  },
);

tc(
  {
    id: "TC-LIFE-CANCEL",
    requirements: ["REQ-LIFE-001"],
    category: "fault",
    objective: "Cancellation before and during scanning closes the query",
    steps: "Abort once before execution, and once after the first of20 one-row batches",
    expected: "CANCELLED; cursors and allocations released",
  },
  async () => {
    const fields = schema({ x: "i32" });
    let closed = 0;
    const q = Query.fromSource({
      name: "cancel",
      schema: fields,
      async *open(ctx) {
        try {
          for (let x = 0; x < 20; x++) yield Batch.fromValues(fields, [[x]], ctx.memory);
        } finally {
          closed++;
        }
      },
    });
    const before = new AbortController();
    before.abort();
    await rejectsCode(() => collect(q, { signal: before.signal }), "CANCELLED");
    expect(closed).toBe(0);
    const during = new AbortController();
    await rejectsCode(async () => {
      for await (const row of q.rows({
        signal: during.signal,
        onFinish: (m) => expect(m.memory.used).toBe(0),
      })) {
        expect(row.x).toBe(0);
        during.abort();
      }
    }, "CANCELLED");
    expect(closed).toBe(1);
  },
);

tc(
  {
    id: "TC-LIFE-SOURCE-ERROR",
    requirements: ["REQ-LIFE-001"],
    category: "fault",
    objective: "Source exceptions preserve cleanup",
    steps:
      "Source yields one batch then throws 'source fault'; also attempt to read a missing CSV file",
    expected: "Original source error or ENOENT propagates; no engine allocations retained",
  },
  async () => {
    const fields = schema({ x: "i32" });
    let closed = false;
    const q = Query.fromSource({
      name: "failure",
      schema: fields,
      async *open(ctx) {
        try {
          yield Batch.fromValues(fields, [[1]], ctx.memory);
          throw new Error("source fault");
        } finally {
          closed = true;
        }
      },
    });
    await expect(collect(q)).rejects.toThrow("source fault");
    expect(closed).toBe(true);
    await expect(
      collect(readCsv(`/tmp/molniya-missing-${crypto.randomUUID()}`, fields)),
    ).rejects.toThrow();
  },
);

tc(
  {
    id: "TC-FAULT-SPILL",
    requirements: ["REQ-SPILL-001", "REQ-LIFE-001"],
    category: "fault",
    objective: "Every spill I/O failure releases state and temporary runs",
    steps:
      "Sort80 descending rows at budget4096/batch64; inject one failure at every store write, record, read, remove, and close call",
    expected: "Failure propagates, store closes and has no files, query budget returns to zero",
  },
  async () => {
    const q = fromRows(
      { x: "i32" },
      Array.from({ length: 80 }, (_, i) => ({ x: 80 - i })),
    ).sort([{ column: "x" }]);
    const baseline = new TestSpill();
    expect(
      (await streamRows(q, { memoryBytes: 4096, batchBytes: 64, spillStore: async () => baseline }))
        .length,
    ).toBe(80);
    for (let failAt = 1; failAt <= baseline.calls; failAt++) {
      const store = new TestSpill(failAt);
      let failed = false;
      try {
        await streamRows(q, { memoryBytes: 4096, batchBytes: 64, spillStore: async () => store });
      } catch (error) {
        failed = true;
        expect(String(error)).toContain("injected I/O failure");
      }
      expect(failed).toBe(true);
      expect(store.closed).toBe(true);
      expect(store.files.size).toBe(0);
    }
  },
);
for (const [name, corrupt] of [
  ["width", () => []],
  ["value", () => ["bad"]],
] as const)
  tc(
    {
      id: `TC-SPILL-CORRUPT-${name}`,
      requirements: ["REQ-SPILL-001"],
      category: "robustness",
      objective: `Reject corrupt spill ${name}`,
      steps: "Sort40 rows at budget2048; injected store returns wrong-width or wrong-type rows",
      expected: "IO error and all files/allocations cleaned",
    },
    async () => {
      const store = new TestSpill(Infinity, corrupt);
      const q = fromRows(
        { x: "i32" },
        Array.from({ length: 40 }, (_, x) => ({ x })),
      ).sort([{ column: "x" }]);
      await rejectsCode(
        () => streamRows(q, { memoryBytes: 2048, batchBytes: 64, spillStore: async () => store }),
        "IO",
      );
      expect(store.files.size).toBe(0);
    },
  );

tc(
  {
    id: "TC-IO-ATOMIC-FAULTS",
    requirements: ["REQ-IO-001"],
    category: "fault",
    objective: "Every output I/O failure preserves an old or complete new file",
    preconditions: "Unique directory and original destination bytes OLD",
    steps:
      "For failAt1 through first success: inject failure before nth create/write/sync/close/rename/remove; force writes to at most3 bytes; inspect destination and directory",
    expected:
      "Destination is OLD or exact 'x\n1\n2\n'; directory contains only destination after every attempt",
    cleanup: "Remove unique directory in finally",
  },
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "molniya-atomic-case-"));
    const path = join(directory, "result.csv");
    try {
      let reachedSuccess = false;
      for (let failAt = 1; failAt < 100; failAt++) {
        await Bun.write(path, "OLD");
        let calls = 0;
        const step = () => {
          if (++calls === failAt) throw new Error("injected output failure");
        };
        const io: AtomicIO = {
          async create(path) {
            step();
            const writer = await fileIO.create(path);
            return {
              async write(bytes) {
                step();
                return writer.write(bytes.subarray(0, 3));
              },
              async sync() {
                step();
                await writer.sync();
              },
              async close() {
                step();
                await writer.close();
              },
            };
          },
          async rename(a, b) {
            step();
            await fileIO.rename(a, b);
          },
          async remove(path) {
            step();
            await fileIO.remove(path);
          },
        };
        try {
          await writeCsv(path, fromRows({ x: "i32" }, [{ x: 1 }, { x: 2 }]), { io });
          reachedSuccess = true;
        } catch (error) {
          expect(String(error)).toContain("injected output failure");
        }
        expect(["OLD", "x\n1\n2\n"]).toContain(await Bun.file(path).text());
        expect(await readdir(directory)).toEqual(["result.csv"]);
        if (reachedSuccess) break;
      }
      expect(reachedSuccess).toBe(true);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  },
);

tc(
  {
    id: "TC-IO-ZERO-WRITE",
    requirements: ["REQ-IO-001"],
    category: "robustness",
    objective: "A stalled output writer fails instead of looping",
    steps: "Inject writer.write returning0 with no side effects",
    expected: "IO error; writer closed and temporary file removed",
  },
  async () => {
    let closed = false,
      removed = false;
    const io: AtomicIO = {
      async create() {
        return {
          async write() {
            return 0;
          },
          async sync() {},
          async close() {
            closed = true;
          },
        };
      },
      async rename() {
        throw new Error("must not rename");
      },
      async remove() {
        removed = true;
      },
    };
    await rejectsCode(() => writeCsv("unused", fromRows({ x: "i32" }, [{ x: 1 }]), { io }), "IO");
    expect(closed && removed).toBe(true);
  },
);

tc(
  {
    id: "TC-SOURCE-SCHEMA-ORDER",
    requirements: ["REQ-SCHEMA-001", "REQ-PLAN-001"],
    category: "regression",
    objective: "Source schema equality ignores object property insertion order",
    steps: "Declare {name,type,nullable}; source returns equivalent {nullable,type,name} field",
    expected: "One row x=1, without a false schema mismatch",
  },
  async () => {
    const fields = schema({ x: "i32" });
    const q = Query.fromSource({
      name: "field-order",
      schema: fields,
      async *open(ctx) {
        yield Batch.fromValues([{ nullable: false, type: "i32", name: "x" }], [[1]], ctx.memory);
      },
    });
    expect(await collect(q)).toEqual([{ x: 1 }]);
  },
);

tc(
  {
    id: "TC-FAULT-OUTPUT-ALLOC",
    requirements: ["REQ-MEM-002", "REQ-IO-001"],
    category: "fault",
    objective: "Output encoding allocation failures preserve the destination",
    steps:
      "Write a two-row query; inject each logical allocation failure, including header and row encoders; inspect old file and temporary directory",
    expected: "Each failure leaves OLD unchanged, no temporary files, and memory0",
    cleanup: "Remove unique temporary directory in finally",
  },
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "molniya-output-alloc-"));
    const path = join(directory, "result.csv");
    const q = fromRows({ x: "utf8" }, [{ x: "a,b" }, { x: "🧪" }]);
    try {
      let allocations = 0;
      await writeCsv(path, q, {
        onFinish: (m) => {
          allocations = m.memory.allocations;
        },
      });
      for (let failAt = 1; failAt <= allocations; failAt++) {
        await Bun.write(path, "OLD");
        await expect(
          writeCsv(path, q, {
            beforeAllocate: (event) => {
              if (event.ordinal === failAt) throw new Error("output allocation");
            },
            onFinish: (m) => expect(m.memory.used).toBe(0),
          }),
        ).rejects.toThrow("output allocation");
        expect(await Bun.file(path).text()).toBe("OLD");
        expect(await readdir(directory)).toEqual(["result.csv"]);
      }
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  },
);

tc(
  {
    id: "TC-SOURCE-WRONG-SCHEMA",
    requirements: ["REQ-SCHEMA-001", "REQ-LIFE-001"],
    category: "robustness",
    objective: "Reject a source that violates its declared schema",
    steps: "Declare x:i32; source returns y:i32 with one row",
    expected: "SCHEMA and no retained batch allocations",
  },
  async () => {
    const q = Query.fromSource({
      name: "bad-schema",
      schema: schema({ x: "i32" }),
      async *open(ctx) {
        yield Batch.fromValues(schema({ y: "i32" }), [[1]], ctx.memory);
      },
    });
    await rejectsCode(() => collect(q), "SCHEMA");
  },
);
