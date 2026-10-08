import { expect } from "bun:test";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { DiskSpillStore, MemoryBudget, Context, fromRows, type Scalar } from "../src/index.ts";
import { tc } from "./registry.ts";
import { rejectsCode, streamRows, TestSpill } from "./helpers.ts";

tc(
  {
    id: "TC-SPILL-CODEC",
    requirements: ["REQ-SPILL-001", "REQ-COLUMN-001"],
    category: "boundary",
    objective: "Spill records preserve all scalar representations",
    steps:
      "Write and read [null,true,false,-0,1.25,0n,9223372036854775807n,'🧪\\n']; close twice; attempt read after close",
    expected: "Exact scalar values and IO after close; zero accounted bytes",
    cleanup: "Close store and remove parent directory in finally",
  },
  async () => {
    const parent = await mkdtemp(join(tmpdir(), "molniya-codec-"));
    const memory = new MemoryBudget(65536);
    const store = await DiskSpillStore.create(parent, memory);
    const row: Scalar[] = [null, true, false, -0, 1.25, 0n, 9223372036854775807n, "🧪\n"];
    try {
      const id = await store.write(
        (async function* () {
          yield row;
        })(),
      );
      const rows = [];
      for await (const value of store.read(id)) rows.push(value);
      expect(rows).toEqual([row]);
      await store.remove(id);
      await store.close();
      await store.close();
      await rejectsCode(async () => {
        for await (const value of store.read(id)) void value;
      }, "IO");
      expect(memory.snapshot.used).toBe(0);
    } finally {
      await store.close();
      await rm(parent, { recursive: true, force: true });
    }
  },
);

tc(
  {
    id: "TC-SPILL-CORRUPTION-FUZZ",
    requirements: ["REQ-SPILL-001"],
    category: "fuzz",
    objective: "Every single-byte mutation of a spill record is rejected",
    steps:
      "Write a fixed record [1,'🧪',2n]; XOR each file byte with1 in isolation; additionally inject valid-checksum malformed JSON and wrong row types",
    expected: "Every mutated record raises IO; memory returns to zero after each read",
    cleanup: "Close store and remove parent directory in finally",
  },
  async () => {
    const parent = await mkdtemp(join(tmpdir(), "molniya-corruption-"));
    const memory = new MemoryBudget(65536);
    const store = await DiskSpillStore.create(parent, memory);
    try {
      const id = await store.write(
        (async function* () {
          yield [1, "🧪", 2n];
        })(),
      );
      const directory = (await readdir(parent))[0]!;
      const path = join(parent, directory, id);
      const original = await Bun.file(path).bytes();
      for (let position = 0; position < original.length; position++) {
        const bytes = Buffer.from(original);
        bytes[position]! ^= 1;
        await Bun.write(path, bytes);
        await rejectsCode(async () => {
          for await (const row of store.read(id)) void row;
        }, "IO");
        expect(memory.snapshot.used).toBe(0);
      }
      const payloads = ["[", "{}", '[{"i64":"bad"}]', '[{"other":1}]'];
      for (const payload of payloads) {
        await Bun.write(path, `M1\t${Bun.hash(payload).toString(16)}\t${payload}\n`);
        await rejectsCode(async () => {
          for await (const row of store.read(id)) void row;
        }, "IO");
      }
      await Bun.write(path, new Uint8Array([255]));
      await rejectsCode(async () => {
        for await (const row of store.read(id)) void row;
      }, "IO");
    } finally {
      await store.close();
      await rm(parent, { recursive: true, force: true });
    }
    expect(memory.snapshot.used).toBe(0);
  },
);

tc(
  {
    id: "TC-SPILL-DEFAULT-AND-CLEANUP",
    requirements: ["REQ-SPILL-001", "REQ-LIFE-001"],
    category: "robustness",
    objective: "Default storage closes and persistent cleanup failures remain visible",
    steps:
      "Create default store and close; create a context whose store.close always throws; call close",
    expected: "Default close succeeds; persistent cleanup is reported as AggregateError",
  },
  async () => {
    const store = await DiskSpillStore.create();
    await store.close();
    const context = new Context({
      spillStore: async () => ({
        async write() {
          return "unused";
        },
        async *read() {},
        async remove() {},
        async close() {
          throw new Error("persistent cleanup");
        },
      }),
    });
    await context.store();
    await expect(context.close()).rejects.toBeInstanceOf(AggregateError);
    const defaults = new Context();
    expect(defaults.memory.snapshot.used).toBe(0);
    await defaults.close();
  },
);

tc(
  {
    id: "TC-FAULT-DISK-ALLOC",
    requirements: ["REQ-MEM-002", "REQ-SPILL-001"],
    category: "fault",
    objective: "Every disk codec allocation failure cleans the query directory",
    steps:
      "Sort24 rows with budget4096/batch64 using real disk store; fail each allocation ordinal individually",
    expected:
      "Injected failure propagates; query memory0; unique parent directory empty after every run",
    cleanup: "Close stores through query cleanup and remove parent directory in finally",
  },
  async () => {
    const parent = await mkdtemp(join(tmpdir(), "molniya-disk-alloc-"));
    const q = fromRows(
      { x: "i32" },
      Array.from({ length: 24 }, (_, i) => ({ x: 24 - i })),
    ).sort([{ column: "x" }]);
    try {
      const spillStore = async (context: Context) => DiskSpillStore.create(parent, context.memory);
      let total = 0;
      await streamRows(q, {
        memoryBytes: 4096,
        batchBytes: 64,
        spillStore,
        onFinish: (m) => {
          total = m.memory.allocations;
        },
      });
      for (let failAt = 1; failAt <= total; failAt++) {
        await expect(
          streamRows(q, {
            memoryBytes: 4096,
            batchBytes: 64,
            spillStore,
            beforeAllocate: (event) => {
              if (event.ordinal === failAt) throw new Error("disk allocation");
            },
          }),
        ).rejects.toThrow("disk allocation");
        expect(await readdir(parent)).toEqual([]);
      }
      expect(await readdir(parent)).toEqual([]);
    } finally {
      await rm(parent, { recursive: true, force: true });
    }
  },
);
