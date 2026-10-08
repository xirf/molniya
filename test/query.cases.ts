import { expect } from "bun:test";
import {
  Query,
  Batch,
  fromRows,
  fromCsv,
  schema,
  col,
  lit,
  eq,
  gt,
  add,
  isNull,
  or,
  and,
  avg,
  count,
  sum,
  min,
  max,
  first,
  last,
  type JoinHow,
  type Row,
  type Expr,
} from "../src/index.ts";
import { tc } from "./registry.ts";
import { assertCode, collect, streamRows } from "./helpers.ts";

const sales = [
  { category: "b", amount: 10 },
  { category: "a", amount: 20 },
  { category: "b", amount: null },
  { category: "a", amount: 5 },
];
const salesSchema = schema({ category: "utf8", amount: { type: "f64", nullable: true } });

tc(
  {
    id: "TC-PLAN-SNAPSHOT",
    requirements: ["REQ-PLAN-001"],
    category: "normal",
    objective: "Plans capture caller data and expression snapshots",
    steps:
      "Create rows [x=1,x=2], select alias x+1, then mutate input row and supplied AST; run twice and concurrently",
    expected: "All executions return [{next:2},{next:3}]; plan and schema are frozen",
  },
  async () => {
    const rows = [{ x: 1 }, { x: 2 }];
    const expr = { kind: "column", name: "x" } as const;
    const q = fromRows({ x: "f64" }, rows).select({ next: add(expr, lit(1)) });
    rows[0]!.x = 999;
    (expr as { name: string }).name = "bad";
    expect(Object.isFrozen(q.plan)).toBe(true);
    expect(Object.isFrozen(q.schema[0])).toBe(true);
    const expected = [{ next: 2 }, { next: 3 }];
    expect(await collect(q)).toEqual(expected);
    expect(await Promise.all([collect(q), collect(q)])).toEqual([expected, expected]);
  },
);

tc(
  {
    id: "TC-PLAN-LAZY",
    requirements: ["REQ-PLAN-001", "REQ-REL-001"],
    category: "boundary",
    objective: "Construction, explain and limit zero do not open the source",
    steps:
      "Source open increments a counter; construct filter/select, explain, then limit(0).collect",
    expected: "Open count remains zero and result is empty",
  },
  async () => {
    let opens = 0;
    const fields = schema({ x: "f64" });
    const q = Query.fromSource({
      name: "lazy",
      schema: fields,
      async *open(ctx) {
        opens++;
        yield Batch.fromValues(fields, [[1]], ctx.memory);
      },
    })
      .where(gt(col("x"), lit(0)))
      .select({ x: col("x") });
    q.explain();
    expect(await collect(q.limit(0))).toEqual([]);
    expect(opens).toBe(0);
  },
);

tc(
  {
    id: "TC-PLAN-PRUNING",
    requirements: ["REQ-PLAN-003"],
    category: "normal",
    objective: "Scan pruning keeps filter dependencies and aliases",
    steps:
      "Source columns x,y,z; filter x>0 then x<10; project z as label; capture requested scan columns",
    expected: "Source receives x,z, fused filter yields label='keep'",
  },
  async () => {
    const fields = schema({ x: "f64", y: "f64", z: "utf8" });
    let requested: readonly string[] = [];
    const source = Query.fromSource({
      name: "prune",
      schema: fields,
      async *open(ctx, columns) {
        requested = columns;
        const selected = schema(
          columns.map((name) => fields.find((field) => field.name === name)!),
        );
        const data: Row[] = [
          { x: 1, y: 999, z: "keep" },
          { x: 20, y: 999, z: "drop" },
        ];
        yield Batch.fromValues(
          selected,
          data.map((row) => columns.map((name) => row[name]!)),
          ctx.memory,
        );
      },
    });
    const q = source
      .where(gt(col("x"), lit(0)))
      .where(eq(gt(col("x"), lit(10)), lit(false)))
      .select({ label: col("z") });
    expect(await collect(q)).toEqual([{ label: "keep" }]);
    expect(requested).toEqual(["x", "z"]);
    expect(JSON.stringify(q.explain())).toContain("column-view");
  },
);

tc(
  {
    id: "TC-PLAN-CONSTANT",
    requirements: ["REQ-PLAN-003", "REQ-REL-001"],
    category: "normal",
    objective: "Constant projections retain input multiplicity",
    steps: "Project literal 7 from three rows, including zero",
    expected: "Three rows of seven",
  },
  async () =>
    expect(
      await collect(fromRows({ x: "i32" }, [{ x: 0 }, { x: 1 }, { x: 2 }]).select({ v: lit(7) })),
    ).toEqual([{ v: 7 }, { v: 7 }, { v: 7 }]),
);

for (const [name, query, strategy] of [
  [
    "hash",
    fromRows(salesSchema, sales).aggregate(["category"], { total: sum(col("amount")) }),
    "hash-aggregate",
  ],
  [
    "ordered",
    fromRows(salesSchema, sales)
      .sort([{ column: "category" }])
      .aggregate(["category"], { total: sum(col("amount")) }),
    "ordered-aggregate",
  ],
  [
    "unknown",
    fromCsv("category,amount\nb,10\na,20\nb,\\N\na,5\n", salesSchema).aggregate(["category"], {
      total: sum(col("amount")),
    }),
    "sort-aggregate",
  ],
] as const)
  tc(
    {
      id: `TC-STRATEGY-${name}`,
      requirements: ["REQ-PLAN-002", "REQ-AGG-001"],
      category: "differential",
      objective: `${strategy} preserves group results`,
      steps: "Aggregate the same b:10,null and a:20,5 rows; inspect explain; sort final groups",
      expected: "a totals25, b totals10; declared physical strategy selected",
    },
    async () => {
      expect((query.explain() as { strategy: string }).strategy).toBe(strategy);
      expect(await collect(query.sort([{ column: "category" }]))).toEqual([
        { category: "a", total: 25 },
        { category: "b", total: 10 },
      ]);
    },
  );

tc(
  {
    id: "TC-AGG-ALL",
    requirements: ["REQ-AGG-001"],
    category: "normal",
    objective: "All aggregates honor null and encounter-order semantics",
    steps: "Global aggregate [null,2,4,null]: count(*),count(x),sum,avg,min,max,first,last",
    expected: "4,2,6,3,2,4,null,null",
  },
  async () => {
    const q = fromRows({ x: { type: "f64", nullable: true } }, [
      { x: null },
      { x: 2 },
      { x: 4 },
      { x: null },
    ]);
    expect(
      await collect(
        q.aggregate([], {
          all: count(),
          n: count(col("x")),
          s: sum(col("x")),
          a: avg(col("x")),
          lo: min(col("x")),
          hi: max(col("x")),
          f: first(col("x")),
          l: last(col("x")),
        }),
      ),
    ).toEqual([{ all: 4, n: 2, s: 6, a: 3, lo: 2, hi: 4, f: null, l: null }]);
  },
);
for (const [name, rows] of [
  ["empty", []],
  ["all-null", [{ x: null }, { x: null }]],
] as const)
  tc(
    {
      id: `TC-AGG-${name}`,
      requirements: ["REQ-AGG-001"],
      category: "boundary",
      objective: `Global aggregates handle ${name} input`,
      steps: "Aggregate x with count,sum,avg,min,max,first,last",
      expected: "Count0; other aggregates null",
    },
    async () => {
      const q = fromRows({ x: { type: "f64", nullable: true } }, rows);
      expect(
        await collect(
          q.aggregate([], {
            n: count(col("x")),
            s: sum(col("x")),
            a: avg(col("x")),
            lo: min(col("x")),
            hi: max(col("x")),
            f: first(col("x")),
            l: last(col("x")),
          }),
        ),
      ).toEqual([{ n: 0, s: null, a: null, lo: null, hi: null, f: null, l: null }]);
    },
  );

tc(
  {
    id: "TC-AGG-GROUP-NULL",
    requirements: ["REQ-AGG-001"],
    category: "boundary",
    objective: "Null keys form one group and BigInt sums stay exact",
    steps: "Group [(null,9007199254740993n),(null,1n),('a',2n)]",
    expected: "null sums9007199254740994n and a sums2n",
  },
  async () => {
    const q = fromRows({ k: { type: "utf8", nullable: true }, v: "i64" }, [
      { k: null, v: 9007199254740993n },
      { k: null, v: 1n },
      { k: "a", v: 2n },
    ])
      .aggregate(["k"], { s: sum(col("v")) })
      .sort([{ column: "k" }]);
    expect(await collect(q)).toEqual([
      { k: "a", s: 2n },
      { k: null, s: 9007199254740994n },
    ]);
  },
);

tc(
  {
    id: "TC-AGG-EMPTY-GROUPED",
    requirements: ["REQ-AGG-001"],
    category: "boundary",
    objective: "Empty grouped input creates no groups",
    steps: "Aggregate empty memory and CSV sources by x",
    expected: "Both return zero rows",
  },
  async () => {
    for (const q of [fromRows({ x: "i32" }, []), fromCsv("x\n", { x: "i32" })])
      expect(await collect(q.aggregate(["x"], { n: count() }))).toEqual([]);
  },
);

for (const [limit, offset, expected] of [
  [0, 0, []],
  [1, 0, [0]],
  [2, 1, [1, 2]],
  [5, 2, [2]],
  [1, 3, []],
  [1, 4, []],
] as const)
  tc(
    {
      id: `TC-LIMIT-${limit}-${offset}`,
      requirements: ["REQ-REL-001"],
      category: "boundary",
      objective: `Limit ${limit} offset ${offset}`,
      steps: "Apply each range to x=[0,1,2] in a single source batch",
      expected: JSON.stringify(expected),
    },
    async () =>
      expect(
        await collect(fromRows({ x: "i32" }, [{ x: 0 }, { x: 1 }, { x: 2 }]).limit(limit, offset), {
          batchBytes: 1024,
        }),
      ).toEqual(expected.map((x) => ({ x }))),
  );

for (const descending of [false, true])
  for (const nulls of ["first", "last"] as const)
    tc(
      {
        id: `TC-SORT-${descending}-${nulls}`,
        requirements: ["REQ-SORT-001"],
        category: "normal",
        objective: `Sort direction ${descending} and nulls ${nulls} independently`,
        steps: "Stable sort [(2,a),(null,b),(1,c),(2,d)] by key",
        expected: "Equal key2 rows retain a,d order; null placement independent of direction",
      },
      async () => {
        const q = fromRows({ key: { type: "i32", nullable: true }, tag: "utf8" }, [
          { key: 2, tag: "a" },
          { key: null, tag: "b" },
          { key: 1, tag: "c" },
          { key: 2, tag: "d" },
        ]).sort([{ column: "key", descending, nulls }]);
        const tags = descending ? ["a", "d", "c"] : ["c", "a", "d"];
        if (nulls === "first") tags.unshift("b");
        else tags.push("b");
        expect((await collect(q)).map((row) => row.tag)).toEqual(tags);
      },
    );

tc(
  {
    id: "TC-SORT-SPILL",
    requirements: ["REQ-SORT-001", "REQ-SPILL-001", "REQ-MEM-001"],
    category: "differential",
    objective: "External sort equals stable in-memory multikey sort",
    steps:
      "240 rows keyed by i%7 and -i, tagged i; compare memory budgets1MiB and8192 bytes with batch128",
    expected:
      "Both outputs match native stable sort; small execution spills, peak never exceeds8192",
  },
  async () => {
    const rows = Array.from({ length: 240 }, (_, i) => ({ k: i % 7, v: -i, tag: i }));
    const q = fromRows({ k: "i32", v: "f64", tag: "i32" }, rows).sort([
      { column: "k" },
      { column: "v", descending: true },
    ]);
    const expected = rows.toSorted((a, b) => a.k - b.k || b.v - a.v);
    expect(await streamRows(q)).toEqual(expected);
    let runs = 0;
    expect(
      await streamRows(q, {
        memoryBytes: 8192,
        batchBytes: 128,
        onFinish: (m) => {
          runs = m.spillRuns;
          expect(m.memory.peak).toBeLessThanOrEqual(8192);
        },
      }),
    ).toEqual(expected);
    expect(runs).toBeGreaterThan(0);
  },
);

tc(
  {
    id: "TC-AGG-SPILL",
    requirements: ["REQ-AGG-001", "REQ-PLAN-002", "REQ-SPILL-001"],
    category: "differential",
    objective: "Unknown-cardinality aggregation spills and preserves encounter order within keys",
    steps: "CSV300 rows k=i%11,v=i; group sum,first,last at budget8192/batch128",
    expected: "Same per-key values as independent array reductions; at least one spill",
  },
  async () => {
    const rows = Array.from({ length: 300 }, (_, i) => ({ k: i % 11, v: i }));
    const csv = "k,v\n" + rows.map((row) => `${row.k},${row.v}`).join("\n");
    const q = fromCsv(csv, { k: "i32", v: "i32" }).aggregate(["k"], {
      s: sum(col("v")),
      f: first(col("v")),
      l: last(col("v")),
    });
    let runs = 0;
    const actual = await streamRows(q, {
      memoryBytes: 8192,
      batchBytes: 128,
      onFinish: (m) => {
        runs = m.spillRuns;
      },
    });
    const expected = Array.from({ length: 11 }, (_, k) => {
      const values = rows.filter((row) => row.k === k).map((row) => row.v);
      return { k, s: values.reduce((a, b) => a + b, 0), f: values[0]!, l: values.at(-1)! };
    });
    expect(actual).toEqual(expected);
    expect(runs).toBeGreaterThan(0);
  },
);

const leftRows = [
  { id: 1, v: "a" },
  { id: 1, v: "b" },
  { id: null, v: "c" },
  { id: 2, v: "d" },
];
const rightRows = [
  { id: 1, w: "p" },
  { id: 1, w: "q" },
  { id: null, w: "r" },
  { id: 3, w: "s" },
];
for (const how of ["inner", "left", "full", "semi", "anti", "cross"] as JoinHow[])
  tc(
    {
      id: `TC-JOIN-${how}`,
      requirements: ["REQ-JOIN-001"],
      category: "normal",
      objective: `${how} join handles duplicates and nulls`,
      steps: "Join left [(1,a),(1,b),(null,c),(2,d)] and right [(1,p),(1,q),(null,r),(3,s)]",
      expected: `${{ inner: 4, left: 6, full: 8, semi: 2, anti: 2, cross: 16 }[how]} rows matching independent nested loops`,
    },
    async () => {
      const left = fromRows({ id: { type: "i32", nullable: true }, v: "utf8" }, leftRows);
      const right = fromRows({ id: { type: "i32", nullable: true }, w: "utf8" }, rightRows);
      const expected: Row[] = [];
      for (const a of leftRows) {
        const matches = rightRows.filter(
          (b) => how === "cross" || (a.id !== null && b.id !== null && a.id === b.id),
        );
        if (how === "semi") {
          if (matches.length) expected.push(a);
          continue;
        }
        if (how === "anti") {
          if (!matches.length) expected.push(a);
          continue;
        }
        for (const b of matches) expected.push({ ...a, id_right: b.id, w: b.w });
        if (!matches.length && (how === "left" || how === "full"))
          expected.push({ ...a, id_right: null, w: null });
      }
      if (how === "full")
        for (const b of rightRows)
          if (!leftRows.some((a) => a.id !== null && b.id !== null && a.id === b.id))
            expected.push({ id: null, v: null, id_right: b.id, w: b.w });
      expect(
        await collect(left.join(right, { how, on: how === "cross" ? [] : [["id", "id"]] })),
      ).toEqual(expected);
    },
  );

tc(
  {
    id: "TC-JOIN-SMALLER",
    requirements: ["REQ-PLAN-002", "REQ-JOIN-001"],
    category: "normal",
    objective: "Inner join builds the measured smaller left side",
    steps: "Join one left row id1 against ten right rows id1",
    expected: "hash-left plan; ten matching rows with original column order",
  },
  async () => {
    const a = fromRows({ id: "i32" }, [{ id: 1 }]);
    const b = fromRows(
      { id: "i32", v: "i32" },
      Array.from({ length: 10 }, (_, v) => ({ id: 1, v })),
    );
    const q = a.join(b, { on: [["id", "id"]] });
    expect((q.explain() as { strategy: string }).strategy).toBe("hash-left");
    expect(await collect(q)).toEqual(
      Array.from({ length: 10 }, (_, v) => ({ id: 1, id_right: 1, v })),
    );
  },
);

tc(
  {
    id: "TC-SET",
    requirements: ["REQ-SET-001"],
    category: "normal",
    objective: "Concat preserves order and distinct retains first rows including null keys",
    steps: "Concat [(null,a),(1,b)] with [(null,c),(1,d),(2,e)]; distinct on k",
    expected: "Concat has five rows; distinct retains (null,a),(1,b),(2,e)",
  },
  async () => {
    const fields = schema({ k: { type: "i32", nullable: true }, v: "utf8" });
    const rows = [
      { k: null, v: "a" },
      { k: 1, v: "b" },
      { k: null, v: "c" },
      { k: 1, v: "d" },
      { k: 2, v: "e" },
    ];
    const q = fromRows(fields, rows.slice(0, 2)).concat(fromRows(fields, rows.slice(2)));
    expect(await collect(q)).toEqual(rows);
    expect(await collect(q.distinct(["k"]))).toEqual([rows[0]!, rows[1]!, rows[4]!]);
    expect(
      await collect(fromRows({ x: "i32" }, [{ x: 1 }, { x: 1 }, { x: 2 }]).distinct()),
    ).toEqual([{ x: 1 }, { x: 2 }]);
  },
);

const base = fromRows({ x: "i32", text: "utf8" }, [{ x: 1, text: "a" }]);
const invalidPlans: [string, () => unknown, string][] = [
  ["nonboolean", () => base.where(col("x")), "TYPE"],
  ["negative-limit", () => base.limit(-1), "RANGE"],
  ["negative-offset", () => base.limit(1, -1), "RANGE"],
  ["empty-sort", () => base.sort([]), "PLAN"],
  ["unknown-sort", () => base.sort([{ column: "bad" }]), "SCHEMA"],
  ["sort-direction", () => base.sort([{ column: "x", descending: 3 as never }]), "PLAN"],
  ["sort-nulls", () => base.sort([{ column: "x", nulls: "other" as never }]), "PLAN"],
  ["empty-select", () => base.select({}), "SCHEMA"],
  ["unknown-select", () => base.select({ v: col("bad") }), "SCHEMA"],
  ["missing-agg", () => base.aggregate([], { s: { op: "sum" } }), "PLAN"],
  ["string-sum", () => base.aggregate([], { s: sum(col("text")) }), "TYPE"],
  ["bigint-avg", () => fromRows({ x: "i64" }, []).aggregate([], { s: avg(col("x")) }), "TYPE"],
  ["unknown-agg", () => base.aggregate([], { s: { op: "no" as never } }), "PLAN"],
  ["duplicate-agg", () => base.aggregate(["x"], { x: count() }), "SCHEMA"],
  ["empty-distinct", () => base.distinct([]), "PLAN"],
  ["concat-types", () => base.concat(fromRows({ x: "f64" }, [])), "SCHEMA"],
  ["join-keys", () => base.join(base), "PLAN"],
  ["cross-keys", () => base.join(base, { how: "cross", on: [["x", "x"]] }), "PLAN"],
  ["join-type", () => base.join(base, { how: "other" as never }), "PLAN"],
  ["join-mismatch", () => base.join(base, { on: [["x", "text"]] }), "TYPE"],
  ["join-names", () => base.join(base, { on: [["x", "x"]], suffix: "" }), "SCHEMA"],
  ["bad-batch", () => base.explain({ memoryBytes: 100, batchBytes: 26 }), "RANGE"],
  ["zero-batch", () => base.explain({ batchBytes: 0 }), "RANGE"],
];
for (const [name, run, code] of invalidPlans)
  tc(
    {
      id: `TC-PLAN-INVALID-${name}`,
      requirements: ["REQ-PLAN-001", "REQ-SCHEMA-001"],
      category: "robustness",
      objective: `Reject invalid plan ${name}`,
      steps: "Construct the explicit invalidPlans entry",
      expected: code,
    },
    () => assertCode(run, code),
  );

const decisionCases = [
  [true, true, false, true],
  [false, true, false, false],
  [true, false, false, false],
  [true, false, true, true],
] as const;
for (const [i, a, b, c, expected] of decisionCases.map((row, i) => [i, ...row] as const))
  tc(
    {
      id: `TC-MCDC-${i}`,
      requirements: ["REQ-NULL-001"],
      category: "decision",
      objective: `Independence witness ${i} for A AND (B OR C)`,
      steps: `A=${a},B=${b},C=${c}; filter singleton by A AND (B OR C)`,
      expected: expected ? "One row" : "Zero rows",
    },
    async () => {
      const q = fromRows({ a: "bool", b: "bool", c: "bool" }, [{ a, b, c }]).where(
        and(col("a"), or(col("b"), col("c"))),
      );
      expect(await q.count()).toBe(expected ? 1 : 0);
    },
  );

tc(
  {
    id: "TC-SORT-UNICODE",
    requirements: ["REQ-SORT-001", "REQ-EXPR-001"],
    category: "boundary",
    objective: "String ordering follows Unicode scalar order and handles prefixes",
    steps:
      "Sort ['', 'aa', 'a', U+E000, U+1F9EA, '🧪x', '🧪a']; compare with explicit scalar-order fixture",
    expected: "Empty,a,aa,U+E000,🧪,🧪a,🧪x",
  },
  async () => {
    const q = fromRows(
      { x: "utf8" },
      ["", "aa", "a", "\ue000", "🧪", "🧪x", "🧪a"].map((x) => ({ x })),
    ).sort([{ column: "x" }]);
    expect(await collect(q)).toEqual(
      ["", "a", "aa", "\ue000", "🧪", "🧪a", "🧪x"].map((x) => ({ x })),
    );
  },
);

tc(
  {
    id: "TC-PLAN-SOURCE-ORDERED",
    requirements: ["REQ-PLAN-002", "REQ-AGG-001"],
    category: "normal",
    objective: "Declared source ordering enables one-group aggregation",
    steps: "Source reports ordering k; rows [(a,1),(a,2),(b,4)], no cardinality estimate",
    expected: "ordered-aggregate plan produces a:3,b:4",
  },
  async () => {
    const fields = schema({ k: "utf8", v: "i32" });
    const q = Query.fromSource({
      name: "ordered",
      schema: fields,
      statistics: { ordering: [{ column: "k" }] },
      async *open(ctx) {
        yield Batch.fromValues(
          fields,
          [
            ["a", 1],
            ["a", 2],
            ["b", 4],
          ],
          ctx.memory,
        );
      },
    }).aggregate(["k"], { s: sum(col("v")) });
    expect((q.explain() as { strategy: string }).strategy).toBe("ordered-aggregate");
    expect(await collect(q)).toEqual([
      { k: "a", s: 3 },
      { k: "b", s: 4 },
    ]);
  },
);

tc(
  {
    id: "TC-PLAN-UNKNOWN-CONCAT",
    requirements: ["REQ-SET-001", "REQ-REL-001"],
    category: "normal",
    objective: "Unknown source sizes do not change concat and limit results",
    steps: "Concat CSV [1,2] and CSV[3], limit2 offset1, then count",
    expected: "Rows2,3 and count2",
  },
  async () => {
    const q = fromCsv("x\n1\n2", { x: "i32" })
      .concat(fromCsv("x\n3", { x: "i32" }))
      .limit(2, 1);
    expect(await collect(q)).toEqual([{ x: 2 }, { x: 3 }]);
    expect(await q.count()).toBe(2);
  },
);

tc(
  {
    id: "TC-SINK-FAILURE",
    requirements: ["REQ-LIFE-001", "REQ-MEM-001"],
    category: "fault",
    objective: "Sink failure releases borrowed batches and sink reservations",
    steps: "Sink start reserves16 bytes; first write throws; close releases the reservation",
    expected: "Original error propagated; close called once; query memory0",
  },
  async () => {
    let lease: import("../src/memory.ts").Lease | undefined;
    let closed = 0;
    const q = fromRows({ x: "i32" }, [{ x: 1 }]);
    await expect(
      q.pipe(
        {
          start(_, ctx) {
            lease = ctx.memory.reserve(16, "sink");
          },
          write() {
            throw new Error("sink failure");
          },
          close() {
            closed++;
            lease?.release();
          },
        },
        { onFinish: (m) => expect(m.memory.used).toBe(0) },
      ),
    ).rejects.toThrow("sink failure");
    expect(closed).toBe(1);
  },
);
