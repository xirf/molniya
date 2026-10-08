import { expect } from "bun:test";
import {
  Batch,
  Column,
  MemoryBudget,
  schema,
  fromRows,
  col,
  lit,
  call,
  type Scalar,
  type DataType,
  type Expr,
  type Op,
  eq,
  ne,
  lt,
  le,
  gt,
  ge,
  add,
  sub,
  mul,
  div,
  and,
  or,
  not,
  isNull,
  coalesce,
  lower,
  upper,
  length,
  contains,
} from "../src/index.ts";
import { bind } from "../src/expr.ts";
import { tc } from "./registry.ts";
import { assertCode, scalarRows, seeded } from "./helpers.ts";

const invalidSchemas = [
  {},
  { "": "f64" },
  { x: "unknown" },
  [
    { name: "a", type: "i32", nullable: false },
    { name: "a", type: "i32", nullable: false },
  ],
  [{ name: "a", type: "i32", nullable: 1 }],
];
invalidSchemas.forEach((spec, i) =>
  tc(
    {
      id: `TC-SCHEMA-${i + 1}`,
      requirements: ["REQ-SCHEMA-001"],
      category: "robustness",
      objective: `Reject invalid schema partition ${i + 1}`,
      steps: `Construct schema ${JSON.stringify(spec)}`,
      expected: "SCHEMA error before source construction",
    },
    () => assertCode(() => schema(spec as never), "SCHEMA"),
  ),
);
const invalidValues: [DataType, unknown][] = [
  ["i32", -2147483649],
  ["i32", 2147483648],
  ["i32", 0.5],
  ["i32", "1"],
  ["i64", -(2n ** 63n) - 1n],
  ["i64", 2n ** 63n],
  ["i64", 1],
  ["f64", NaN],
  ["f64", Infinity],
  ["f64", -Infinity],
  ["bool", 1],
  ["utf8", "\ud800"],
  ["utf8", 4],
  ["f64", undefined],
  ["f64", null],
];
invalidValues.forEach(([type, value], i) =>
  tc(
    {
      id: `TC-VALUE-${i + 1}`,
      requirements: ["REQ-SCHEMA-001"],
      category: "boundary",
      objective: `Reject invalid ${type} value ${String(value)}`,
      steps: `fromRows({x:'${type}'}, [{x:${String(value)}}])`,
      expected: "TYPE error; no query created",
    },
    () => assertCode(() => fromRows({ x: type }, [{ x: value } as never]), "TYPE"),
  ),
);
for (const [i, row] of [{ y: 1 }, { x: 1, y: 2 }].entries())
  tc(
    {
      id: `TC-FIELDS-${i}`,
      requirements: ["REQ-SCHEMA-001"],
      category: "robustness",
      objective: "Reject missing or extra row fields",
      steps: JSON.stringify(row),
      expected: i === 0 ? "TYPE error for missing x" : "SCHEMA error for extra field",
    },
    () => assertCode(() => fromRows({ x: "f64" }, [row as never]), i === 0 ? "TYPE" : "SCHEMA"),
  );

const valuesByType: [DataType, Scalar[]][] = [
  ["i32", [-2147483648, -1, 0, 2147483647, null]],
  ["i64", [-(2n ** 63n), 0n, 2n ** 63n - 1n, null]],
  ["f64", [-Number.MAX_VALUE, -0, 0, Number.MIN_VALUE, Number.MAX_VALUE, null]],
  ["bool", [false, true, null]],
  ["utf8", ["", "🧪", "你好", "a\0b", "repeat", "repeat", null]],
];
for (const [type, values] of valuesByType)
  for (const encoding of [undefined, "utf8", "dictionary"] as const)
    tc(
      {
        id: `TC-COLUMN-${type}-${encoding ?? "auto"}`,
        requirements: ["REQ-COLUMN-001", "REQ-OWN-001"],
        category: "boundary",
        objective: `Round-trip ${type} through ${encoding ?? "automatic"} storage`,
        steps: `Build the literal boundary vector in valuesByType.${type}, read each cell, dispose twice`,
        expected: "Exact values including signed zero; used bytes return to zero",
      },
      () => {
        const fields = schema({ x: { type, nullable: true } });
        const memory = new MemoryBudget(65536);
        const batch = Batch.fromValues(
          fields,
          values.map((x) => [x]),
          memory,
          encoding,
        );
        try {
          expect(scalarRows(batch, fields)).toEqual(values.map((x) => [x]));
          expect(batch.column(0).stats.nulls).toBe(1);
        } finally {
          batch.dispose();
          batch.dispose();
        }
        expect(memory.snapshot.used).toBe(0);
        assertCode(() => batch.get(0, 0), "DISPOSED");
      },
    );
for (const [name, values, expected] of [
  ["repeated", Array(100).fill("abcdefghij"), "dictionary"],
  ["unique", ["a", "b", "c"], "utf8"],
  ["empty", [], "utf8"],
  ["nulls", [null, null], "utf8"],
] as const)
  tc(
    {
      id: `TC-ENCODING-${name}`,
      requirements: ["REQ-COLUMN-002"],
      category: "boundary",
      objective: `Choose the smallest representation for ${name}`,
      steps: `Build automatic UTF-8 storage with the ${name} fixture`,
      expected: `Encoding is ${expected}; accounted bytes equal reported column bytes`,
    },
    () => {
      const memory = new MemoryBudget(65536);
      const field = schema({ x: { type: "utf8", nullable: true } })[0]!;
      const column = Column.build(field, values, memory);
      try {
        expect(column.stats.encoding).toBe(expected);
        expect(memory.snapshot.used).toBe(column.stats.bytes);
      } finally {
        column.release();
      }
      expect(memory.snapshot.used).toBe(0);
    },
  );

tc(
  {
    id: "TC-OWN-VIEWS",
    requirements: ["REQ-OWN-001"],
    category: "normal",
    objective: "Nested selection and projection retain shared storage",
    steps:
      "Build [(1,'a'),(2,'b'),(3,'c')]; take [2,0]; project y; dispose ancestors; read the projected view",
    expected: "Rows c,a remain readable; final dispose returns budget to zero",
  },
  () => {
    const fields = schema({ x: "i32", y: "utf8" });
    const memory = new MemoryBudget(4096);
    const batch = Batch.fromValues(
      fields,
      [
        [1, "a"],
        [2, "b"],
        [3, "c"],
      ],
      memory,
    );
    const selected = batch.take([2, 0]);
    const nested = selected.take([1, 0]);
    const projected = nested.project([1], schema({ y: "utf8" }));
    batch.dispose();
    selected.dispose();
    nested.dispose();
    expect(projected.values(0)).toEqual(["a"]);
    expect(projected.values(1)).toEqual(["c"]);
    const column = projected.column(0);
    projected.dispose();
    assertCode(() => column.get(0), "DISPOSED");
    assertCode(() => column.retain(), "DISPOSED");
    expect(memory.snapshot.used).toBe(0);
  },
);

tc(
  {
    id: "TC-OWN-RANGE",
    requirements: ["REQ-OWN-001", "REQ-SCHEMA-001"],
    category: "robustness",
    objective: "Invalid views cannot leak reservations",
    steps:
      "On one-row batch request take[-1],take[1],column[2],get(1,0),get(0,-1),column.get(1), and a wrong-width row",
    expected: "RANGE or SCHEMA errors; existing reservations unchanged",
  },
  () => {
    const memory = new MemoryBudget(4096);
    const fields = schema({ x: "i32" });
    const batch = Batch.fromValues(fields, [[1]], memory);
    const used = memory.snapshot.used;
    for (const run of [
      () => batch.take([-1]),
      () => batch.take([1]),
      () => batch.column(2),
      () => batch.get(1, 0),
      () => batch.get(0, -1),
      () => batch.column(0).get(1),
      () => batch.project([2], fields),
    ])
      assertCode(run, "RANGE");
    assertCode(() => Batch.fromValues(fields, [[1, 2]], memory), "SCHEMA");
    expect(memory.snapshot.used).toBe(used);
    batch.dispose();
    expect(memory.snapshot.used).toBe(0);
  },
);

tc(
  {
    id: "TC-MEM-BOUNDARIES",
    requirements: ["REQ-MEM-001"],
    category: "boundary",
    objective: "Budget reservations honor the exact byte boundary",
    steps: "Budget 100; reserve 0 then resize 99,100,101; shrink to 1; release twice",
    expected:
      "100 succeeds,101 fails MEMORY without changing used; peak100; final used0; released lease rejects resize",
  },
  () => {
    const memory = new MemoryBudget(100);
    const lease = memory.reserve(0, "case");
    lease.resize(99);
    lease.resize(100);
    assertCode(() => lease.resize(101), "MEMORY");
    expect(memory.snapshot.used).toBe(100);
    lease.resize(1);
    lease.release();
    lease.release();
    expect(memory.snapshot).toMatchObject({ used: 0, peak: 100, limit: 100, allocations: 2 });
    assertCode(() => lease.resize(1), "DISPOSED");
    assertCode(() => memory.shrink(1), "RANGE");
  },
);
for (const bad of [0, -1, 1.5, Infinity, Number.MAX_SAFE_INTEGER + 1])
  tc(
    {
      id: `TC-MEM-INVALID-${String(bad)}`,
      requirements: ["REQ-MEM-001"],
      category: "robustness",
      objective: `Reject invalid budget ${bad}`,
      steps: `new MemoryBudget(${bad})`,
      expected: "RANGE",
    },
    () => assertCode(() => new MemoryBudget(bad), "RANGE"),
  );

const truth: (boolean | null)[] = [true, false, null];
for (const a of truth)
  for (const b of truth)
    for (const op of ["and", "or"] as const)
      tc(
        {
          id: `TC-LOGIC-${op}-${a}-${b}`,
          requirements: ["REQ-NULL-001"],
          category: "decision",
          objective: `Evaluate ${a} ${op} ${b}`,
          steps: `Bind ${op} on nullable Boolean literals`,
          expected: `${op === "and" ? (a === false || b === false ? false : a === null || b === null ? null : true) : a === true || b === true ? true : a === null || b === null ? null : false}`,
        },
        () => {
          const expected =
            op === "and"
              ? a === false || b === false
                ? false
                : a === null || b === null
                  ? null
                  : true
              : a === true || b === true
                ? true
                : a === null || b === null
                  ? null
                  : false;
          expect(
            bind(call(op, lit(a, "bool"), lit(b, "bool")), schema({ x: "i32" })).evaluate(() => 0),
          ).toBe(expected);
        },
      );
const expressionCases: [string, Expr, Scalar][] = [
  ["add", add(lit(2), lit(3)), 5],
  ["sub", sub(lit(2), lit(3)), -1],
  ["mul", mul(lit(2), lit(3)), 6],
  ["div", div(lit(7), lit(2)), 3.5],
  ["add64", add(lit(2n), lit(3n)), 5n],
  ["sub64", sub(lit(2n), lit(3n)), -1n],
  ["mul64", mul(lit(2n), lit(3n)), 6n],
  ["div64", div(lit(-7n), lit(2n)), -3n],
  ["zero", div(lit(7), lit(0)), null],
  ["zero64", div(lit(7n), lit(0n)), null],
  ["lower", lower(lit("ÄBC")), "äbc"],
  ["upper", upper(lit("aß")), "ASS"],
  ["length", length(lit("A🧪é")), 3],
  ["contains", contains(lit("你好🧪"), lit("好")), true],
  ["not", not(lit(true)), false],
  ["not-false", not(lit(false)), true],
  ["not-null", not(lit(null, "bool")), null],
  ["null", isNull(lit(null)), true],
  ["not-null2", isNull(lit(0)), false],
  ["coalesce", coalesce(lit(null), lit(3)), 3],
  ["coalesce-value", coalesce(lit(0), lit(3)), 0],
];
for (const [name, expression, expected] of expressionCases)
  tc(
    {
      id: `TC-EXPR-${name}`,
      requirements: ["REQ-EXPR-001", "REQ-NULL-001"],
      category: "normal",
      objective: `Expression ${name} matches its declared semantics`,
      steps: "Evaluate the literal expression in expressionCases",
      expected: String(expected),
    },
    () => expect(bind(expression, schema({ x: "i32" })).evaluate(() => 0)).toBe(expected),
  );
for (const op of ["eq", "ne", "lt", "le", "gt", "ge"] as const)
  for (const [i, a, b] of [
    [0, 1, 2],
    [1, 2, 1],
    [2, 1, 1],
    [3, null, 1],
    [4, 1, null],
  ] as const)
    tc(
      {
        id: `TC-COMPARE-${op}-${i}`,
        requirements: ["REQ-EXPR-001", "REQ-NULL-001"],
        category: "boundary",
        objective: `Comparison ${op} partition ${i}`,
        steps: `${String(a)} ${op} ${String(b)}`,
        expected: "Null propagation or ordinary ordered comparison",
      },
      () => {
        const expected =
          a === null || b === null
            ? null
            : op === "eq"
              ? a === b
              : op === "ne"
                ? a !== b
                : op === "lt"
                  ? a < b
                  : op === "le"
                    ? a <= b
                    : op === "gt"
                      ? a > b
                      : a >= b;
        expect(
          bind({ eq, ne, lt, le, gt, ge }[op](lit(a), lit(b)), schema({ x: "i32" })).evaluate(
            () => 0,
          ),
        ).toBe(expected);
      },
    );
const invalidExpressions: [Expr, string][] = [
  [col("missing"), "SCHEMA"],
  [call("add", lit(1)), "PLAN"],
  [not(lit(1)), "TYPE"],
  [lower(lit(1)), "TYPE"],
  [coalesce(lit(1), lit("x")), "TYPE"],
  [call("eq", lit(1), lit("x")), "TYPE"],
  [add(lit(1n), lit(1)), "TYPE"],
  [mul(lit("a"), lit("b")), "TYPE"],
];
invalidExpressions.forEach(([expr, code], i) =>
  tc(
    {
      id: `TC-EXPR-INVALID-${i}`,
      requirements: ["REQ-EXPR-001"],
      category: "robustness",
      objective: `Reject invalid expression partition ${i}`,
      steps: "Bind the explicit invalidExpressions entry",
      expected: code,
    },
    () => assertCode(() => bind(expr, schema({ x: "i32" })), code),
  ),
);
for (const [name, expr] of [
  ["f64", mul(lit(Number.MAX_VALUE), lit(2))],
  ["i64", add(lit(2n ** 63n - 1n), lit(1n))],
] as const)
  tc(
    {
      id: `TC-OVERFLOW-${name}`,
      requirements: ["REQ-EXPR-001"],
      category: "boundary",
      objective: `Reject ${name} arithmetic overflow`,
      steps: "Evaluate max value multiplied by two or incremented by one",
      expected: "TYPE",
    },
    () => assertCode(() => bind(expr, schema({ x: "i32" })).evaluate(() => 0), "TYPE"),
  );

tc(
  {
    id: "TC-COLUMN-PROPERTY",
    requirements: ["REQ-COLUMN-001"],
    category: "property",
    objective: "Seeded string columns agree across encodings and selections",
    steps:
      "Seed 7301; 128 vectors of 0..31 strings drawn from empty, ASCII, Unicode, embedded null, and null; compare reversed views",
    expected:
      "All representations equal the independent input vector; every budget returns to zero",
  },
  () => {
    const random = seeded(7301);
    const fields = schema({ x: { type: "utf8", nullable: true } });
    const alphabet = ["", "a", "abc", "你好", "🧪", "\0", null];
    for (let trial = 0; trial < 128; trial++) {
      const rows = Array.from(
        { length: Math.floor(random() * 32) },
        () => [alphabet[Math.floor(random() * alphabet.length)]!] as Scalar[],
      );
      for (const encoding of ["utf8", "dictionary"] as const) {
        const memory = new MemoryBudget(65536);
        const batch = Batch.fromValues(fields, rows, memory, encoding);
        const selected = batch.take(rows.map((_, i) => rows.length - i - 1));
        batch.dispose();
        expect(scalarRows(selected, fields)).toEqual(rows.toReversed());
        selected.dispose();
        expect(memory.snapshot.used).toBe(0);
      }
    }
  },
);

tc(
  {
    id: "TC-COLUMN-DEFINITION-SNAPSHOT",
    requirements: ["REQ-COLUMN-001", "REQ-PLAN-001"],
    category: "regression",
    objective: "Columns and batches snapshot externally mutable definitions",
    steps:
      "Build Boolean column from mutable field; mutate field.type to i32; build a batch and mutate supplied schema name",
    expected: "Stored Boolean remains true and batch schema name remains x",
  },
  () => {
    const memory = new MemoryBudget(4096);
    const field = { name: "x", type: "bool" as DataType, nullable: false };
    const column = Column.build(field, [true], memory);
    try {
      field.type = "i32";
      expect(column.get(0)).toBe(true);
    } finally {
      column.release();
    }
    const fields = [{ name: "x", type: "i32" as DataType, nullable: false }];
    const batch = Batch.fromValues(fields, [[1]], memory);
    try {
      fields[0]!.name = "changed";
      expect(batch.row(0)).toEqual({ x: 1 });
    } finally {
      batch.dispose();
    }
    expect(memory.snapshot.used).toBe(0);
  },
);

tc(
  {
    id: "TC-DISPOSE-PROTOCOL",
    requirements: ["REQ-OWN-001"],
    category: "normal",
    objective: "Explicit resource management disposes a batch",
    steps: "Call Symbol.dispose on a live one-row batch",
    expected: "Budget returns to zero and subsequent reads fail DISPOSED",
  },
  () => {
    const memory = new MemoryBudget(4096);
    const batch = Batch.fromValues(schema({ x: "i32" }), [[1]], memory);
    batch[Symbol.dispose]();
    expect(memory.snapshot.used).toBe(0);
    assertCode(() => batch.values(0), "DISPOSED");
  },
);

tc(
  {
    id: "TC-SCHEMA-ROBUST-OBJECTS",
    requirements: ["REQ-SCHEMA-001"],
    category: "robustness",
    objective: "Malformed JavaScript schema and row objects fail with engine errors",
    steps: "Schemas null,3,[null],{x:null}; rows null,[],3",
    expected: "SCHEMA error for each malformed container",
  },
  () => {
    for (const value of [null, 3, [null], { x: null }])
      assertCode(() => schema(value as never), "SCHEMA");
    for (const value of [null, [], 3])
      assertCode(() => fromRows({ x: "i32" }, [value as never]), "SCHEMA");
  },
);

tc(
  {
    id: "TC-EXPR-UNKNOWN",
    requirements: ["REQ-EXPR-001"],
    category: "robustness",
    objective: "Unknown expression op is rejected before execution",
    steps: "Bind a manually supplied call with op='unknown' and two literals",
    expected: "PLAN error",
  },
  () => assertCode(() => bind(call("unknown" as Op, lit(1), lit(2)), schema({ x: "i32" })), "PLAN"),
);

tc(
  {
    id: "TC-EXPR-MIXED-NUMERIC",
    requirements: ["REQ-EXPR-001"],
    category: "normal",
    objective: "Compatible numeric types promote without truncating",
    steps:
      "coalesce nullable i32 null with f64 literal1.5; compare i32 column2 against f64 literal2",
    expected: "f64 result1.5 and Booleantrue",
  },
  () => {
    const fields = schema({ x: { type: "i32", nullable: true } });
    const a = bind(coalesce(col("x"), lit(1.5)), fields);
    expect(a.type).toBe("f64");
    expect(a.evaluate(() => null)).toBe(1.5);
    expect(bind(eq(col("x"), lit(2)), fields).evaluate(() => 2)).toBe(true);
  },
);
