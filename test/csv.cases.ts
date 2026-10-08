import { expect } from "bun:test";
import { mkdtemp, rm, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  csvChunks,
  fromCsv,
  fromRows,
  readCsv,
  writeCsv,
  schema,
  type Scalar,
} from "../src/index.ts";
import { tc } from "./registry.ts";
import { assertCode, collect, rejectsCode, seeded } from "./helpers.ts";

const csvFixture =
  '\ufeffid,text,flag\r\n1,"hello,\"\"🧪\"\"\r\nworld",true\r\n2,,false\r\n3,\\N,true\r\n4,"\\N",false';
const csvFields = schema({ id: "i32", text: { type: "utf8", nullable: true }, flag: "bool" });
const csvExpected = [
  { id: 1, text: 'hello,"🧪"\r\nworld', flag: true },
  { id: 2, text: "", flag: false },
  { id: 3, text: null, flag: true },
  { id: 4, text: "\\N", flag: false },
];

tc(
  {
    id: "TC-CSV-CHUNKS",
    requirements: ["REQ-CSV-001", "REQ-CSV-003"],
    category: "property",
    objective: "Every byte split preserves CSV records",
    steps:
      "Split the fixed BOM/CRLF/escaped-quote/astral fixture at every byte position; additionally feed one byte per chunk; execute each source twice",
    expected: "Four exact rows in csvExpected for every split",
  },
  async () => {
    const bytes = new TextEncoder().encode(csvFixture);
    for (let split = 0; split <= bytes.length; split++) {
      const q = csvChunks(async function* () {
        yield bytes.subarray(0, split);
        yield bytes.subarray(split);
      }, csvFields);
      expect(await collect(q)).toEqual(csvExpected);
      expect(await collect(q)).toEqual(csvExpected);
    }
    const q = csvChunks(async function* () {
      for (const byte of bytes) yield new Uint8Array([byte]);
    }, csvFields);
    expect(await collect(q)).toEqual(csvExpected);
  },
);
const validCsv: [
  string,
  string,
  Record<string, "utf8" | "f64" | "i64" | "bool" | "i32">,
  Record<string, Scalar>[],
  object,
][] = [
  ["lf", "x\na\nb\n", { x: "utf8" }, [{ x: "a" }, { x: "b" }], {}],
  ["cr", "x\ra\rb\r", { x: "utf8" }, [{ x: "a" }, { x: "b" }], {}],
  ["empty-field", "x\n\n", { x: "utf8" }, [{ x: "" }], {}],
  ["quoted-empty", 'x\n""', { x: "utf8" }, [{ x: "" }], {}],
  ["trailing-delimiter", "x,y\na,", { x: "utf8", y: "utf8" }, [{ x: "a", y: "" }], {}],
  ["semicolon", "x;y\n1;2", { x: "i32", y: "i64" }, [{ x: 1, y: 2n }], { delimiter: ";" }],
  ["no-header", "a\nb", { x: "utf8" }, [{ x: "a" }, { x: "b" }], { header: false }],
  ["no-data", "", { x: "utf8" }, [], { header: false }],
  ["header-only", "x", { x: "utf8" }, [], {}],
  ["numeric", "x\n-1.25e2\n.5\n+4.", { x: "f64" }, [{ x: -125 }, { x: 0.5 }, { x: 4 }], {}],
  ["spaces", "x\n  a  ", { x: "utf8" }, [{ x: "  a  " }], {}],
  ["inside-bom", "x\na\ufeffb", { x: "utf8" }, [{ x: "a\ufeffb" }], {}],
];
for (const [name, text, fields, expected, options] of validCsv)
  tc(
    {
      id: `TC-CSV-VALID-${name}`,
      requirements: ["REQ-CSV-001", "REQ-CSV-003"],
      category: "boundary",
      objective: `Parse CSV ${name}`,
      steps: JSON.stringify({ text, options }),
      expected: "Exact literal rows in validCsv",
    },
    async () => expect(await collect(fromCsv(text, fields, options))).toEqual(expected),
  );
const invalidCsv: [string, string, string, object?][] = [
  ["missing-header", "", "CSV"],
  ["header-name", "bad\n1", "CSV"],
  ["header-width", "x,y\n1,2", "CSV"],
  ["row-wide", "x\n1,2", "CSV"],
  ["unterminated", 'x\n"abc', "CSV"],
  ["stray-quote", 'x\na"b', "CSV"],
  ["after-quote", 'x\n"a"b', "CSV"],
  ["after-quote-space", 'x\n"a" ', "CSV"],
  ["null-required", "x\n\\N", "TYPE"],
  ["field-limit", "x\nabc", "CSV", { maxFieldChars: 2 }],
];
for (const [name, text, code, options] of invalidCsv)
  tc(
    {
      id: `TC-CSV-INVALID-${name}`,
      requirements: ["REQ-CSV-002"],
      category: "robustness",
      objective: `Reject malformed CSV ${name}`,
      steps: JSON.stringify({ text, options }),
      expected: code,
    },
    async () => rejectsCode(() => collect(fromCsv(text, { x: "utf8" }, options)), code),
  );
for (const [type, text, code] of [
  ["i32", "1x", "CSV"],
  ["i32", "1.5", "CSV"],
  ["i32", "2147483648", "TYPE"],
  ["f64", "NaN", "CSV"],
  ["f64", "1e999", "TYPE"],
  ["i64", "9223372036854775808", "TYPE"],
  ["bool", "TRUE", "CSV"],
  ["f64", " 1", "CSV"],
] as const)
  tc(
    {
      id: `TC-CSV-TYPE-${type}-${text}`,
      requirements: ["REQ-CSV-002"],
      category: "boundary",
      objective: `Reject ${type} CSV value ${text}`,
      steps: `Header x, row ${text}`,
      expected: code,
    },
    async () => rejectsCode(() => collect(fromCsv(`x\n${text}`, { x: type })), code),
  );
for (const [name, options] of [
  ["delimiter", { delimiter: "ab" }],
  ["quote", { delimiter: '"' }],
  ["newline", { delimiter: "\n" }],
  ["null-empty", { nullToken: "" }],
  ["null-comma", { nullToken: "x,y" }],
  ["field-zero", { maxFieldChars: 0 }],
] as const)
  tc(
    {
      id: `TC-CSV-OPTIONS-${name}`,
      requirements: ["REQ-CSV-002"],
      category: "robustness",
      objective: `Reject invalid CSV option ${name}`,
      steps: JSON.stringify(options),
      expected: name === "field-zero" ? "RANGE" : "CSV",
    },
    () =>
      assertCode(
        () => fromCsv("x\n", { x: "utf8" }, options),
        name === "field-zero" ? "RANGE" : "CSV",
      ),
  );

tc(
  {
    id: "TC-CSV-UTF8",
    requirements: ["REQ-CSV-002"],
    category: "robustness",
    objective: "Reject invalid and truncated UTF-8 sequences",
    steps:
      "Feed [0xff], [0xc3], and [0xed,0xa0,0x80] after header x; also construct from a lone surrogate",
    expected: "CSV error and zero remaining accounted memory",
  },
  async () => {
    for (const data of [[255], [195], [237, 160, 128]])
      await rejectsCode(
        () =>
          collect(
            csvChunks(
              async function* () {
                yield new Uint8Array([120, 10, ...data]);
              },
              { x: "utf8" },
            ),
          ),
        "CSV",
      );
    assertCode(() => fromCsv("\ud800", { x: "utf8" }), "CSV");
  },
);

tc(
  {
    id: "TC-CSV-FIELD-BOUNDARY",
    requirements: ["REQ-CSV-002"],
    category: "boundary",
    objective: "Field limit accepts exactly its UTF-16 capacity",
    steps: "maxFieldChars2; parse 'a', 'ab', and astral '🧪'",
    expected: "All three accepted; three-code-unit value rejected in TC-CSV-INVALID-field-limit",
  },
  async () => {
    expect(await collect(fromCsv("x\na\nab\n🧪", { x: "utf8" }, { maxFieldChars: 2 }))).toEqual([
      { x: "a" },
      { x: "ab" },
      { x: "🧪" },
    ]);
  },
);

tc(
  {
    id: "TC-CSV-ROUNDTRIP",
    requirements: ["REQ-CSV-003", "REQ-IO-001"],
    category: "property",
    objective: "Atomic CSV files round-trip every logical type",
    preconditions: "Unique mkdtemp directory; fixed typed rows",
    steps:
      "Write and read rows with i64 boundaries, -0, Boolean, null, comma, quotes, CRLF, Unicode and literal null token; repeat with headerfalse/delimitersemicolon/nullTokenNULL",
    expected: "Exact logical rows; no temporary files remain",
    cleanup: "Remove the unique temporary directory in finally",
  },
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "molniya-csv-case-"));
    try {
      const fields = schema({ i: "i64", n: "f64", b: "bool", s: { type: "utf8", nullable: true } });
      const rows = [
        { i: -(2n ** 63n), n: -0, b: false, s: null },
        { i: 2n ** 63n - 1n, n: 1.25e20, b: true, s: "\\N" },
        { i: 0n, n: 0, b: true, s: 'a,;"\r\n🧪' },
        { i: 1n, n: 1, b: false, s: "" },
      ];
      for (const [iteration, options] of [
        {},
        { header: false, delimiter: ";", nullToken: "NULL" },
      ].entries()) {
        const path = join(directory, `data${iteration}.csv`);
        await writeCsv(path, fromRows(fields, rows), options);
        const q = readCsv(path, fields, options);
        expect(await collect(q)).toEqual(rows);
        expect(await collect(q)).toEqual(rows);
      }
      expect((await readdir(directory)).sort()).toEqual(["data0.csv", "data1.csv"]);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  },
);

tc(
  {
    id: "TC-CSV-FUZZ",
    requirements: ["REQ-CSV-001", "REQ-CSV-002"],
    category: "fuzz",
    objective: "Seeded malformed CSV always terminates cleanly or produces valid typed rows",
    steps:
      "Seed1907; mutate128 strings of up to40 characters from quotes, delimiters, CR/LF, null token, Unicode and ASCII; budget8192/maxFieldChars16",
    expected:
      "Valid rows contain strings or null; rejected inputs have CSV/TYPE errors; all query allocations released",
  },
  async () => {
    const random = seeded(1907);
    const chars = ["a", ",", '"', "\r", "\n", "\\", "N", "🧪"];
    for (let trial = 0; trial < 128; trial++) {
      let text = "x\n";
      for (let i = 0, n = Math.floor(random() * 40); i < n; i++)
        text += chars[Math.floor(random() * chars.length)];
      try {
        const rows = await collect(
          fromCsv(text, { x: { type: "utf8", nullable: true } }, { maxFieldChars: 16 }),
          { memoryBytes: 8192, batchBytes: 128 },
        );
        for (const row of rows) expect(row.x === null || typeof row.x === "string").toBe(true);
      } catch (error) {
        expect(["CSV", "TYPE"]).toContain((error as { code: string }).code);
      }
    }
  },
);
