import { afterAll, expect } from "bun:test";
import pl from "nodejs-polars";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import {
  readCsv,
  fromRows,
  fromCsv,
  col,
  lit,
  gt,
  mul,
  sum,
  avg,
  count,
  min,
  max,
  type Expr,
  type Row,
  type JoinHow,
  type ExecutionMetrics,
} from "../src/index.ts";
import { tc } from "./registry.ts";
import { collect, seeded, streamRows } from "./helpers.ts";

const evidence: Record<string, unknown>[] = [];
const scenarios = [
  { seed: 17, rows: 0, memoryBytes: 65536 },
  { seed: 17, rows: 1, memoryBytes: 65536 },
  { seed: 17, rows: 250, memoryBytes: 65536 },
  { seed: 17, rows: 250, memoryBytes: 8192 },
  { seed: 91, rows: 1000, memoryBytes: 8192 },
  { seed: 2026, rows: 2000, memoryBytes: 8192 },
];
for (const scenario of scenarios)
  tc(
    {
      id: `TC-POLARS-FLOW-${scenario.seed}-${scenario.rows}-${scenario.memoryBytes}`,
      requirements: [
        "REQ-ORACLE-001",
        "REQ-CSV-003",
        "REQ-AGG-001",
        "REQ-SORT-001",
        "REQ-PLAN-002",
      ],
      category: "differential",
      objective: "CSV to grouped top results agrees with the Polars JS lazy engine",
      preconditions:
        "Unique CSV fixture; pinned nodejs-polars 0.26.1; fixed seed; explicit schemas and null policies",
      steps: `Seed ${scenario.seed}; ${scenario.rows} rows; budget ${scenario.memoryBytes}. Scan CSV, filter amount>10, project region and amount*1.25, group sum/mean/count/min/max, sort total descending then region, limit4`,
      expected:
        "Identical ordered result rows; small-budget nonempty workloads spill; accounted memory returns to zero",
      cleanup: "Close engine resources and remove the unique fixture directory in finally",
    },
    async () => {
      const directory = await mkdtemp(join(tmpdir(), "molniya-polars-"));
      try {
        const random = seeded(scenario.seed);
        const regions = ["north", "south", 'quoted"name', "🧪,lab", "line\nbreak", null];
        const rows = Array.from({ length: scenario.rows }, (_, id) => ({
          id,
          region: regions[Math.floor(random() * regions.length)]!,
          amount: random() < 0.2 ? null : Math.floor(random() * 80) - 20,
        }));
        const encode = (value: unknown) =>
          value === null
            ? "\\N"
            : typeof value === "string"
              ? `"${value.replaceAll('"', '""')}"`
              : String(value);
        const csv =
          "id,region,amount\n" +
          rows.map((row) => [row.id, row.region, row.amount].map(encode).join(",")).join("\n");
        const path = join(directory, "sales.csv");
        await Bun.write(path, csv);
        const query = readCsv(path, {
          id: "i32",
          region: { type: "utf8", nullable: true },
          amount: { type: "f64", nullable: true },
        })
          .where(gt(col("amount"), lit(10)))
          .select({ region: col("region"), net: mul(col("amount"), lit(1.25)) })
          .aggregate(["region"], {
            total: sum(col("net")),
            mean: avg(col("net")),
            n: count(col("net")),
            low: min(col("net")),
            high: max(col("net")),
          })
          .sort([{ column: "total", descending: true }, { column: "region" }])
          .limit(4);
        const oracle = pl
          .scanCSV(path, {
            schema: { id: pl.Int32, region: pl.String, amount: pl.Float64 },
            nullValues: "\\N",
            missingUtf8IsEmptyString: true,
          })
          .filter(pl.col("amount").gt(10))
          .select(pl.col("region"), pl.col("amount").mul(1.25).alias("net"))
          .groupBy("region")
          .agg(
            pl.col("net").sum().alias("total"),
            pl.col("net").mean().alias("mean"),
            pl.col("net").count().alias("n"),
            pl.col("net").min().alias("low"),
            pl.col("net").max().alias("high"),
          )
          .sort({ by: ["total", "region"], descending: [true, false], nullsLast: true })
          .limit(4);
        const expected = oracle.collectSync().toRecords() as Row[];
        let metrics: ExecutionMetrics | undefined;
        const actual = await collect(query, {
          memoryBytes: scenario.memoryBytes,
          batchBytes: 128,
          onFinish: (value) => {
            metrics = value;
          },
        });
        expect(actual).toEqual(expected);
        if (scenario.rows >= 250 && scenario.memoryBytes === 8192)
          expect(metrics!.spillRuns).toBeGreaterThan(0);
        evidence.push({
          case: `flow-${scenario.seed}-${scenario.rows}-${scenario.memoryBytes}`,
          ...scenario,
          inputSha256: new Bun.CryptoHasher("sha256").update(csv).digest("hex"),
          equivalent: true,
          actual,
          polars: expected,
          metrics,
          molniyaPlan: query.explain({ memoryBytes: scenario.memoryBytes, batchBytes: 128 }),
          polarsPlan: oracle.describeOptimizedPlan(),
        });
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    },
  );

for (const how of ["inner", "left", "full", "semi", "anti", "cross"] as JoinHow[])
  tc(
    {
      id: `TC-POLARS-JOIN-${how}`,
      requirements: ["REQ-ORACLE-001", "REQ-JOIN-001"],
      category: "differential",
      objective: `${how} join agrees with Polars JS duplicate and null behavior`,
      steps:
        "Left keys [1,1,null,2], right keys [1,1,null,3]; join on equality with nullsEqualfalse; compare source row identifiers in canonical order",
      expected:
        "Exact ordered row identifier pairs; null keys never match except in an unconditional cross join",
    },
    async () => {
      const leftKeys = [1, 1, null, 2];
      const rightKeys = [1, 1, null, 3];
      const left = fromRows(
        { id: { type: "i32", nullable: true }, lrow: "i32" },
        leftKeys.map((id, lrow) => ({ id, lrow })),
      );
      const right = fromRows(
        { rid: { type: "i32", nullable: true }, rrow: "i32" },
        rightKeys.map((rid, rrow) => ({ rid, rrow })),
      );
      const onlyLeft = how === "semi" || how === "anti";
      const projection: Record<string, Expr> = onlyLeft
        ? { lrow: col("lrow") }
        : { lrow: col("lrow"), rrow: col("rrow") };
      const query = left
        .join(right, { how, on: how === "cross" ? [] : [["id", "rid"]] })
        .select(projection)
        .sort(onlyLeft ? [{ column: "lrow" }] : [{ column: "lrow" }, { column: "rrow" }]);
      const a = pl.DataFrame(
        { id: leftKeys, lrow: [0, 1, 2, 3] },
        { schema: { id: pl.Int32, lrow: pl.Int32 } },
      );
      const b = pl.DataFrame(
        { rid: rightKeys, rrow: [0, 1, 2, 3] },
        { schema: { rid: pl.Int32, rrow: pl.Int32 } },
      );
      const oracle =
        how === "cross"
          ? a.join(b, { how })
          : a.join(b, { how, leftOn: "id", rightOn: "rid", nullsEqual: false });
      const names = onlyLeft ? ["lrow"] : ["lrow", "rrow"];
      const expected = oracle
        .select(...names)
        .sort({ by: names, nullsLast: true })
        .toRecords() as Row[];
      const actual = await collect(query);
      expect(actual).toEqual(expected);
      evidence.push({ case: `join-${how}`, equivalent: true, actual, polars: expected });
    },
  );

tc(
  {
    id: "TC-POLARS-NULL-SUM-CONTRACT",
    requirements: ["REQ-ORACLE-001", "REQ-AGG-001"],
    category: "differential",
    objective: "Record the deliberate SQL-compatible all-null sum difference",
    steps: "Sum two nullable Float64 nulls in Molniya and Polars JS",
    expected:
      "Molniya sum null per REQ-AGG-001; Polars sum0; difference is explicit rather than normalized away",
  },
  async () => {
    const actual = await collect(
      fromRows({ x: { type: "f64", nullable: true } }, [{ x: null }, { x: null }]).aggregate([], {
        s: sum(col("x")),
      }),
    );
    const oracle = pl
      .DataFrame({ x: [null, null] }, { schema: { x: pl.Float64 } })
      .select(pl.col("x").sum().alias("s"))
      .toRecords();
    expect(actual).toEqual([{ s: null }]);
    expect(oracle).toEqual([{ s: 0 }]);
    evidence.push({
      case: "all-null-sum",
      equivalent: false,
      expectedDifference: "REQ-AGG-001 uses SQL empty/all-null sum semantics",
      actual,
      polars: oracle,
    });
  },
);

afterAll(async () => {
  const output = process.env.MOLNIYA_EVIDENCE_DIR ?? resolve(import.meta.dir, "../evidence");
  await mkdir(output, { recursive: true });
  await Bun.write(
    join(output, "polars.json"),
    JSON.stringify(
      {
        oracle: "nodejs-polars",
        version: pl.version,
        runtime: `Bun ${Bun.version}`,
        cases: evidence,
      },
      null,
      2,
    ) + "\n",
  );
});
