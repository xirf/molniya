import { createReadStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import { cpus } from "node:os";
import { join, resolve } from "node:path";
import pl from "nodejs-polars";
import { and, avg, col, contains, count, ge, le, lit, readCsv } from "../src/index.ts";
import { fingerprint } from "./provenance.ts";

const root = resolve(import.meta.dir, "..");
const cache = resolve(root, ".cache/benchmark");
const path = join(cache, "uber-rides-data1.csv");
const url = "https://raw.githubusercontent.com/plotly/datasets/master/uber-rides-data1.csv";
const sha256 = "d09620932af5dbaf94ba3c61fb7ed0985d624eff6736c7f1583c4d0f8f899890";
const expectedBytes = 58_604_196;
const expectedRows = 1_511_443;
const memoryBytes = 64 * 1024 * 1024;
const schema = { "Date/Time": "utf8", Lat: "f64", Lon: "f64" } as const;

await mkdir(cache, { recursive: true });
const cached = await Bun.file(path).exists();
if (!cached) {
  const response = await fetch(url);
  if (!response.ok)
    throw new Error(`Dataset download failed: ${response.status} ${response.statusText}`);
  await Bun.write(path, response);
}

const hash = new Bun.CryptoHasher("sha256");
let inputBytes = 0;
let lineBreaks = 0;
for await (const chunk of createReadStream(path)) {
  inputBytes += chunk.byteLength;
  for (const byte of chunk) if (byte === 10) lineBreaks++;
  hash.update(chunk);
}
const inputSha256 = hash.digest("hex");
if (inputBytes !== expectedBytes || inputSha256 !== sha256 || lineBreaks - 1 !== expectedRows) {
  throw new Error(
    `Dataset did not match the pinned file: ${inputBytes} bytes, ${lineBreaks - 1} rows, SHA-256 ${inputSha256}`,
  );
}

const query = () =>
  readCsv(path, schema)
    .where(
      and(
        contains(col("Date/Time"), lit("2014-04")),
        and(
          and(ge(col("Lat"), lit(40.6)), le(col("Lat"), lit(40.9))),
          and(ge(col("Lon"), lit(-74.1)), le(col("Lon"), lit(-73.7))),
        ),
      ),
    )
    .aggregate([], { rides: count(), meanLat: avg(col("Lat")), meanLon: avg(col("Lon")) });

const oracle = () =>
  pl
    .scanCSV(path, { schema: { "Date/Time": pl.String, Lat: pl.Float64, Lon: pl.Float64 } })
    .filter(
      pl
        .col("Date/Time")
        .str.contains("2014-04", true)
        .and(pl.col("Lat").gtEq(40.6))
        .and(pl.col("Lat").ltEq(40.9))
        .and(pl.col("Lon").gtEq(-74.1))
        .and(pl.col("Lon").ltEq(-73.7)),
    )
    .select(
      pl.col("Lat").count().alias("rides"),
      pl.col("Lat").mean().alias("meanLat"),
      pl.col("Lon").mean().alias("meanLon"),
    );

const clean = (rows: Record<string, unknown>[]) =>
  rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        typeof value === "bigint" ? value.toString() : value,
      ]),
    ),
  );
function compare(actual: Record<string, unknown>[], expected: Record<string, unknown>[]): void {
  if (actual.length !== 1 || expected.length !== 1)
    throw new Error("Expected one aggregate row from each engine");
  const left = actual[0]!;
  const right = expected[0]!;
  if (Number(left.rides) !== Number(right.rides))
    throw new Error(`Ride counts differ: ${left.rides} != ${right.rides}`);
  for (const field of ["meanLat", "meanLon"]) {
    const tolerance = Math.max(1, Math.abs(Number(right[field]))) * 1e-10;
    if (Math.abs(Number(left[field]) - Number(right[field])) > tolerance) {
      throw new Error(`${field} differs: ${left[field]} != ${right[field]}`);
    }
  }
}

const molniyaRun = async () => {
  let metrics;
  const start = performance.now();
  const result = await query().collect({
    memoryBytes,
    onFinish: (value) => {
      metrics = value;
    },
  });
  return {
    elapsedMs: performance.now() - start,
    result: result as Record<string, unknown>[],
    metrics,
  };
};
const polarsRun = () => {
  const start = performance.now();
  const result = oracle().collectSync().toRecords() as Record<string, unknown>[];
  return { elapsedMs: performance.now() - start, result };
};

const warmMolniya = await molniyaRun();
const warmPolars = polarsRun();
compare(warmMolniya.result, warmPolars.result);
const molniyaRuns = [];
const polarsRuns = [];
for (let iteration = 0; iteration < 3; iteration++) {
  const first = iteration % 2 === 0 ? "molniya" : "polars";
  const molniya = first === "molniya" ? await molniyaRun() : undefined;
  const polars = first === "polars" ? polarsRun() : undefined;
  const secondMolniya = molniya ?? (await molniyaRun());
  const secondPolars = polars ?? polarsRun();
  compare(secondMolniya.result, secondPolars.result);
  molniyaRuns.push({ elapsedMs: secondMolniya.elapsedMs, metrics: secondMolniya.metrics });
  polarsRuns.push({ elapsedMs: secondPolars.elapsedMs });
}

const median = (values: number[]) =>
  values.slice().sort((a, b) => a - b)[Math.floor(values.length / 2)]!;
const molniyaMedianMs = median(molniyaRuns.map((run) => run.elapsedMs));
const polarsMedianMs = median(polarsRuns.map((run) => run.elapsedMs));
const report = {
  sourceDigest: await fingerprint(root),
  dataset: {
    name: "New York City Uber rides, April to September 2014",
    source: "Plotly datasets repository",
    url,
    rows: expectedRows,
    bytes: inputBytes,
    sha256: inputSha256,
  },
  query:
    "Filter April 2014 rides within the NYC coordinate bounds; report ride count and mean latitude and longitude.",
  procedure:
    "One warm-up per engine and three measured runs per engine, alternating execution order. Each timing includes CSV scan, parsing, and query. Both outputs are compared before the report is written.",
  memoryBytes,
  runtime: `Bun ${Bun.version}`,
  polars: pl.version,
  cpu: cpus()[0]?.model,
  platform: `${process.platform}/${process.arch}`,
  results: {
    equivalent: true,
    comparison:
      "Ride counts match exactly; mean latitude and longitude are compared with relative tolerance 1e-10.",
    molniya: { medianMs: molniyaMedianMs, runs: molniyaRuns, output: clean(warmMolniya.result) },
    polars: { medianMs: polarsMedianMs, runs: polarsRuns, output: clean(warmPolars.result) },
    molniyaToPolarsMedianRatio: molniyaMedianMs / polarsMedianMs,
  },
  memoryNote:
    "Molniya peak bytes are engine-accounted allocations. They do not include all JavaScript, native, runtime, allocator, or operating system memory and are not directly comparable with Polars process memory.",
};
const evidence = resolve(root, "evidence");
await mkdir(evidence, { recursive: true });
await Bun.write(join(evidence, "benchmark.json"), JSON.stringify(report, null, 2) + "\n");
console.log(
  `Molniya ${molniyaMedianMs.toFixed(2)} ms; Polars JS ${polarsMedianMs.toFixed(2)} ms; ratio ${report.results.molniyaToPolarsMedianRatio.toFixed(2)}x; verified aggregate for ${expectedRows.toLocaleString()} rides.`,
);
