# Molniya

**Make a clear path through your data.**

Molniya is an experimental query library for Bun. Describe how a dataset should be read, filtered, grouped, and sorted; then run that same query as rows or batches.

```text
CSV or rows → filter → select → group → sort → collect or stream
```

```ts
import { readCsv, col, lit, gt, sum } from "molniya";

const report = readCsv("sales.csv", {
  region: "utf8",
  amount: "f64",
})
  .where(gt(col("amount"), lit(10)))
  .aggregate(["region"], { total: sum(col("amount")) })
  .sort([{ column: "total", descending: true }])
  .limit(10);

const rows = await report.collect({ memoryBytes: 64 * 1024 * 1024 });
```

The schema is explicit, and each operation stays in the query until you ask for results. Use `collect()` for a result array, `rows()` for individual rows, `stream()` for batches, or `pipe()` to write into your own sink. A memory budget guides execution and supported sort and group operations can use temporary files when their working state grows. The budget tracks engine allocations; it is not a cap on total process memory.

## Try it

Molniya currently targets Bun 1.3.5 or newer.

```sh
bun add molniya
```

The package is experimental and does not preserve the previous API. The current foundation covers CSV and in-memory sources, typed columns, expressions, filtering, projection, grouping, sorting, distinct rows, concatenation, and joins. Parquet, MBF, and plotting support are not part of this version. See the [migration notes](docs/migration.md) before changing an existing project.

## See the flow

`explain()` shows the selected steps and execution strategy before a query runs:

```ts
console.log(report.explain({ memoryBytes: 64 * 1024 * 1024 }));
```

For a reproducible comparison against Polars JS on 1,511,443 New York City ride records, run:

```sh
bun run benchmark
```

The script downloads and verifies the public dataset, checks that both libraries produce the same result, and records each run in `evidence/benchmark.json`. The current measurements and limits are in [the benchmark notes](docs/benchmark.md). Results depend on the machine and runtime; they are measurements, not a performance promise.

In the recorded run on Bun 1.3.5 and an AMD EPYC 9V74, Molniya took about 6.2 seconds at the median; Polars JS took about 0.13 seconds. Molniya was about 47 times slower for this CSV scan and summary. Both returned 557,096 rides and matching coordinate averages. This one workload shows that the current CSV path has substantial room to improve.

## How it is checked

Tests link each case to a written requirement. The suite uses SQLite and Polars JS as independent result checks, includes boundaries and seeded CSV cases, and injects allocation and file errors. Coverage is measured with branches and checked against a gate. [Testing notes](docs/testing.md) explain the evidence and [the traceability report](evidence/traceability.md) links requirements to the latest passing cases.

## Project notes

- [Query model and memory behavior](docs/architecture.md)
- [Benchmark method and results](docs/benchmark.md)
- [Requirements and test approach](docs/testing.md)
- [Moving from earlier Molniya versions](docs/migration.md)

MIT license.
