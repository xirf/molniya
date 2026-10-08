# Molniya

## A clear path through your data

Molniya is an experimental query library for Bun. Build a query from a CSV file or typed rows, inspect its steps, then collect rows, stream batches, or send them to a sink.

```text
read → filter → select → group → sort → result
```

```ts
import { readCsv, col, lit, gt, sum } from "molniya";

const rows = await readCsv("sales.csv", { region: "utf8", amount: "f64" })
  .where(gt(col("amount"), lit(10)))
  .aggregate(["region"], { total: sum(col("amount")) })
  .sort([{ column: "total", descending: true }])
  .limit(10)
  .collect();
```

Schemas make input types visible. Queries can be inspected with `explain()` before they run. The memory budget accounts for engine allocations, while sort and group operations can spill working data to temporary files when needed. It does not limit the whole Bun process.

Molniya currently supports CSV and in-memory inputs, expressions, filters, selected columns, aggregates, sorts, distinct rows, concatenation, and joins. The library is experimental and the earlier API is not compatible. Parquet, MBF, and plotting support are not currently included.

## Learn more

- [Query model and memory behavior](architecture.md)
- [Benchmark method and results](benchmark.md)
- [Requirements and test approach](testing.md)
- [Moving from earlier Molniya versions](migration.md)
