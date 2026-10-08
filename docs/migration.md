# Moving to the current Molniya foundation

The current version changes the public API and is not a drop-in update for earlier releases. The previous dataframe methods, Parquet and MBF readers, plotting helpers, and older expression builders are not included. Keep an earlier release pinned if a project depends on those features.

The current flow starts with `readCsv(path, schema)` or `fromRows(schema, rows)`, then adds immutable query steps. For example:

```ts
const result = await readCsv("sales.csv", { region: "utf8", amount: "f64" })
  .where(gt(col("amount"), lit(10)))
  .aggregate(["region"], { total: sum(col("amount")) })
  .collect();
```

Use `collect()` when an array of all results is appropriate. Use `rows()`, `stream()`, or `pipe()` when the caller can consume results incrementally. Provide the complete input schema explicitly; automatic type inference is not part of this API.

The project currently targets Bun 1.3.5 or newer. Review the [query model](architecture.md) for batch lifetimes, memory accounting, and operator limits before moving workloads that rely on spilling or process-wide memory caps.
