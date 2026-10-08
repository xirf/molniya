# Benchmark

The benchmark compares a CSV filter and global aggregate in Molniya with the equivalent lazy CSV query in Polars JS. It uses `uber-rides-data1.csv` from the Plotly datasets repository, a 58,604,196-byte file with 1,511,443 New York City ride records from 2014.

The query keeps April rides with coordinates inside a New York City bounding box, then returns the ride count and mean latitude and longitude. Both implementations read the same file, use explicit types, run in the same Bun process, and must agree on the result before timings are written.

The benchmark performs one warm-up and three measured runs per library, alternating which library runs first. It reports median elapsed time for a complete file scan and query. Molniya's peak accounted bytes and spill runs are recorded separately. Polars' native memory use is not measured, and Molniya's memory accounting is not total process memory, so those values should not be compared as equivalent memory measurements.

Run with:

```sh
bun run benchmark
```

The input is cached under `.cache/benchmark/`; its SHA-256 is checked on every run. The benchmark writes machine-readable run details to `evidence/benchmark.json`. Timing depends on the local machine and runtime, and this single workload does not describe every query or dataset.

## Latest result

The latest recorded result is in [`evidence/benchmark.json`](../evidence/benchmark.json). Molniya and Polars results are checked before timing output is saved. Review the medians, per-run times, runtime, CPU, and input digest there; no speed claim is inferred from one machine's result.

On the recorded machine (Bun 1.3.5, Polars JS 0.26.1, AMD EPYC 9V74), the latest run measured Molniya at about **6.7 seconds** and Polars JS at about **92 milliseconds**. Molniya was about **72 times slower** for this workload. Both returned 557,096 rides; mean latitude and longitude agreed within the comparison tolerance. The evidence file lists every measured run.

This measures one query on one machine. It points to CSV scanning and parsing as an area to improve; it does not predict performance for other files or queries.

## Source

- [Plotly datasets: Uber rides data](https://github.com/plotly/datasets/blob/master/uber-rides-data1.csv)
- [Dataset repository](https://github.com/plotly/datasets)
