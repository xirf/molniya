# Query model

Molniya builds a query description first. Reading, filtering, selecting, grouping, sorting, and limiting add steps to that description. `collect()`, `rows()`, `stream()`, or `pipe()` starts execution. A query can be run more than once; each run opens a fresh source cursor.

## Data path

1. A source provides a schema and batches of values. The built-in sources read CSV files or typed rows.
2. Expressions are checked against that schema when operations are added.
3. The planner combines adjacent filters and removes scan columns that are not needed downstream.
4. Execution converts rows into typed columns, applies the plan, and sends batches to the chosen output.

The plan is visible with `query.explain(options)`. It reports the selected strategy and the reason for it. Source row counts, byte sizes, distinct counts, and ordering are optional hints; inaccurate hints can affect strategy selection, so they should only be supplied when the source can support them.

## Memory and temporary files

`memoryBytes` sets the engine's accounting budget. It tracks allocations made through the engine's memory budget, such as batches, group state, sorting runs, and CSV parsing buffers. JavaScript runtime objects, allocator overhead, operating system caches, and Polars or other native allocations are outside that accounting. It is not a process memory limit.

Sorts and group operations can write sorted runs to temporary storage and merge them as they finish. Hash joins use budgeted in-memory state and can fail with a memory error when their build side does not fit; joins do not spill. Collecting results also retains the returned rows, so `stream()` or `pipe()` is a better fit when the caller does not need the entire result at once.

Temporary stores are closed after execution, including failed queries. `ExecutionMetrics` reports scanned and output rows, peak accounted bytes, and spill runs. These values describe the engine's accounting model.

## Column and batch lifetime

Columns store values in typed buffers, with a shared schema and explicit null handling. `stream()` yields a batch that remains valid until the iterator advances. Copy rows that need to outlive the current batch. The `rows()` iterator returns detached row objects.

## Current scope

The current implementation is a foundation: CSV and in-memory sources, numeric, Boolean, and UTF-8 columns, expressions, filters, projection, aggregation, sorting, distinct rows, concatenation, and joins. It does not yet provide the earlier Parquet or MBF readers, plotting, or convenience dataframe API. See [migration notes](migration.md) for the user-facing changes.
