# Requirements and tests

The test suite starts from written requirements in `requirements.json`. Every case has a stable ID, a requirement link, an objective, exact steps, an expected result, a category, and cleanup. The runner records case status and the source digest in `evidence/tests.json`; `evidence/traceability.md` lists the passing cases for each requirement.

## Checks in the suite

- Normal, boundary, invalid-input, and state-cleanup cases for core operations.
- Truth-table cases for Boolean expressions and decisions.
- CSV chunk boundaries, quoting, null tokens, malformed rows, and seeded generated inputs.
- Allocation and file-operation fault injection, including cleanup after a failed query.
- SQLite result checks for SQL behavior and Polars JS comparisons for CSV flows and join semantics.
- Native and instrumented test runs, with identical outcomes required. Istanbul reports statement, branch, function, and line coverage; configured gates are in `coverage-gates.json`.
- Mutation checks change selected semantics and require the suite to catch each change.

Run the suite and checks with:

```sh
bun install --frozen-lockfile
bun run check
```

The Polars comparison uses the pinned `nodejs-polars` package and requires its native module to load. `bun run benchmark` is separate because it downloads a public dataset and measures elapsed time on the current machine.

## Scope of the evidence

The evidence is a repeatable project test record, not a certification claim. Source coverage does not prove that requirements are complete. The JavaScript coverage report is not machine-code MC/DC evidence. Mutation checks cover selected changes and do not replace independent review. Benchmark values can vary by runtime, operating system, and hardware.
