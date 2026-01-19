# Changelog

All notable changes to this project will be documented in this file.


## [UNRELEASED]

### Added
- Documentation now uses `shiki-twoslash` for type-safe code examples with interactive tooltips.
- Added `DataFrame.fromColumns` factory method for type-safe instantiation.
- `LazyFrame` optimizations: `raw: true` for byte-level processing and `forceGc: true` for aggressive memory management.
- Documentation: New "Byte-Level Optimizations" section in Performance Guide.
- Documentation: Updated `scanCsv` docs with `LazyFrameConfig` options.

### Changed
- **Breaking**: `GroupBy.agg`, `sum`, and `mean` now return strongly typed `IDataFrame` results instead of `Record<string, unknown>[]`.
- Refactored internal `GroupBy` implementation to use `DataFrame` factory.
- Updated documentation and code comments to use objective, high-performance terminology.
- Enhanced styling of Twoslash tooltips to match the "Molniya" premium aesthetic.
- Updated benchmarks to log scan time separately.

## 0.0.1 - 2026-01-18

- Initial release of molniya.
- DataFrame and Series APIs for typed column operations and joins.
- CSV reader/writer with schema inference and streaming readers.
- Lazyframe execution with chunk caching and deferred parsing controls.
- Docs, examples, and benchmarks for the first cut.
