# Changelog

All notable changes to this project will be documented in this file.

## [0.0.2] - 2026-01-25

### 🎉 Complete Architecture Rewrite

This release represents a **major rewrite** of Molniya with significant breaking changes and improvements.

### ⚡ Breaking Changes

- **Throwing API by default**: `fromArrays()`, `filter()`, and `select()` now throw errors instead of returning `Result<T, Error>`
  - Use try/catch for error handling instead of checking `result.ok`
  - File I/O operations (`readCsv`, `scanCsv`) still return Result types
- **Type inference changed**: Schema types now show DType literals (`"string"`, `"float64"`, `"bool"`) instead of TypeScript types
- **Removed custom error classes**: Using standard `Error` instead of custom error hierarchy
- **Removed Sync API variants**: No more `fromArraysSync`, `filterSync`, `selectSync` - main functions throw by default
- **Architecture completely rewritten**: New columnar storage, buffer management, and optimization system

### ✨ New Features

#### Core DataFrame

- **Columnar storage** with efficient TypedArray-backed buffers
- **Null bitmap** support for proper null value handling
- **Dictionary encoding** for string columns
- **Schema validation** and automatic type inference
- **SIMD-optimized operations** for numeric data processing
- **Memory budgeting** and tracking system

#### Operations

- **fromArrays()**: Create DataFrames with automatic type inference (throws on error)
- **filter()**: Filter rows with optimized predicates (throws on error)
- **select()**: Project columns (throws on error)
- **groupby()**: Aggregations with count, sum, mean, min, max, first, last
- **joins()**: Inner, left, right, outer, cross joins
- **concat()**: Vertical and horizontal concatenation
- **merge()**: Advanced join operations
- **String operations**: lower, upper, strip, contains, startsWith, endsWith, replace, length
- **Row operations**: append, duplicate, dropDuplicates, unique
- **Missing data**: dropna, fillna, isna, notna
- **Type conversion**: astype for column type changes

#### LazyFrame & Optimization

- **Query plan building** with lazy evaluation
- **Predicate pushdown**: Filter at scan time for massive speedups
- **Column pruning**: Only read needed columns from CSV
- **Query optimizer**: Automatically reorders operations for best performance
- **Cache management**: Configurable query result caching
- **Streaming CSV**: Memory-efficient large file processing

#### Type System

- **unwrap()** helper for converting Result to throwing behavior
- **InferSchemaType** for schema type inference
- **Better type safety** with Record<string, unknown> instead of any

### 📚 Documentation

- **Complete rewrite** of all documentation
- **New guides**: Getting Started, Data Types, Lazy Evaluation, CSV I/O, Error Handling
- **API references**: DataFrame, LazyFrame, CSV Reading/Writing, DType, Schema, Result
- **Cookbooks**: Recipes for common tasks with DataFrames, LazyFrames, CSV I/O, data cleaning
- **Migration guides**: From Pandas, Polars, Arquero, and Danfo.js
- **Updated landing page** with new examples

### 🧪 Testing

- **Comprehensive test suite** for new architecture
- Tests for DataFrame operations, LazyFrame, CSV I/O, SIMD, groupby, joins, missing data
- Column pruning and predicate pushdown tests
- Type system and schema validation tests

### 📊 Examples & Benchmarks

- **New examples**: LazyFrame demo, groupby, SIMD operations, string operations, row operations
- **Performance benchmarks**: Column pruning, predicate pushdown, SIMD operations
- **Simple examples**: CSV reading, missing data handling

### 🔧 Internal Improvements

- Efficient buffer management with pooling
- Optimized sort algorithms for groupby operations
- Fast CSV scanner implementations
- Memory-efficient null handling
- Dictionary-based string storage

### 🗑️ Removed

- Old Series-based architecture
- Custom error class hierarchy
- Result-based API variants (Sync functions)
- Old LazyFrame chunk-cache implementation
- Outdated documentation and examples

## 0.0.1 - 2026-01-18

- Initial release of molniya.
- DataFrame and Series APIs for typed column operations and joins.
- CSV reader/writer with schema inference and streaming readers.
- Lazyframe execution with chunk caching and deferred parsing controls.
- Docs, examples, and benchmarks for the first cut.
