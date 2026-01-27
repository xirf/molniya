# Molniya Roadmap

## 🎯 v0.1.0 Focus: Fast, Memory-Efficient CSV Parsing

**Target Runtime:** Bun (only), no Node.js/Deno/browser support in v0.1.0

### Current Problem
- `scanCsv()` currently loads entire file into memory (`await file.text()`)
- 9GB CSV → 9GB+ RAM spike immediately
- Custom byte parsing may be slower than native JS methods

### Adaptive scanCsv Strategy

**Goal:** Auto-detect file size and use optimal parsing strategy

#### Small Files (<100MB)
- **Load fully** into memory via `file.text()`
- **Parse fast** using native JS (`split()`, `Number()`)
- **Trade-off:** Higher memory, but instant results for common case

#### Large Files (≥100MB)  
- **True streaming** via `Bun.file().stream()` (ReadableStream)
- **Incremental parsing** without loading entire file
- **Predictable memory:** Process chunks, write to DataFrame, free buffers
- **Backpressure-aware** to avoid memory bloat

### Performance Targets (v0.1.0)

- [ ] **Small files (10-100MB):** <500ms parse time
- [ ] **Medium files (100MB-1GB):** <2GB peak RSS, streaming mode under 10s
- [ ] **Large files (1-10GB):** <2GB peak RSS, predictable memory growth under 2minutes

### Priority Order

1. **Fix Memory Issue** - Implement true streaming for large files
2. **Benchmark Parsing** - Native JS vs byte-level custom parsers
3. **SIMD Optimization** - Vectorize numeric conversions where possible
4. **String Interning** - Optimize dictionary encoding timing

### Key Architectural Principles

#### 1. Columnar Storage (TypedArrays Only)
- All data stored as `Float64Array`, `Int32Array`, etc.
- Zero row objects to avoid GC pressure
- SIMD-friendly memory layout

#### 2. Async-Only API
- All operations return `Promise<DataFrame>`
- No blocking, even for small datasets
- Prevents UI/server freezes

#### 3. No WASM (For Now)
- Pure TypeScript/JavaScript implementation
- Explore limits of native JS performance first
- Revisit if hitting insurmountable walls

---

## Core Features

### Data Structures

- [x] Core `DataFrame` structure with typed columns ✅
- [x] `Series` class for single column operations ✅
- [x] Type system (`float64`, `int32`, `string`, `bool`, `datetime`, `date`) ✅

### CSV I/O

- [x] CSV reading with type inference (`readCsv`, `readCsvFromString`) ✅
- [x] Lazy CSV scanning (`scanCsv`, `scanCsvFromString`) - **Primary path; optimize for speed/memory** ⚠️
- [ ] CSV writing (`writeCsv`, `toCsv`) ❌
- [ ] JSON support (`toJson` / `readJson`) ❌
- [ ] Parquet support (after CSV streaming is stable) ❌

### DataFrame Operations

| Method               | Description                     | Status |
| -------------------- | ------------------------------- | ------ |
| `filter()`           | Filter rows by predicate        | ✅     |
| `select()`           | Select specific columns         | ✅     |
| `drop()`             | Remove columns or rows by index | ✅     |
| `rename()`           | Rename columns                  | ✅     |
| `dropna()`           | Drop rows with missing values   | ✅     |
| `fillna()`           | Fill missing values             | ✅     |
| `isna()` / `notna()` | Detect missing values           | ✅     |
| `astype()`           | Convert column types            | ✅     |
| `head()` / `tail()`  | Get first/last N rows           | ✅     |
| `copy()`             | Deep copy DataFrame             | ❌     |
| `sample()`           | Random row sampling             | ❌     |
| `iloc()`             | Integer-location indexing       | ❌     |
| `loc()`              | Label-based indexing            | ❌     |

### Aggregation Functions

| Function/Method   | DataFrame API | Series API | Status |
| ----------------- | ------------- | ---------- | ------ |
| `sum()`           | ✅            | ✅         | ✅     |
| `mean()`          | ✅            | ✅         | ✅     |
| `min()`           | ✅            | ✅         | ✅     |
| `max()`           | ✅            | ✅         | ✅     |
| `count()`         | ✅            | ✅         | ✅     |
| `unique()`        | ❌            | ✅         | ⚠️     |
| `median()`        | ✅            | ✅         | ✅     |
| `mode()`          | ✅            | ✅         | ✅     |
| `quantile()`      | ❌            | ❌         | ❌     |
| `std()` / `var()` | ❌            | ❌         | ❌     |
| `cumsum()`        | ❌            | ✅         | ⚠️     |
| `cummax()`        | ❌            | ✅         | ⚠️     |
| `cummin()`        | ❌            | ✅         | ⚠️     |

### GroupBy Operations

- [x] Single and multi-column grouping ✅
- [x] Aggregation functions: `count`, `sum`, `mean`, `min`, `max`, `first`, `last` ✅
- [ ] Multiple aggregations per column ❌
- [ ] Custom aggregation functions ❌

## LazyFrame & Query Optimization

| Feature             | Description                          | Status |
| ------------------- | ------------------------------------ | ------ |
| `scanCsv()`         | Adaptive CSV loading (size-aware)    | ⚠️     |
| `LazyFrame`         | Deferred execution for large data    | ⚠️     |
| `collect()`         | Execute and materialize DataFrame    | ✅     |
| Column Pruning      | Skip reading unused columns          | ✅     |
| Predicate Pushdown  | Filter during CSV parsing            | ✅     |

### Joining & Combining

| Operation          | Description                                | Status |
| ------------------ | ------------------------------------------ | ------ |
| `merge()`          | SQL-like joins (inner, left, right, outer) | ✅     |
| `concat()`         | Concatenate DataFrames vertically/horiz.   | ✅     |
| `join()`           | Join on index                              | ✅     |
| `append()`         | Append rows to DataFrame                   | ✅     |
| `dropDuplicates()` | Drop duplicate rows                        | ✅     |
| `duplicate()`      | Duplicate the dataframe                    | ✅     |
| `unique()`         | Get unique rows                            | ✅     |

### Sorting & Ordering

- [x] Single column sort ✅
- [x] Multi-column sort ✅
- [ ] Stable sort guarantee ❌
- [ ] Index-based sorting ❌

### String Operations (`Series.str`)

| Method          | Description                  | Status |
| --------------- | ---------------------------- | ------ |
| `toLowerCase()` | Convert strings to lowercase | ✅     |
| `toUpperCase()` | Convert strings to uppercase | ✅     |
| `contains()`    | Check if contains substring  | ✅     |
| `startsWith()`  | Check if starts with prefix  | ✅     |
| `endsWith()`    | Check if ends with suffix    | ✅     |
| `length()`      | Get string lengths           | ✅     |
| `split()`       | Split strings into arrays    | ❌     |
| `trim()`        | Remove whitespace            | ❌     |
| `replace()`     | Replace substring            | ❌     |

### DateTime Operations (`Series.dt`)

- [ ] `year`, `month`, `day`, `hour`, `minute`, `second` ❌
- [ ] `dayofweek`, `dayofyear` ❌
- [ ] DateTime parsing and formatting ❌

## Performance & Optimization

### Current Status
- [x] TypedArray-based columnar storage ✅
- [x] String dictionary encoding ✅
- [x] Column pruning during CSV scan ✅
- [x] Predicate pushdown during CSV scan ✅
- [x] Basic SIMD operations (sum, min, max, filters) ✅
- [ ] Adaptive file size detection ❌
- [ ] True streaming for large files ❌
- [ ] Native JS parsing benchmarks ❌

### Code Quality Standards
- **Line limit**: 500 lines per file (hard cap: 600)
- **Type safety**: Strong typing with TypeScript
- **Zero-copy**: Minimize allocations in hot paths
- **Performance-first**: Benchmark before optimizing

## Future Considerations (v0.2.0+)

### Binary Format & Caching
- Custom `.mbin` format for faster re-loads
- Memory-mapped file access
- Background cache writing during first parse

### Advanced Operations
- Streaming joins (hash join with memory budget)
- External merge sort for large datasets
- Window functions
- Worker thread parallelization

### Optimizations
- String dictionary compression
- Column statistics for query planning
- Predicate pushdown improvements
