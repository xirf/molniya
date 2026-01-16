# Mornye Roadmap

A prioritized list of features needed for pandas API parity.

## ✅ Completed (v0.0.1)

- [x] Core `DataFrame` and `Series` structures
- [x] Type system (`float64`, `int32`, `string`, `bool`)
- [x] CSV reading with type inference
- [x] `head()`, `tail()`, `shape`, `columns()`, `info()`, `describe()`
- [x] `filter()`, `where()`, `select()`
- [x] `sort()` (single column)
- [x] `groupby()` with `sum`, `mean`, `min`, `max`, `count`, `first`, `last`
- [x] Series: `map()`, `unique()`, `valueCounts()`, `slice()`

| Method               | Description                     | Status |
| -------------------- | ------------------------------- | ------ |
| `drop()`             | Remove columns or rows by index | ✅      |
| `rename()`           | Rename columns                  | ✅      |
| `dropna()`           | Drop rows with missing values   | ✅      |
| `fillna()`           | Fill missing values             | ✅      |
| `isna()` / `notna()` | Detect missing values           | ✅      |
| `copy()`             | Deep copy DataFrame             | ✅      |
| `sample()`           | Random row sampling             | ✅      |
| `assign()`           | Add/modify columns              | ✅      |
| `iloc()`             | Integer-location indexing       | ✅      |
| `loc()`              | Label-based indexing            | ✅      |


| Feature             | Description                                | Status |
| ------------------- | ------------------------------------------ | ------ |
| `scanCsv()`         | Lazy CSV loading (on-demand parsing)       | ✅      |
| `LazyFrame`         | Memory-efficient DataFrame for big CSV     | ✅      |
| Row Index           | O(1) random row access via byte offsets    | ✅      |
| LRU Chunk Cache     | Configurable memory budget (100MB default) | ✅      |
| `head()` / `tail()` | Returns DataFrame without full load        | ✅      |
| `filter()`          | Streaming filter with chunked processing   | ✅      |
| `select()`          | Column projection on lazy data             | ✅      |
| `collect()`         | Convert to full DataFrame when needed      | ✅      |


### Joining & Combining
- [ ] `merge()` - SQL-like joins (inner, left, right, outer)
- [ ] `concat()` - Concatenate DataFrames vertically/horizontally
- [ ] `join()` - Join on index
- [ ] `append()` - Append rows to DataFrame


- [ ] `dropDuplicates()` - Drop duplicate rows
- [ ] `duplicate()` - Duplicate the dataframe
- [ ] `unique()` - Get unique rows

### I/O
- [ ] `toCsv()` - Write DataFrame to CSV
- [ ] `toJson()` / `readJson()` - JSON support
- [ ] `toParquet()` / `readParquet()` - Parquet support (optional)


### Aggregation
- [ ] `median()`, `mode()`, `quantile()`
- [ ] `cumsum()`, `cummax()`, `cummin()`, `cumprod()`

### String Operations (`Series.str`)
- [ ] `lower()`, `upper()`, `strip()`
- [ ] `contains()`, `startswith()`, `endswith()`
- [ ] `split()`, `replace()`, `len()`

### DateTime Operations (`Series.dt`)
- [ ] `year`, `month`, `day`, `hour`, `minute`, `second`
- [ ] `dayofweek`, `dayofyear`
- [ ] DateTime dtype support

### Rolling/Window
- [ ] `rolling()` with `mean`, `sum`, `min`, `max`
- [ ] `shift()`, `diff()`

- [ ] `pivot_table()`, `melt()`, `stack()`, `unstack()`
- [ ] `corr()`, `cov()` - Correlation/covariance
- [ ] `rank()`, `pct_change()`
- [ ] `astype()` - Type casting
- [ ] Multi-Index support
- [ ] Lazy evaluation optimization
