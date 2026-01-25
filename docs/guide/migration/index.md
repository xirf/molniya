# Migration Guide

Switching to Molniya from another data library? Choose your current tool to see a detailed comparison.

## Quick Comparison

| Library                  | Language    | Main Use Case         | Migration Effort                            |
| ------------------------ | ----------- | --------------------- | ------------------------------------------- |
| [Pandas](#from-pandas)   | Python      | General data analysis | Medium - Similar concepts, different syntax |
| [Polars](#from-polars)   | Python/Rust | High performance      | **Easy** - Very similar API!                |
| [Arquero](#from-arquero) | JavaScript  | JS data tables        | Medium - More explicit typing               |
| [Danfo.js](#from-danfo)  | JavaScript  | Pandas-like for JS    | Medium - Add schemas, handle Results        |

## Choose Your Path

### From Pandas

**You'll recognize:** DataFrames, filtering, groupby, aggregations

**New concepts:** Explicit schemas, Result type for errors, LazyFrame optimization

**Time to migrate:** ~1-2 hours for basic operations

[**→ Read Pandas Migration Guide**](./from-pandas)

---

### From Polars

**You'll recognize:** Nearly everything! Lazy evaluation, scan_csv, type system

**New concepts:** Slightly simpler filter syntax, Result type

**Time to migrate:** ~30 minutes

[**→ Read Polars Migration Guide**](./from-polars)

---

### From Arquero

**You'll recognize:** Method chaining, table operations, CSV loading

**New concepts:** Explicit schemas everywhere, Result type, operator-based filters

**Time to migrate:** ~1-2 hours

[**→ Read Arquero Migration Guide**](./from-arquero)

---

### From Danfo.js

**You'll recognize:** DataFrame operations, CSV I/O, basic filtering

**New concepts:** Immutability, explicit schemas, Result type

**Time to migrate:** ~1-2 hours

[**→ Read Danfo.js Migration Guide**](./from-danfo)

---

## Key Molniya Concepts

No matter which library you're coming from, these are the core differences:

### 1. **Explicit Schemas**

Molniya requires you to specify types for all columns:

```typescript
const schema = {
  id: DType.Int32,
  name: DType.String,
  price: DType.Float64,
};
```

**Why?** Type safety, better performance, clearer intent.

### 2. **Result Type**

Operations that can fail return `Result<T, E>` instead of throwing:

```typescript
const result = await readCsv("data.csv", schema);

if (result.ok) {
  const df = result.data;
} else {
  console.error(result.error.message);
}
```

**Why?** Explicit error handling, no hidden control flow.

### 3. **Immutability**

DataFrames are never modified:

```typescript
const filtered = df.filter("age", ">", 25);
// df is unchanged, filtered is new DataFrame
```

**Why?** Prevents bugs, enables optimizations.

### 4. **Lazy vs Eager**

Two execution modes:

```typescript
// Eager: load immediately (< 10MB files)
const result = await readCsv("small.csv", schema);

// Lazy: optimize then execute (> 10MB files)
const result = await LazyFrame.scanCsv("huge.csv", schema)
  .filter("status", "==", "active")
  .collect();
```

**Why?** 10-100x faster for large files with filtering.

## Migration Checklist

Before you start:

- ☐ Understand your data types (prepare schemas)
- ☐ Decide eager vs lazy loading strategy
- ☐ Plan error handling approach
- ☐ Review your most common operations

After migration:

- ☐ Test with real data
- ☐ Verify type conversions
- ☐ Benchmark performance (especially large files)
- ☐ Update error handling

## Need Help?

**Stuck on a specific operation?**

- Check the [Cookbook](/cookbook/) for recipes
- Browse [API docs](/api/overview)

**Missing a feature you rely on?**

- See the [Roadmap](https://github.com/xirf/molniya/blob/main/ROADMAP.md)
- [Open an issue](https://github.com/xirf/molniya/issues)

**Found a bug?**

- [Report it](https://github.com/xirf/molniya/issues/new)
- Include: version, code sample, expected vs actual behavior
