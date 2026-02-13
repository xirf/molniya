# MBF Offloading Guide

**MBF** (Molniya Binary Format) is an optimized columnar format for caching and reusing data. Molniya can automatically convert CSV and Parquet files to MBF for faster repeated access.

## What is MBF Offloading?

When you enable offloading, Molniya:
1. Reads your source file (CSV/Parquet)
2. Converts it to MBF format on disk
3. Returns a DataFrame that reads from the MBF file

**Why?** MBF files are faster to read than CSV (no parsing) and Parquet (no decompression).

## Quick Start

### Temporary Cache (Auto-Cleanup)

Use `offload: true` for automatic temporary caching:

```typescript
import { readCsv, readParquet } from "molniya";

// First read: CSV → MBF conversion
const users = await readCsv("users.csv", schema, {
  offload: true
});

// Use in joins
const result = await events.innerJoin(users, "user_id", "id");

// Process exits: ./cache/<hash>.mbf is deleted automatically
```

**What happens:**
- Cache file created in `./cache/<hash>.mbf` (hash based on source path + mtime + size)
- File registered for cleanup on process exit
- Second read (same process) reuses cache if source hasn't changed

### Persistent Cache (Manual Management)

Use `offloadFile` for persistent caching across runs:

```typescript
// Run 1: Creates cache
const users = await readCsv("users.csv", schema, {
  offloadFile: "./cache/users.mbf"
});

// Run 2: Reuses cache (instant)
const users = await readCsv("users.csv", schema, {
  offloadFile: "./cache/users.mbf"
});
```

**What happens:**
- Cache file created at specified path
- NOT deleted on exit - you manage the lifecycle
- Cache validated before use: if source file is newer, cache is rebuilt

---

## How Offloading Works

### Step 1: Cache Check

When you read with `offload` or `offloadFile`:

```typescript
const df = await readCsv("data.csv", schema, { offload: true });
```

Molniya checks:
1. Does cache file exist?
2. Is cache newer than source file? (compares modification times)

### Step 2: Cache Hit or Miss

**Cache Hit** (cache exists and is valid):
```
readCsv() → readMbf("./cache/<hash>.mbf") → DataFrame
```
- Fast: Reads from optimized MBF
- No CSV parsing, no Parquet decompression

**Cache Miss** (no cache or source is newer):
```
readCsv() → parse CSV → write to MBF → readMbf() → DataFrame
```
- First read is slower (conversion overhead)
- Subsequent reads are fast

### Step 3: Return DataFrame

The returned DataFrame:
- ✅ Is backed by MBF file (fast streaming)
- ✅ Works with all operators (filter, select, join, etc.)
- ✅ Read-only (modifications don't update MBF)
- ✅ Lazy (no data in memory until you call `collect()` or `toArray()`)

---

## Performance Characteristics

### CSV Offloading

```typescript
// 100MB CSV file, 1M rows
const df = await readCsv("data.csv", schema, { offload: true });

// First read: CSV parsing + MBF write
// Time: ~3s (parse) + ~200ms (write) = 3.2s
// Disk: data.csv (100MB) + ./cache/<hash>.mbf (200MB)

// Second read: MBF read only
// Time: ~100ms (10-30× faster!)
// Memory: Streaming, minimal RAM
```

**Speedup**: 10-30× on subsequent reads

### Parquet Offloading

```typescript
// 50MB Parquet file (compressed), 1M rows
const df = await readParquet("data.parquet", { offload: true });

// First read: Parquet decompress + MBF write
// Time: ~800ms (decompress) + ~200ms (write) = 1s
// Disk: data.parquet (50MB) + ./cache/<hash>.mbf (200MB)

// Second read: MBF read only
// Time: ~100ms (5-10× faster)
```

**Speedup**: 2-5× on subsequent reads

**Note**: Parquet is already efficient, so gains are smaller than CSV

---

## Common Use Cases

### Use Case 1: Reusing Build Tables in Joins

**Problem**: Joining multiple probe tables with the same small build table

```typescript
// users.csv: 10K rows, used as build table
const users = await readCsv("users.csv", schema, {
  offload: true  // Auto temp cache
});

// Join 10 different event partitions
for (let i = 0; i < 10; i++) {
  const events = await readParquet(`events_${i}.parquet`);
  const result = await events.innerJoin(users, "user_id", "id");
  await processResults(result);
}

// users.csv parsed once, read from MBF 10× (fast)
```

**Performance**:
- Without offload: Parse CSV 10× = 30s
- With offload: Parse once + read MBF 9× = 3s + 0.9s = 3.9s
- **Speedup: 7.7×**

### Use Case 2: Interactive Data Exploration

**Problem**: Running multiple queries on the same dataset

```typescript
// Persistent cache for interactive session
const data = await readCsv("sales.csv", schema, {
  offloadFile: "./cache/sales.mbf"
});

// Query 1: Group by region
const byRegion = await data
  .groupBy("region", [{ name: "total", expr: sum("amount") }])
  .collect();

// Query 2: Group by product
const byProduct = await data
  .groupBy("product", [{ name: "total", expr: sum("amount") }])
  .collect();

// Query 3: Filter and aggregate
const highValue = await data
  .filter(col("amount").gt(1000))
  .groupBy("category", [{ name: "avg", expr: avg("amount") }])
  .collect();

// All queries read from fast MBF cache
```

**Performance**:
- First run: Build cache (~3s)
- Subsequent queries: ~100-500ms each
- **Total**: Much faster than re-parsing CSV each time

### Use Case 3: Persistent Lookup Tables

**Problem**: Application needs small reference table on every run

```typescript
// products.csv: 1K rows, reference data
const products = await readCsv("products.csv", schema, {
  offloadFile: "./cache/products-lookup.mbf"
});

// First run: Creates cache (CSV → MBF)
// Every subsequent run: Instant load from MBF

// Cache only rebuilt if products.csv is updated
```

---

## When to Use Offloading

### ✅ Use Offloading When

1. **Same file read multiple times** (same process or across runs)
2. **CSV files** (biggest speedup from avoiding parsing)
3. **Join build tables** (small table on right, used repeatedly)
4. **Interactive workflows** (exploring, iterating on queries)
5. **Reference/lookup tables** (static data loaded often)

### ❌ Skip Offloading When

1. **One-time reads** (no re-use, overhead not worth it)
2. **Very small files** (<1MB - parsing is already fast)
3. **MBF source files** (already optimized)
4. **Disk space constrained** (MBF files are ~2× CSV, ~4× Parquet)
5. **Simple pipelines** (read → process → write, no re-use)

---

## API Reference

### readCsv Options

```typescript
interface CsvReadOptions {
  filter?: Expr;           // Optional filter predicate
  offload?: boolean;       // Auto temp cache (./cache/<hash>.mbf)
  offloadFile?: string;    // Persistent cache at specific path
}

const df = await readCsv(path: string, schema: SchemaSpec, options?: CsvReadOptions);
```

### readParquet Options

```typescript
interface ParquetReadOptions {
  projection?: string[];   // Column selection
  filter?: Expr;           // Optional filter predicate
  offload?: boolean;       // Auto temp cache (./cache/<hash>.mbf)
  offloadFile?: string;    // Persistent cache at specific path
}

const df = await readParquet(path: string, options?: ParquetReadOptions);
```

### Combining Options

```typescript
// Projection + filter + offload
const df = await readParquet("data.parquet", {
  projection: ["id", "name", "amount"],
  filter: col("amount").gt(100),
  offloadFile: "./cache/filtered.mbf"
});
```

**Note**: Projection and filter are applied **before** writing to MBF, so the cache contains only the filtered/projected data.

---

## Cache Management

### Auto Cleanup (Temp Mode)

```typescript
const df = await readCsv("data.csv", schema, { offload: true });

// Cache created: ./cache/<hash>.mbf
// Registered for cleanup

// On process exit (or SIGINT/SIGTERM):
// - ./cache/<hash>.mbf is deleted
// - ./cache directory remains (may contain other caches)
```

### Manual Cleanup (Persistent Mode)

```typescript
import * as fs from "node:fs/promises";

const df = await readCsv("data.csv", schema, {
  offloadFile: "./cache/data.mbf"
});

// Use the data
await processData(df);

// Manually delete when done
await fs.unlink("./cache/data.mbf");
```

### Cache Invalidation

Molniya automatically invalidates cache if source is newer:

```typescript
// Time: 10:00 AM
const df1 = await readCsv("data.csv", schema, {
  offloadFile: "./cache/data.mbf"
});
// Creates cache

// Time: 10:05 AM - Edit data.csv

// Time: 10:10 AM
const df2 = await readCsv("data.csv", schema, {
  offloadFile: "./cache/data.mbf"
});
// Detects source is newer → rebuilds cache
```

**Logic**: `cache.mtime >= source.mtime` = valid

---

## Limitations

### Current Limitations

1. **No compression** - MBF files are uncompressed
   - CSV: MBF is ~2-3× larger
   - Parquet: MBF is ~4-5× larger (Parquet is compressed)

2. **Not portable** - MBF is Molniya-specific
   - Use Parquet for sharing data with other tools

3. **No statistics** - Can't skip data based on filters
   - Parquet has min/max stats for filtering

4. **Read-only cache** - DataFrame operations don't update MBF
   - Filters/projections create new execution plan, don't modify cache

### Workarounds

**Large files + limited disk**:
```typescript
// Use projection to reduce cache size
const df = await readParquet("huge.parquet", {
  projection: ["id", "name"],  // Only 2 columns
  offloadFile: "./cache/small.mbf"
});
// Cache is much smaller than full file
```

**Sharing data**:
```typescript
// Use Parquet for final output, MBF for intermediate
const interim = await readCsv("raw.csv", schema, {
  offloadFile: "./cache/interim.mbf"  // Fast reuse
});

// Process and write to Parquet
const result = await interim.filter(...).select(...);
await writeParquet(result, "output.parquet");  // Portable
```

---

## Advanced: Manual MBF Usage

For advanced users, you can directly read/write MBF:

### Writing MBF Manually

```typescript
import { BinaryWriter } from "molniya/io/mbf/writer";

const writer = new BinaryWriter("output.mbf", df.schema);
await writer.open();

for await (const chunk of df.stream()) {
  await writer.writeChunk(chunk);
}

await writer.close();
```

### Reading MBF Manually

```typescript
import { readMbf, DataFrame } from "molniya";

const source = await readMbf("data.mbf");
const df = DataFrame.fromStream(
  source.value,
  source.value.getSchema(),
  null
);
```

**When to use manual mode**:
- Custom caching strategies
- Multi-stage pipelines with intermediate MBF files
- Integration with external tools

---

## FAQ

### Q: Does offloading use more memory?

**A**: No, DataFrames are still lazy/streaming.

```typescript
const df = await readCsv("data.csv", schema, { offload: true });
// Data is NOT in memory yet

const rows = await df.toArray();
// NOW data is loaded via collect()
```

### Q: Can I use both `offload` and `offloadFile`?

**A**: `offloadFile` takes precedence. Don't specify both.

```typescript
// ✅ GOOD: One or the other
{ offload: true }
{ offloadFile: "./cache/data.mbf" }

// ❌ BAD: Redundant (offloadFile wins)
{ offload: true, offloadFile: "./cache/data.mbf" }
```

### Q: Does cache survive server restart?

**A**:
- `offload: true` → **NO** (temp, deleted on exit)
- `offloadFile: "path"` → **YES** (persistent)

### Q: What if I delete the cache manually?

**A**: Next read rebuilds it automatically.

```typescript
const df = await readCsv("data.csv", schema, {
  offloadFile: "./cache/data.mbf"
});
// Cache exists: fast
// Cache missing: rebuild
```

### Q: Can I offload the result of a filter?

**A**: Filters are applied before caching:

```typescript
const df = await readCsv("data.csv", schema, {
  filter: col("amount").gt(100),
  offloadFile: "./cache/filtered.mbf"
});
// Cache contains ONLY rows where amount > 100
```

---

## Best Practices

### 1. Use Temp Mode for Exploratory Work

```typescript
// Quick script, temporary caching
const data = await readCsv("data.csv", schema, { offload: true });
// Auto-cleanup on exit
```

### 2. Use Persistent Mode for Production

```typescript
// Production app, stable reference data
const products = await readCsv("products.csv", schema, {
  offloadFile: "./cache/products.mbf"
});
// Persistent across deployments
```

### 3. Offload Small Build Tables

```typescript
// ✅ GOOD: Small build table (10K rows)
const users = await readCsv("users.csv", schema, { offload: true });

// ❌ BAD: Large probe table (100M rows)
const events = await readParquet("events.parquet", { offload: true });
// Huge MBF file, not worth it for single read
```

### 4. Clean Persistent Caches Periodically

```typescript
// Cron job or deployment script
const cacheDir = "./cache";
const files = await fs.readdir(cacheDir);

for (const file of files) {
  if (file.endsWith(".mbf")) {
    const stats = await fs.stat(path.join(cacheDir, file));
    const ageHours = (Date.now() - stats.mtime.getTime()) / 1000 / 60 / 60;
    
    if (ageHours > 24) {
      await fs.unlink(path.join(cacheDir, file));
    }
  }
}
```

---

## See Also

- [Reading CSV](./reading-csv.md) - CSV parsing options
- [Reading Parquet](./reading-parquet.md) - Parquet streaming
- [Join Operations](../api/joins.md) - Using cached build tables in joins
