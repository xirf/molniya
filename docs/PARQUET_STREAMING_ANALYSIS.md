# Parquet Streaming Architecture: Analysis & Proposals

## Context

Currently, Parquet reading materializes the entire file into memory. This raises the question: should we convert Parquet to a streaming format (.mbf) for better streaming characteristics, or implement native Parquet streaming?

## Problem Statement

**Current Implementation** (src/io/parquet/):
```
Parquet File → Decompress All → Parse All → Columnar Chunks → DataFrame
```

**Characteristics**:
- ✅ Direct to columnar format
- ✅ No intermediate storage
- ❌ Must load entire file into memory
- ❌ No streaming for large files
- ❌ High initial memory spike
- ❌ Cannot start processing until fully loaded

**Key Observation from convert.ts**:
The anti-pattern of `Array.from(data)` for Date conversion shows we're already paying allocation costs. The question is: where should we pay them?

---

## Proposal 1: Parquet → .mbf Conversion

### Architecture

```
┌─────────────┐
│ Parquet     │
│ File        │
└──────┬──────┘
       │ (One-time cost)
       ↓
┌─────────────────────────────┐
│ Decompress & Convert        │
│ - Row group by row group    │
│ - Write to .mbf             │
│ - With index/metadata       │
└──────┬──────────────────────┘
       │
       ↓
┌─────────────┐
│ .mbf File   │  ← Cached on disk
│ (Streaming) │
└──────┬──────┘
       │ (Fast subsequent reads)
       ↓
┌─────────────────────────────┐
│ Stream chunks               │
│ - Like CSV streaming        │
│ - Low memory usage          │
│ - Random access via index   │
└─────────────────────────────┘
```

### What is .mbf Format?

Based on the codebase artifacts (build.mbf, probe.mbf), .mbf appears to be a custom binary columnar format. Requirements for streaming:

1. **Header**: Metadata, schema, chunk index
2. **Chunks**: Self-contained columnar blocks
3. **Index**: Byte offsets for random access
4. **Dictionary**: Global string dictionary (optional per-chunk)
5. **Compression**: Optional (zstd/snappy/none)

### Advantages

#### 1. **Amortized Cost for Repeated Reads**
```
First read:  Parquet (slow) → .mbf (one-time cost)
Read 2-N:    .mbf (fast) → streaming chunks
```

If you query the same dataset 10 times:
- **Current**: Decompress Parquet 10 times
- **Proposed**: Decompress once, stream 10 times

#### 2. **Streaming Memory Profile**
```
Current:  ████████████████ (entire file in memory)
Proposed: ██░░░░░░░░░░░░░░ (only active chunks)
```

For a 10GB Parquet file:
- Current: ~10GB RAM
- Proposed: ~64MB RAM (chunk size)

#### 3. **Incremental Processing**
Can start processing first chunk while converting remaining chunks:
```
┌───────────┐
│ Convert   │ ████░░░░░░░░  (converting chunk 3)
└───────────┘
┌───────────┐
│ Process   │ ████████░░░░  (processing chunk 8)
└───────────┘
```

#### 4. **Query Optimization**
.mbf can include:
- **Column statistics** (min/max/null count) → skip chunks
- **Bloom filters** → skip non-matching chunks
- **Zone maps** → prune chunks for range queries

#### 5. **Network Efficiency**
For remote files (S3, HTTP):
```
Parquet:  Download entire file → process
.mbf:     Download header + needed chunks only
```

With index, can fetch only required chunks via HTTP range requests.

### Disadvantages

#### 1. **Storage Overhead**
```
Original:  data.parquet (1 GB, compressed)
Cache:     data.mbf      (1.2 GB, less compressed)
Total:     2.2 GB disk usage
```

- Requires 1-2x original file size
- Disk I/O for writing .mbf
- Cache invalidation strategy needed

#### 2. **Cold Start Penalty**
First read is slower:
```
Direct Parquet:      ████████ (8 seconds)
Parquet → .mbf:      ████████████ (12 seconds)
.mbf read:           ██ (2 seconds)

Break-even: 3 reads
```

#### 3. **Compression Trade-off**
Parquet uses sophisticated compression:
- Dictionary encoding
- RLE (Run-Length Encoding)
- Bit-packing
- Snappy/Gzip/Zstd

.mbf would need similar complexity or accept larger size.

#### 4. **Maintenance Burden**
- Need to define .mbf format specification
- Version compatibility
- Cache invalidation when source changes
- Disk space management

#### 5. **Write-Heavy Workloads**
If data changes frequently:
```
Update cycle: Parquet updated → .mbf cache invalid → reconvert
```

Conversion overhead on every update.

---

## Proposal 2: Native Parquet Streaming

### Architecture

Parquet files are already organized for streaming via **row groups**:

```
Parquet File Structure:
┌────────────────────────────┐
│ Magic: PAR1               │
├────────────────────────────┤
│ Row Group 0 (64MB)        │  ← Stream this
│  - Column Chunk 0         │
│  - Column Chunk 1         │
│  - Column Chunk N         │
├────────────────────────────┤
│ Row Group 1 (64MB)        │  ← Then this
├────────────────────────────┤
│ Row Group 2 (64MB)        │  ← Then this
├────────────────────────────┤
│ Footer Metadata           │
└────────────────────────────┘
```

### Implementation Strategy

#### Phase 1: Row Group Streaming

```typescript
class ParquetStreamSource implements AsyncIterable<Chunk> {
  async *[Symbol.asyncIterator]() {
    const metadata = await this.readFooter();

    for (const rowGroup of metadata.row_groups) {
      // Decompress & parse one row group at a time
      const chunk = await this.readRowGroup(rowGroup);
      yield chunk;

      // Previous row group can be GC'd
    }
  }
}

// Usage (like CSV streaming)
const df = await DataFrame.fromParquetStream("large.parquet");
await df.filter(...).select(...).collect();  // Streams row groups
```

#### Phase 2: Page-Level Streaming (Advanced)

Parquet row groups contain **pages** (~1MB each):

```
Row Group
├─ Column Chunk (age)
│  ├─ Dictionary Page
│  ├─ Data Page 0 (1MB)
│  ├─ Data Page 1 (1MB)
│  └─ Data Page N
└─ Column Chunk (name)
   └─ Data Pages...
```

Can stream at page granularity for finer control.

### Advantages

#### 1. **No Intermediate Storage**
```
Parquet → Stream → Process
```
No .mbf files, no cache management, no disk overhead.

#### 2. **Native Format Support**
Parquet is widely adopted:
- Standard interchange format
- Tooling compatibility (Spark, DuckDB, Pandas)
- Compression optimizations

#### 3. **Predicate Pushdown via Statistics**

Parquet footer contains statistics per row group:
```typescript
interface RowGroupStats {
  min_values: Map<string, any>;
  max_values: Map<string, any>;
  null_counts: Map<string, bigint>;
}

// Skip row groups that can't match filter
if (filter: age > 50 && rowGroup.stats.age.max < 50) {
  skip_row_group();  // Don't even decompress
}
```

This is **native Parquet optimization** - no custom format needed.

#### 4. **Incremental Decompression**
Decompress one row group at a time:
```
Memory usage: max(row_group_size) instead of sum(all_row_groups)
```

For 1GB file with 16 row groups:
- Current: 1GB in memory
- Streaming: 64MB in memory

### Disadvantages

#### 1. **Decompression Cost Per Read**
Every read pays decompression cost:
```
Read 1: Decompress all row groups
Read 2: Decompress all row groups (again)
Read N: Decompress all row groups (again)
```

Unlike .mbf where decompression is one-time.

#### 2. **Complex Implementation**
Parquet format is complex:
- Multiple compression codecs
- Multiple encodings (RLE, bit-packing, delta)
- Nested types (maps, lists, structs)
- Definition/repetition levels

Current implementation in src/io/parquet/ is basic - streaming adds complexity.

#### 3. **Random Access Overhead**
To read specific row groups:
```
1. Read footer (end of file)
2. Parse metadata
3. Seek to row group N
4. Decompress
```

.mbf could have faster random access with upfront index.

---

## Proposal 3: Hybrid Approach

### Smart Caching Strategy

```typescript
interface ParquetReader {
  async read(path: string, options: {
    cache?: boolean;
    cacheDir?: string;
  }) {
    const cacheKey = await this.getCacheKey(path);
    const mbfPath = `${cacheDir}/${cacheKey}.mbf`;

    if (await exists(mbfPath)) {
      // Cache hit - stream from .mbf
      return DataFrame.fromMbfStream(mbfPath);
    }

    if (options.cache) {
      // Cache miss - convert in background
      const convertTask = this.convertToMbf(path, mbfPath);

      // Stream from Parquet now, .mbf available for next read
      return DataFrame.fromParquetStream(path);
    } else {
      // No caching - direct stream
      return DataFrame.fromParquetStream(path);
    }
  }
}
```

### Use Cases

#### Use Case 1: Ad-hoc Exploration (No Cache)
```typescript
// One-time analysis, don't cache
const df = await readParquet("download.parquet", { cache: false });
await df.groupBy("category").count().show();
```
→ Direct Parquet streaming, no .mbf overhead

#### Use Case 2: Repeated Analytics (Cache)
```typescript
// Production dashboard, same file queried hourly
const df = await readParquet("sales.parquet", { cache: true });

// First run: converts to .mbf (slow)
// Runs 2-N: streams from .mbf (fast)
```
→ Amortize conversion cost over multiple reads

#### Use Case 3: ETL Pipeline (Explicit Conversion)
```typescript
// ETL job: download Parquet, convert, discard Parquet
await convertParquetToMbf("s3://data/*.parquet", "./local_cache/");

// Downstream jobs stream from .mbf
const df = await readMbf("./local_cache/data.mbf");
```
→ Separate conversion from query execution

#### Use Case 4: Large Dataset Subset (Index)
```typescript
// Only need rows where region='APAC'
const df = await readParquet("global_sales.parquet", {
  filter: col("region").eq("APAC"),
  cache: true,
  indexColumns: ["region"]  // Build index during conversion
});
```
→ .mbf includes bloom filter/index, skip irrelevant chunks

---

## Performance Modeling

### Scenario: 10GB Parquet File, Queried 5 Times

Assumptions:
- Parquet: 10GB compressed, 40GB uncompressed
- Decompression: 500 MB/s
- Disk I/O: 1 GB/s
- .mbf: 12GB (less compression)

#### Option A: Direct Parquet (Current)

```
Read 1: 10GB ÷ 500MB/s = 20s decompression + processing
Read 2: 20s + processing
Read 3: 20s + processing
Read 4: 20s + processing
Read 5: 20s + processing

Total: 100s decompression + 5× processing
Peak RAM: 40GB
```

#### Option B: Parquet Streaming (Proposal 2)

```
Read 1: 16 row groups × 1.25s = 20s + processing (streamed)
Read 2: 20s + processing
Read 3: 20s + processing
Read 4: 20s + processing
Read 5: 20s + processing

Total: 100s decompression + 5× processing
Peak RAM: 2.5GB (one row group)
```

**Savings**: 37.5GB memory, same time

#### Option C: .mbf Conversion (Proposal 1)

```
Conversion: 20s decompress + 12s write = 32s
Read 1: 12GB ÷ 1GB/s = 12s (from .mbf cache)
Read 2: 12s
Read 3: 12s
Read 4: 12s
Read 5: 12s

Total: 32s conversion + 60s reads = 92s + 5× processing
Peak RAM: 64MB (chunk size)
Disk: +12GB
```

**Savings**: 8s time, 39.9GB memory, cost: 12GB disk

#### Option D: Hybrid (Proposal 3)

```
Read 1: 20s Parquet stream + convert to .mbf in background
Read 2-5: 12s each from .mbf

Total: 20s + 48s = 68s + 5× processing
Peak RAM: 2.5GB → 64MB after conversion
Disk: +12GB
```

**Savings**: 32s time, best of both worlds

---

## Decision Matrix

| Criterion | Direct Parquet | Stream Parquet | .mbf Conversion | Hybrid |
|-----------|---------------|----------------|-----------------|--------|
| **First Read Speed** | Medium | Medium | Slow (-60%) | Medium |
| **Subsequent Reads** | Medium | Medium | Fast (+60%) | Fast (+60%) |
| **Memory Usage** | High (40GB) | Low (2.5GB) | Very Low (64MB) | Low → Very Low |
| **Disk Usage** | Low (10GB) | Low (10GB) | High (22GB) | High (22GB) |
| **Implementation Complexity** | Low | Medium | High | Very High |
| **Parquet Feature Support** | Full | Full | Partial* | Full |
| **Network Efficiency** | Poor | Medium | Excellent | Excellent |
| **Cache Invalidation** | N/A | N/A | Complex | Complex |

\* Depends on .mbf format capabilities

---

## Recommendations

### Recommendation 1: Implement Native Parquet Streaming First

**Priority: HIGH**

Start with Proposal 2 (native streaming) because:

1. **Immediate value**: Reduces memory 10-20x with moderate effort
2. **No storage overhead**: No cache management complexity
3. **Standard format**: Leverage Parquet's built-in optimizations
4. **Foundation**: Can add .mbf caching later if needed

**Implementation Steps**:

1. Refactor `ParquetReader` to async iterator
2. Implement row-group-level streaming
3. Add predicate pushdown via row group statistics
4. Add column projection (only read needed columns)

**Impact**:
- ✅ Handle 100GB+ files with 2-4GB RAM
- ✅ Start processing immediately (no full load)
- ✅ Compatible with streaming pipeline architecture
- ⚠️ Still pays decompression cost per read

### Recommendation 2: Add .mbf Conversion as Opt-In Feature

**Priority: MEDIUM**

Add .mbf conversion for specific use cases:

1. **Production dashboards**: Same file queried repeatedly
2. **Network-limited environments**: Slow download, fast local reads
3. **Low-power devices**: CPU-bound, decompression expensive

**Implementation Approach**:

```typescript
// Explicit conversion
await convertToMbf("data.parquet", "data.mbf");

// Or opt-in caching
const df = await readParquet("data.parquet", { cache: true });
```

**When NOT to use .mbf**:
- One-time analysis
- Frequently updated data
- Storage-constrained environments
- Ad-hoc exploration

### Recommendation 3: Consider Arrow IPC Format

**Priority: LOW (Research)**

Instead of custom .mbf, consider **Apache Arrow IPC** (Feather v2):

**Advantages**:
- Standardized streaming format
- Zero-copy deserialization
- Interop with other tools (Pandas, Polars, DuckDB)
- Designed for in-memory columnar data

**Conversion Pipeline**:
```
Parquet → Arrow IPC (streaming) → Molniya chunks
```

This gives streaming benefits without custom format maintenance.

---

## Compression Trade-offs Deep Dive

### Parquet Compression Characteristics

```
Original CSV: 100 GB
Parquet (zstd): 8 GB (12.5× compression)
  - Dictionary encoding: 3×
  - RLE: 2×
  - Bit-packing: 1.5×
  - Zstd: 2.5×
```

### .mbf Compression Options

#### Option A: Minimal Compression
```
.mbf with dictionary-only: 25 GB
- Fast reads (no decompression)
- 3× larger than Parquet
- Still 4× smaller than CSV
```

**Use case**: SSD storage, CPU-bound queries

#### Option B: Balanced Compression
```
.mbf with Snappy: 12 GB
- Fast decompression (500 MB/s)
- 1.5× larger than Parquet
- Good read performance
```

**Use case**: General purpose

#### Option C: Maximum Compression
```
.mbf with Zstd level 9: 9 GB
- Slow reads (200 MB/s)
- Comparable to Parquet
- Defeats caching purpose
```

**Use case**: Storage-constrained, infrequent reads

**Recommendation**: Option B (Snappy) balances size and speed.

---

## Implementation Roadmap

### Phase 1: Foundation (Week 1-2)
- [ ] Define .mbf format specification
- [ ] Implement basic .mbf writer (uncompressed)
- [ ] Implement basic .mbf reader (streaming)
- [ ] Add unit tests for round-trip conversion

### Phase 2: Parquet Streaming (Week 3-4)
- [ ] Refactor ParquetReader to async iterator
- [ ] Implement row group streaming
- [ ] Add predicate pushdown via statistics
- [ ] Add column projection
- [ ] Integration tests with large files

### Phase 3: .mbf Optimization (Week 5-6)
- [ ] Add Snappy compression to .mbf
- [ ] Implement chunk index for random access
- [ ] Add bloom filters for string columns
- [ ] Add zone maps (min/max per chunk)
- [ ] Performance benchmarks

### Phase 4: Hybrid System (Week 7-8)
- [ ] Implement cache key generation
- [ ] Add background conversion task
- [ ] Implement cache invalidation strategy
- [ ] Add cache size limits (LRU eviction)
- [ ] Documentation and examples

---

## Open Questions

1. **Cache Location**: Where to store .mbf files?
   - `/tmp` (cleared on restart)
   - User's home directory (persistent)
   - Configurable cache directory
   - Recommendation: XDG cache dir on Linux, similar on Windows/Mac

2. **Cache Invalidation**: When to invalidate .mbf cache?
   - Source file mtime changed
   - Source file size changed
   - Hash-based (expensive)
   - Recommendation: mtime + size check

3. **Compression Algorithm**: Which to use for .mbf?
   - Snappy: Fast, moderate compression
   - Zstd: Slower, better compression
   - LZ4: Fastest, least compression
   - Recommendation: Snappy (default), configurable

4. **.mbf Format Versioning**: How to handle format evolution?
   - Magic number + version in header
   - Breaking changes require cache invalidation
   - Recommendation: Semantic versioning, v1.0.0 to start

5. **Chunk Size**: What's optimal for .mbf?
   - 64KB: Too small, overhead from many chunks
   - 64MB: Like Parquet row groups
   - 1MB: Balance between memory and I/O
   - Recommendation: 8MB default, configurable

---

## Conclusion

**Short Term**: Implement native Parquet streaming (Proposal 2)
- Solves memory problem immediately
- No storage overhead
- Moderate implementation effort
- Leverages Parquet's built-in features

**Long Term**: Add opt-in .mbf caching (Hybrid Proposal 3)
- For production workloads with repeated queries
- Explicit conversion or automatic caching
- Decouple conversion from execution

**Don't**: Make .mbf conversion mandatory
- Adds complexity for diminishing returns on one-time reads
- Storage overhead not justified for all use cases
- Native Parquet streaming is sufficient for most scenarios

The key insight: **Parquet is already a columnar streaming format**. The opportunity is to leverage its structure better rather than converting to another format. .mbf conversion is an optimization for specific workloads, not a general requirement.

---

*Analysis Date: 2026-02-08*
*Author: Based on Molniya codebase analysis*
