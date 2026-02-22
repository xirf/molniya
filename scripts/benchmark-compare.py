"""
Side-by-side benchmark: Polars vs Molniya
Tests the same operations on the same data for honest comparison.

Two phases:
  Phase 1: End-to-end CSV read + query (cold)
  Phase 2: In-memory query only (hot, pre-loaded data)

Run: python scripts/benchmark-compare.py
"""

import polars as pl
import time
import os
import sys

CSV_PATH = "students_worldwide_100k.csv"

if not os.path.exists(CSV_PATH):
    print(f"ERROR: {CSV_PATH} not found")
    sys.exit(1)

file_size_mb = os.path.getsize(CSV_PATH) / (1024 * 1024)
print(f"CSV file: {CSV_PATH} ({file_size_mb:.1f} MB)")
print(f"Polars version: {pl.__version__}")
print()

# ─── Phase 1: End-to-end CSV read ───────────────────────────────────────

print("=" * 70)
print("Phase 1: CSV Read (cold, from disk)")
print("=" * 70)

t0 = time.perf_counter()
df = pl.read_csv(CSV_PATH, schema_overrides={"student_rollno": pl.Int32})
t1 = time.perf_counter()
csv_time = (t1 - t0) * 1000
row_count = len(df)
print(f"  Rows loaded: {row_count:,}")
print(f"  Read time:   {csv_time:.0f} ms")
print(f"  Throughput:  {row_count / csv_time * 1000 / 1e6:.1f} M rows/s")
print()

# ─── Phase 2: In-memory queries ────────────────────────────────────────

print("=" * 70)
print("Phase 2: In-memory query benchmarks")
print("=" * 70)

N_RUNS = 5  # multiple runs for stable timing


def bench(label, fn, expected_count=None):
    """Run a query N_RUNS times, report median time and verify result."""
    times = []
    result = None
    for _ in range(N_RUNS):
        t0 = time.perf_counter()
        result = fn()
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000)

    times.sort()
    median_ms = times[N_RUNS // 2]
    throughput = row_count / median_ms * 1000

    # Verify result
    check = ""
    if expected_count is not None:
        actual = result if isinstance(result, int) else len(result)
        check = f" ✓" if actual == expected_count else f" ✗ got {actual}"

    if throughput >= 1e9:
        tp_str = f"{throughput / 1e9:.2f} B/s"
    elif throughput >= 1e6:
        tp_str = f"{throughput / 1e6:.1f} M/s"
    else:
        tp_str = f"{throughput / 1e3:.0f} K/s"

    print(f"  {label:30s} │ {median_ms:8.1f} ms │ {tp_str:>12s} │ result={result if isinstance(result, int) else len(result):>10,}{check}")
    return median_ms, result


print(f"\n  {'Query':30s} │ {'Time':>8s}    │ {'Throughput':>12s} │ {'Result':>10s}")
print("  " + "─" * 30 + "─┼─" + "─" * 11 + "─┼─" + "─" * 12 + "─┼─" + "─" * 20)

# 1. Count all
bench("Count All", lambda: row_count)

# 2. Simple numeric filter
r1_expected = len(df.filter(pl.col("student_rollno") < 500000))
bench("Filter: rollno < 500K",
      lambda: len(df.filter(pl.col("student_rollno") < 500000)),
      r1_expected)

# 3. String contains
r2_expected = len(df.filter(pl.col("country").str.contains("ian")))
bench("String: country contains 'ian'",
      lambda: len(df.filter(pl.col("country").str.contains("ian"))),
      r2_expected)

# 4. Complex filter (AND)
r3_expected = len(df.filter(
    (pl.col("student_rollno") >= 10000) & pl.col("country").str.starts_with("A")
))
bench("Complex: rollno>=10K AND country^A",
      lambda: len(df.filter(
          (pl.col("student_rollno") >= 10000) & pl.col("country").str.starts_with("A")
      )),
      r3_expected)

# 5. GroupBy count
bench("GroupBy country count",
      lambda: len(df.group_by("country").agg(pl.count())))

# 6. Sort
bench("Sort by rollno",
      lambda: len(df.sort("student_rollno")),
      row_count)

print()

# ─── Phase 3: Scaled tests ─────────────────────────────────────────────

print("=" * 70)
print("Phase 3: Scaling behavior (1K → Full)")
print("=" * 70)

scales = [
    ("1K", 1_000),
    ("10K", 10_000),
    ("100K", 100_000),
    ("1M", 1_000_000),
    ("Full", row_count),
]

print(f"\n  Simple Filter (rollno < 500K) at different scales:")
print(f"  {'Scale':8s} │ {'Rows':>10s} │ {'Time':>8s}    │ {'Throughput':>12s} │ {'Matched':>10s}")
print("  " + "─" * 8 + "─┼─" + "─" * 10 + "─┼─" + "─" * 11 + "─┼─" + "─" * 12 + "─┼─" + "─" * 10)

for name, n in scales:
    subset = df.head(n) if n < row_count else df
    times = []
    result = None
    for _ in range(N_RUNS):
        t0 = time.perf_counter()
        result = len(subset.filter(pl.col("student_rollno") < 500000))
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000)
    times.sort()
    median_ms = times[N_RUNS // 2]
    throughput = n / median_ms * 1000
    if throughput >= 1e9:
        tp_str = f"{throughput / 1e9:.2f} B/s"
    elif throughput >= 1e6:
        tp_str = f"{throughput / 1e6:.1f} M/s"
    else:
        tp_str = f"{throughput / 1e3:.0f} K/s"
    print(f"  {name:8s} │ {n:>10,} │ {median_ms:8.1f} ms │ {tp_str:>12s} │ {result:>10,}")

print()
print("─" * 70)
print("These are Polars numbers to compare against Molniya's benchmark.")
print("Molniya benchmark: bun run scripts/benchmark-compare.ts")
print()
