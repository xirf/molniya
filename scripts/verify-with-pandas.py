"""
Verify Molniya benchmark results against pandas.
Runs the same queries at the same row scales and compares counts.
"""

import pandas as pd
import time

CSV_PATH = "students_worldwide_100k.csv"

SCALES = [
    ("1K",   1_000),
    ("10K",  10_000),
    ("100K", 100_000),
    ("1M",   1_000_000),
]

# Expected results from Molniya benchmark run
MOLNIYA = {
    "1K":   {"Simple Filter": 1_000,  "String Search": 147,     "Complex Filter": 0,      "Count All": 1_000},
    "10K":  {"Simple Filter": 10_000, "String Search": 1_546,   "Complex Filter": 0,      "Count All": 10_000},
    "100K": {"Simple Filter": 100_000,"String Search": 15_194,  "Complex Filter": 6_128,  "Count All": 100_000},
    "1M":   {"Simple Filter": 499_999,"String Search": 151_261, "Complex Filter": 69_135, "Count All": 1_000_000},
}

print("Loading full CSV into pandas...")
t0 = time.perf_counter()
full_df = pd.read_csv(CSV_PATH, dtype=str)
# Coerce student_rollno to int
full_df["student_rollno"] = pd.to_numeric(full_df["student_rollno"], errors="coerce")
elapsed = time.perf_counter() - t0
print(f"Loaded {len(full_df):,} total rows in {elapsed:.2f}s\n")

print(f"{'Scale':<8} {'Query':<22} {'Pandas':>12} {'Molniya':>12} {'Match':>6}")
print("-" * 65)

all_ok = True

for scale_name, max_rows in SCALES:
    df = full_df.head(max_rows)
    actual_rows = len(df)

    queries = {
        "Simple Filter":  len(df[df["student_rollno"] < 500_000]),
        "String Search":  len(df[df["country"].str.contains("or", na=False)]),
        "Complex Filter": len(df[(df["student_rollno"] >= 10_000) & df["country"].str.startswith("A", na=False)]),
        "Count All":      actual_rows,
    }

    molniya = MOLNIYA[scale_name]
    for qname, pandas_result in queries.items():
        molniya_result = molniya[qname]
        match = "✅" if pandas_result == molniya_result else "❌"
        if pandas_result != molniya_result:
            all_ok = False
        print(f"{scale_name:<8} {qname:<22} {pandas_result:>12,} {molniya_result:>12,} {match:>6}")

print("-" * 65)
if all_ok:
    print("\n✅ All results match! Molniya is correct.")
else:
    print("\n❌ Some results differ. Check above for mismatches.")
