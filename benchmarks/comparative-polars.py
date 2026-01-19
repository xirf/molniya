import polars as pl
import time
import os
import psutil

DATA_PATH = './artifac/2019-Nov.csv'

def get_memory():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

print("--- Polars (Python) Benchmark ---")
start = time.time()

# 1. Scan (Lazy)
lazy = pl.scan_csv(DATA_PATH)

# 2. Filter & Aggregate (Lazy)
query = (
    lazy
    .filter(pl.col("event_type") == "purchase")
    .group_by("brand")
    .count()
    .sort("count", descending=True)
)

# Execute
print("Executing lazy query...")
result = query.collect()

# 4. Encoding
# In polars, cast to Categorical or use factorize
encoded = lazy.select(pl.col("event_type").to_physical()).head(10).collect()

end = time.time()
print("━" * 40)
print(f"Total Time: {end - start:.2f}s")
print(f"Process RSS: {get_memory():.2f}MB")
print("Top Brands (purchases):")
print(result.head(5))
