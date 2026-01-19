import pandas as pd
import time
import os
import psutil

DATA_PATH = './artifac/2019-Nov.csv'
CHUNK_SIZE = 100000

def get_memory():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

print("--- Pandas Benchmark (Chunked) ---")
start = time.time()

# 1. Scan (Metadata only in pandas isn't really thing, so we start reading)
brand_counts = pd.Series(dtype='int64')

# 2. Filter & Aggregate in chunks
print("Iterating chunks...")
chunks_processed = 0
for chunk in pd.read_csv(DATA_PATH, chunksize=CHUNK_SIZE):
    # Filter
    purchases = chunk[chunk['event_type'] == 'purchase']
    # Aggregate
    brand_counts = brand_counts.add(purchases['brand'].value_counts(), fill_value=0)
    
    chunks_processed += 1
    if chunks_processed % 100 == 0:
        print(f"Processed {chunks_processed} chunks... Memory: {get_memory():.2f}MB")

# 4. Encoding (Standardized to first 10 rows like mornye test)
# For pandas, to_ordinal is usually factorize()
head_df = pd.read_csv(DATA_PATH, nrows=10)
codes, uniques = pd.factorize(head_df['event_type'])

end = time.time()
print("━" * 40)
print(f"Total Time: {end - start:.2f}s")
print(f"Process RSS: {get_memory():.2f}MB")
print("Top Brands (purchases):")
print(brand_counts.sort_values(ascending=False).head(5))
