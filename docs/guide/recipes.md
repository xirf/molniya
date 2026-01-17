# Common Recipes

A collection of patterns for solving real-world data problems with Molniya.

## Cleaning Messy Data

Data from the wild is rarely clean. Here's a standard cleanup pipeline.

```typescript
// 1. Load raw data (everything as strings initially if it's very messy)
const { df } = await readCsv('./raw_leads.csv', { 
  nullValues: ['N/A', '-', ''] 
});

const clean = df
  // 2. Fix text case and trim whitespace
  .assign('email', row => row.email?.toLowerCase().trim())
  
  // 3. Fill missing values
  .fillna({
    status: 'new',
    score: 0
  })
  
  // 4. Convert types explicitly if needed
  .assign('score', row => Number(row.score))
  
  // 5. Drop rows that are still broken
  .filter(row => row.email && row.email.includes('@'));
```

## Time Series Analysis

### Moving Averages (SMA)
Calculate a 30-day rolling average. Assumes data is sorted by date.

```typescript
// (Note: Optimized window functions coming soon. For now, use map)
const prices = df.col('close');
const sma30 = prices.map((val, i) => {
  if (i < 29) return null; // Not enough data yet
  // Slice last 30 items
  const window = prices.slice(i - 29, i + 1);
  return window.mean();
});

const withIndicators = df.assign('sma_30', sma30);
```

### Resampling (Daily -> Monthly)
Aggregation by time period.

```typescript
const monthlySales = df
  // 1. Create a grouping key (e.g., "2023-01")
  .assign('month_key', row => new Date(row.timestamp).toISOString().slice(0, 7))
  // 2. Group and aggregate
  .groupby('month_key')
  .agg({
    revenue: 'sum',
    orders: 'count'
  })
  .sort('month_key');
```

## Deduplication

Keep only the *latest* entry for each user.

```typescript
const latestUserStatuses = logs
  // 1. Sort by time descending (newest first)
  .sort('timestamp', false)
  // 2. Unique by user_id (keeps the first occurrence, which is now the newest)
  .dropDuplicates('user_id');
```

## Merging Multiple Sources

Combining User data with Transaction data (Join).

```typescript
const users = await readCsv('users.csv');
const transactions = await readCsv('transactions.csv');

// Inner Join: Only keep transactions where we know the user
const enriched = transactions.merge(users, {
  left: 'user_id',   // column in transactions
  right: 'id',       // column in users
  how: 'inner'
});
```

## Finding Outliers (IQR Method)

Detect values that are statistically far from the norm.

```typescript
const prices = df.col('price');
const q1 = prices.quantile(0.25);
const q3 = prices.quantile(0.75);
const iqr = q3 - q1;

const upperBound = q3 + 1.5 * iqr;
const lowerBound = q1 - 1.5 * iqr;

// Filter to see the anomalies
const outliers = df.filter(row => 
  row.price > upperBound || row.price < lowerBound
);
```

