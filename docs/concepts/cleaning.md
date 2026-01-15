# Data Cleaning

Real-world data is often messy. Mornye provides robust tools for handling missing values, duplicates, and type mismatches.

## Handling Missing Data

### Fill NA
Replace missing values (`null`, `undefined`, `NaN`) with a specific value.

```typescript
// Forward fill (propagate last valid observation)
const filled = df.ffill();

// Backward fill (use next valid observation)
const bfilled = df.bfill();

// Fill specific value (soon)
// const zeros = df.fillna(0); 
```

### Drop NA
Remove rows or columns containing missing values.

```typescript
// Drop rows with any missing values (coming soon)
// const clean = df.dropna();
```

## Duplicates

Remove duplicate rows based on all or specific columns.

```typescript
// Drop exact duplicate rows
const unique = df.dropDuplicates();

// Drop duplicates based on specific columns
const uniqueByDept = df.dropDuplicates('department');
```

## Type Conversion

Convert column types safely using `astype`.

```typescript
const df = await readCsv('data.csv');

// Convert string column to float64
// Non-numeric strings become NaN
const numericSeries = df.col('price_str').astype('float64');
const cleanDf = df.assign('price', numericSeries.values());
```

## Replacing Values

Replace specific values across the DataFrame or Series.

```typescript
// Replace all -99 with NaN
const clean = df.replace(-99, NaN);
```

## Clipping

Limit values to a specific range.

```typescript
// Clip values between 0 and 100
const capped = df.clip(0, 100);
```
