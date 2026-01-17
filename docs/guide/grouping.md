# Grouping & Aggregation

## GroupBy

Group rows by column values:

```typescript
const byCategory = df.groupby('category');
```

## Aggregations

```typescript
// Count per group
const counts = df.groupby('category').count();

// Sum per group
const totals = df.groupby('region').sum('revenue');

// Mean per group
const averages = df.groupby('department').mean('salary');
```

## Multiple Groups

```typescript
const nested = df.groupby(['year', 'month']).sum('sales');
```

## Describe

Get summary statistics:

```typescript
df.describe().print();
```

Output:
```
┌───────┬───────────┬───────────┬───────────┐
│       │ age       │ salary    │ score     │
├───────┼───────────┼───────────┼───────────┤
│ count │ 100       │ 100       │ 100       │
│ mean  │ 34.5      │ 65000     │ 78.2      │
│ std   │ 8.2       │ 15000     │ 12.4      │
│ min   │ 22        │ 35000     │ 45        │
│ max   │ 58        │ 120000    │ 99        │
└───────┴───────────┴───────────┴───────────┘
```

## Unique Values

```typescript
// Remove duplicate rows
const unique = df.unique();

// Unique values in a column
const categories = df.col('category').unique();
```
