# Filtering & Sorting

## Filter

Keep rows that match a condition:

```typescript
const adults = df.filter(row => row.age >= 18);

const expensive = df.filter(row => row.price > 100);
```

## Where

Filter by column value:

```typescript
const active = df.where('status', 'active');

const highScore = df.where('score', '>', 90);
```

## Sort

```typescript
// Ascending (default)
const sorted = df.sort('price');

// Descending
const sorted = df.sort('price', false);
```

## Select Columns

Pick specific columns:

```typescript
const subset = df.select(['name', 'email']);
```

## Drop Columns

Remove columns:

```typescript
const cleaned = df.drop('temp_id');

const minimal = df.drop(['debug_info', 'internal_code']);
```

## Head & Tail

```typescript
df.head(5).print();  // First 5 rows
df.tail(3).print();  // Last 3 rows
```
