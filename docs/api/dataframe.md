# DataFrame API

A `DataFrame` is a typed 2D columnar data structure.

## Creation

```typescript
import { DataFrame, m } from 'mornye';

const schema = {
  name: m.string(),
  age: m.int32(),
  score: m.float64(),
} as const;

const df = DataFrame.from(schema, [
  { name: 'Alice', age: 25, score: 95.5 },
  { name: 'Bob', age: 30, score: 87.2 },
]);

// Empty DataFrame
const empty = DataFrame.empty(schema);
```

## Properties

| Property | Type           | Description  |
| -------- | -------------- | ------------ |
| `shape`  | `[rows, cols]` | Dimensions   |
| `schema` | `Schema`       | Column types |

## Column Access

```typescript
df.col('age');      // Series<'int32'>
df.columns();       // ['name', 'age', 'score']
```

## Row Operations

```typescript
df.head(5);                    // First 5 rows
df.tail(5);                    // Last 5 rows
df.select('name', 'age');      // Pick columns
```

## Filtering

```typescript
// Predicate filter
df.filter(row => row.age > 25);

// SQL-like where
df.where('age', '>', 25);
df.where('name', '=', 'Bob');
df.where('score', '>=', 90);
df.where('name', 'in', ['Alice', 'Bob']);
df.where('name', 'contains', 'li');
```

### Operators

| Operator   | Description      |
| ---------- | ---------------- |
| `=`        | Equal            |
| `!=`       | Not equal        |
| `>`        | Greater than     |
| `>=`       | Greater or equal |
| `<`        | Less than        |
| `<=`       | Less or equal    |
| `in`       | Value in array   |
| `contains` | String contains  |

## Sorting

```typescript
df.sort('age');           // Ascending
df.sort('score', false);  // Descending
```

## GroupBy & Aggregation

```typescript
// Group by column
df.groupby('category')
  .agg({ price: 'mean', quantity: 'sum' });

// Shortcuts
df.groupby('category').sum('price', 'quantity');
df.groupby('category').mean('price');
df.groupby('category').count();
```

### Aggregation Functions

| Function | Description   |
| -------- | ------------- |
| `sum`    | Sum of values |
| `mean`   | Average       |
| `min`    | Minimum       |
| `max`    | Maximum       |
| `count`  | Row count     |
| `first`  | First value   |
| `last`   | Last value    |

## Apply & Transform

```typescript
// Apply function to each row
df.apply(row => `${row.name}: ${row.age}`);
// ['Alice: 25', 'Bob: 30']
```

## Info & Stats

```typescript
df.describe();  // Stats for numeric columns
df.info();      // { rows, columns, dtypes }
df.toArray();   // Array of row objects
```

## Iteration

```typescript
for (const row of df.rows()) {
  console.log(row.name, row.age);
}
```

## Display

```typescript
df.print();       // ASCII table to console
df.toString();    // ASCII table as string
```
