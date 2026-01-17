# Core Concepts

## DataFrame

A **DataFrame** is a 2D table with named columns. Think of it like a spreadsheet or SQL table.

```typescript
const df = DataFrame.fromColumns({
  product: ['Apple', 'Banana', 'Orange'],
  price: [1.20, 0.50, 0.80],
  quantity: [100, 150, 80]
});
```

Each column has a consistent type: `string`, `int32`, `float64`, or `bool`.

## Series

A **Series** is a single column. You can extract it from a DataFrame:

```typescript
const prices = df.col('price');

console.log(prices.sum());   // 2.50
console.log(prices.mean());  // 0.83
console.log(prices.max());   // 1.20
```

## Type Inference

When you load a CSV, Mornye automatically infers column types:

```csv
name,age,active
Alice,25,true
Bob,30,false
```

Results in:
- `name` → `string`
- `age` → `float64` (all numbers default to float64)
- `active` → `bool`

You can also provide an explicit schema:

```typescript
import { m } from 'mornye';

const { df } = await readCsv('./data.csv', {
  schema: {
    name: m.string(),
    age: m.float64(),  // Force float instead of int
    active: m.bool()
  }
});
```

## Immutability

All operations return a **new** DataFrame. The original is never modified.

```typescript
const filtered = df.filter(row => row.price > 1);
// df is unchanged
// filtered is a new DataFrame
```
