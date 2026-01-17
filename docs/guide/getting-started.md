# Getting Started

## Installation

::: code-group
```bash [bun]
bun add mornye
```
```bash [npm]
npm install mornye
```
```bash [pnpm]
pnpm add mornye
```
:::

## Your First DataFrame

```typescript
import { DataFrame } from 'mornye';

const df = DataFrame.fromColumns({
  name: ['Alice', 'Bob', 'Carol'],
  age: [25, 30, 22],
  city: ['NYC', 'LA', 'Chicago']
});

df.print();
```

Output:
```
┌───────┬─────┬─────────┐
│ name  │ age │ city    │
├───────┼─────┼─────────┤
│ Alice │  25 │ NYC     │
│ Bob   │  30 │ LA      │
│ Carol │  22 │ Chicago │
└───────┴─────┴─────────┘
```

## Loading CSV Files

```typescript
import { readCsv } from 'mornye';

const { df } = await readCsv('./data.csv');

// Check the shape
console.log(df.shape); // [rows, columns]

// See column names
console.log(df.columns());

// Preview the data
df.head(5).print();
```

## What's Next?

- Learn about [Core Concepts](/guide/concepts)
- Explore [Filtering & Sorting](/guide/filtering)
- Check out the [API Reference](/api/dataframe)
