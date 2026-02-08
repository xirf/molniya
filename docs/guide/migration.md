# Migration Guides

Coming from another DataFrame library? These guides will help you transition to Molniya.

## Available Guides

- [Migrating from Pandas](./migration-pandas.md) - From Python's pandas library
- [Migrating from Polars](./migration-polars.md) - From the Rust-based Polars library
- [Migrating from Danfo.js](./migration-danfo.md) - From the JavaScript DataFrame library
- [Migrating from Arquero](./migration-arquero.md) - From the UW IDL's data processing library

## Quick Comparison

| Feature         | Pandas   | Polars       | Danfo.js     | Arquero      | Molniya      |
| --------------- | -------- | ------------ | ------------ | ------------ | ------------ |
| **Language**    | Python   | Python/Rust  | JavaScript   | JavaScript   | TypeScript   |
| **Runtime**     | CPython  | CPython      | Node/Browser | Node/Browser | Bun          |
| **Execution**   | Eager    | Lazy/Eager   | Eager        | Eager        | Lazy         |
| **Schema**      | Inferred | Optional     | Inferred     | Inferred     | Required     |
| **Streaming**   | Limited  | Yes          | Limited      | No           | Yes          |
| **Type Safety** | Runtime  | Compile-time | Limited      | None         | Compile-time |

## Common Patterns

### Reading Data

::: code-group

```python [Pandas]
import pandas as pd
df = pd.read_csv("data.csv")
```

```python [Polars]
import polars as pl
df = pl.read_csv("data.csv")
```

```javascript [Danfo.js]
import dfd from "danfojs";
const df = await dfd.read_csv("data.csv");
```

```javascript [Arquero]
import * as aq from "arquero";
const dt = await aq.loadCSV("data.csv");
```

```typescript [Molniya]
import { readCsv, DType } from "molniya";
const df = await readCsv("data.csv", {
  id: DType.int32,
  name: DType.string
});
```

:::

### Filtering

::: code-group

```python [Pandas]
df[df["age"] > 25]
```

```python [Polars]
df.filter(pl.col("age") > 25)
```

```javascript [Danfo.js]
df.loc({ rows: df["age"].gt(25) })
```

```javascript [Arquero]
dt.filter(d => d.age > 25)
```

```typescript [Molniya]
df.filter(col("age").gt(25))
```

:::

### GroupBy

::: code-group

```python [Pandas]
df.groupby("dept").agg({"salary": "sum"})
```

```python [Polars]
df.group_by("dept").agg(pl.col("salary").sum())
```

```javascript [Danfo.js]
df.groupby(["dept"]).col(["salary"]).sum()
```

```javascript [Arquero]
dt.groupby("dept").rollup({ total: d => aq.op.sum(d.salary) })
```

```typescript [Molniya]
df.groupBy("dept", [{ name: "total", expr: sum("salary") }])
```

:::

## Key Differences to Remember

### 1. Schema is Required

Unlike other libraries, Molniya requires you to define the schema upfront:

```typescript
const df = await readCsv("data.csv", {
  id: DType.int32,
  name: DType.string,
  amount: DType.float64
});
```

### 2. Lazy Execution

Operations don't execute until you call a terminal method:

```typescript
// Builds plan only
const filtered = df.filter(col("age").gt(25));

// Executes
await filtered.show();
```

### 3. Async Operations

All terminal methods return Promises:

```typescript
const data = await df.toArray();
const count = await df.count();
await df.show();
```

### 4. Expression-Based API

Molniya uses expression objects rather than callbacks or operators:

```typescript
// Not: df["age"] > 25
// Not: d => d.age > 25
// But:
col("age").gt(25)
```

## Which Guide Should I Read?

- **Coming from Python?** Start with the [Pandas](./migration-pandas.md) guide
- **Familiar with Polars?** The [Polars](./migration-polars.md) guide will feel very familiar
- **Using JavaScript DataFrames?** Check [Danfo.js](./migration-danfo.md) or [Arquero](./migration-arquero.md)
