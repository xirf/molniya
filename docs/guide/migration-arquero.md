# Migrating from Arquero

This guide helps you transition from Arquero (JavaScript library for data processing, part of the UW Interactive Data Lab) to Molniya.

## Key Differences

| Aspect | Arquero | Molniya |
|--------|---------|---------|
| **Design** | Method chaining, table-oriented | Expression-based, columnar |
| **Runtime** | Node.js/Browser | Bun only |
| **Execution** | Eager | Lazy |
| **Memory** | Standard arrays/objects | TypedArrays (columnar) |
| **Schema** | Inferred | Required |
| **Streaming** | No | Native |
| **Type Safety** | None | Full TypeScript |

## Installation

::: code-group

```bash [Arquero]
npm install arquero
```

```bash [Molniya]
bun add molniya
```

:::

## Loading Data

### From Arrays

::: code-group

```javascript [Arquero]
import * as aq from "arquero";

const data = [
  { a: 1, b: "x" },
  { a: 2, b: "y" },
  { a: 3, b: "z" }
];

const dt = aq.table(data);
```

```typescript [Molniya]
import { fromRecords, DType } from "molniya";

const data = [
  { a: 1, b: "x" },
  { a: 2, b: "y" },
  { a: 3, b: "z" }
];

const df = fromRecords(data, {
  a: DType.int32,
  b: DType.string
});
```

:::

### From CSV

::: code-group

```javascript [Arquero]
import * as aq from "arquero";

// From file
const dt = await aq.loadCSV("data.csv");

// From string
const dt = aq.fromCSV(csvString);
```

```typescript [Molniya]
import { readCsv, DType } from "molniya";

// From file (streaming)
const df = await readCsv("data.csv", {
  a: DType.int32,
  b: DType.string
});

// From string
import { fromCsvString } from "molniya";
const df = fromCsvString(csvString, {
  a: DType.int32,
  b: DType.string
});
```

:::

### From JSON

::: code-group

```javascript [Arquero]
import * as aq from "arquero";

const dt = await aq.loadJSON("data.json");
```

```typescript [Molniya]
// Parse JSON and use fromRecords
const data = await Bun.file("data.json").json();
const df = fromRecords(data, schema);
```

:::

## Verbs vs Expressions

Arquero uses "verbs" (methods like `filter`, `select`) with callback functions. Molniya uses expression objects:

::: code-group

```javascript [Arquero]
import * as aq from "arquero";

// Arquero uses op functions inside callbacks
const result = dt
  .filter(d => d.age > 25)
  .derive({ bonus: d => d.salary * 0.1 });
```

```typescript [Molniya]
import { col } from "molniya";

// Molniya uses expression objects
const result = df
  .filter(col("age").gt(25))
  .withColumn("bonus", col("salary").mul(0.1));
```

:::

## Selecting Columns

::: code-group

```javascript [Arquero]
// Select columns
dt.select("a", "b");
dt.select(aq.not("temp_col"));

// Drop columns
dt.select(aq.not("temp_col"));
```

```typescript [Molniya]
// Select columns
df.select("a", "b");

// Drop columns
df.drop("temp_col");
```

:::

## Filtering

::: code-group

```javascript [Arquero]
// Filter with callback
dt.filter(d => d.age > 25);
dt.filter(d => d.dept === "Engineering" && d.age > 25);
dt.filter(aq.escape(d => externalArray.includes(d.id)));
```

```typescript [Molniya]
import { col, and, or } from "molniya";

// Filter with expressions
df.filter(col("age").gt(25));
df.filter(and(
  col("dept").eq("Engineering"),
  col("age").gt(25)
));
```

:::

## Adding/Deriving Columns

::: code-group

```javascript [Arquero]
// Derive single column
dt.derive({ total: d => d.a + d.b });

// Derive multiple columns
dt.derive({
  total: d => d.a + d.b,
  avg: d => (d.a + d.b) / 2
});

// Modify existing
dt.assign({ a: d => d.a * 2 });
```

```typescript [Molniya]
import { col } from "molniya";

// Single column
df.withColumn("total", col("a").add(col("b")));

// Multiple columns
df.withColumns({
  total: col("a").add(col("b")),
  avg: col("a").add(col("b")).div(2)
});
```

:::

## Aggregation

::: code-group

```javascript [Arquero]
import * as aq from "arquero";

// Group and rollup
dt.groupby("dept")
  .rollup({
    total: d => aq.op.sum(d.salary),
    avg: d => aq.op.mean(d.salary),
    count: () => aq.op.count()
  });

// Multiple keys
dt.groupby(["dept", "role"]).rollup({ ... });
```

```typescript [Molniya]
import { sum, avg, count } from "molniya";

// Group and aggregate
df.groupBy("dept", [
  { name: "total", expr: sum("salary") },
  { name: "avg", expr: avg("salary") },
  { name: "count", expr: count() }
]);

// Multiple keys
df.groupBy(["dept", "role"], [...]);
```

:::

## Sorting

::: code-group

```javascript [Arquero]
// Sort
dtorderby("a");
dtorderby(aq.desc("a"));
dtorderby(["dept", "salary"], [aq.asc, aq.desc]);
```

```typescript [Molniya]
import { asc, desc } from "molniya";

// Sort
df.sort(asc("a"));
df.sort(desc("a"));
df.sort([asc("dept"), desc("salary")]);
```

:::

## Joining

::: code-group

```javascript [Arquero]
// Join
dt1.join(dt2, ["id"]);
dt1.join_left(dt2, ["id"]);
dt1.join_right(dt2, ["id"]);
dt1.join_full(dt2, ["id"]);

// Different keys
dt1.join(dt2, [["a"], ["b"]]);  // dt1.a = dt2.b
```

```typescript [Molniya]
// Joins
await df1.innerJoin(df2, "id");
await df1.leftJoin(df2, "id");

// Different keys
await df1.innerJoin(df2, "a", "b");
```

:::

## Window Functions

::: code-group

```javascript [Arquero]
// Window functions
dt.derive({
  row_num: aq.op.row_number(),
  rank: aq.op.rank(),
  lag_val: aq.op.lag("value")
});
```

```typescript [Molniya]
// Window functions not yet implemented
```

:::

## Set Operations

::: code-group

```javascript [Arquero]
// Concatenate
dt1.concat(dt2);

// Union (deduplicated)
aq.union(dt1, dt2);

// Intersection
aq.intersect(dt1, dt2);

// Except (set difference)
aq.except(dt1, dt2);
```

```typescript [Molniya]
// Concatenate available
import { concat } from "molniya";
concat([df1, df2]);

// Union, intersect, except not available
```

:::

## Sampling

::: code-group

```javascript [Arquero]
// Sample
dt.sample(100);  // 100 rows
dt.sample(0.1);  // 10% of rows
```

```typescript [Molniya]
// Sampling not available
// Use filter with random condition as workaround
```

:::

## Missing Values

::: code-group

```javascript [Arquero]
// Drop missing
dt.drop({ if: d => d.name === undefined });

// Impute
dt.impute({ value: d => 0 });
dt.impute({ value: d => aq.op.mean(d.column) });
```

```typescript [Molniya]
// Drop nulls
df.dropNulls();
df.dropNulls("column_name");

// Fill nulls
df.fillNull("column_name", 0);
```

:::

## Exporting Data

::: code-group

```javascript [Arquero]
// To array of objects
dt.objects();

// To array of arrays
dt.array("column");

// To CSV
aq.toCSV(dt);
```

```typescript [Molniya]
// To array of objects
await df.toArray();

// To CSV - not yet implemented
// Use toArray and format manually
```

:::

## Printing/Inspecting

::: code-group

```javascript [Arquero]
// Print
dt.print();
dt.print(10);  // First 10 rows

// Get info
dt.numRows();
dt.numCols();
dt.columnNames();
```

```typescript [Molniya]
// Print
await df.show();
await df.limit(10).show();

// Get info
await df.count();
df.columnNames;
df.schema;
```

:::

## Arquero's `op` vs Molniya's Expression Builders

Arquero uses `aq.op` for operations inside callbacks:

```javascript
// Arquero
import * as aq from "arquero";

dt.rollup({
  total: d => aq.op.sum(d.value),
  avg: d => aq.op.mean(d.value),
  min: d => aq.op.min(d.value),
  max: d => aq.op.max(d.value)
});
```

Molniya uses standalone functions that create expression ASTs:

```typescript
// Molniya
import { sum, avg, min, max } from "molniya";

df.groupBy("key", [
  { name: "total", expr: sum("value") },
  { name: "avg", expr: avg("value") },
  { name: "min", expr: min("value") },
  { name: "max", expr: max("value") }
]);
```

## Key API Mapping

| Arquero | Molniya | Notes |
|---------|---------|-------|
| `aq.table(data)` | `fromRecords(data, schema)` | Schema required |
| `aq.loadCSV(path)` | `readCsv(path, schema)` | Schema required |
| `aq.fromCSV(str)` | `fromCsvString(str, schema)` | |
| `aq.loadJSON(path)` | Parse + `fromRecords()` | |
| `dt.print()` | `await df.show()` | Async |
| `dt.numRows()` | `await df.count()` | Async |
| `dt.numCols()` | `df.columnNames.length` | |
| `dt.columnNames()` | `df.columnNames` | Property |
| `dt.select("a", "b")` | `df.select("a", "b")` | |
| `dt.select(aq.not("x"))` | `df.drop("x")` | |
| `dt.filter(d => ...)` | `df.filter(col(...))` | Expression-based |
| `dt.derive({...})` | `df.withColumns({...})` | |
| `dt.assign({...})` | `df.withColumns({...})` | |
| `dt.groupby().rollup()` | `df.groupBy()` | Different API |
| `dt.orderby()` | `df.sort()` | |
| `aq.desc()` | `desc()` | |
| `dt.join()` | `df.innerJoin()` | Async |
| `dt.join_left()` | `df.leftJoin()` | Async |
| `dt.concat()` | `concat([df1, df2])` | |
| `dt.objects()` | `await df.toArray()` | Async |
| `dt.drop()` | `df.dropNulls()` | |
| `dt.impute()` | `df.fillNull()` | |
| `dt.sample()` | Not available | |
| `dt.slice()` | `df.limit()` | |

## Expression Comparison

| Arquero | Molniya |
|---------|---------|
| `d => d.a + d.b` | `col("a").add(col("b"))` |
| `d => d.a - d.b` | `col("a").sub(col("b"))` |
| `d => d.a * d.b` | `col("a").mul(col("b"))` |
| `d => d.a / d.b` | `col("a").div(col("b"))` |
| `d => d.a > 5` | `col("a").gt(5)` |
| `d => d.a === 5` | `col("a").eq(5)` |
| `d => d.a !== 5` | `col("a").neq(5)` |
| `d => d.name == null` | `col("name").isNull()` |
| `d => d.a > 5 && d.b < 10` | `and(col("a").gt(5), col("b").lt(10))` |
| `d => d.a > 5 \|\| d.b < 10` | `or(col("a").gt(5), col("b").lt(10))` |

## Best Practices

1. **Schema definition** - Unlike Arquero's inference, Molniya requires explicit schemas
2. **Async operations** - All terminal operations are async in Molniya
3. **Expression building** - Build expressions with `col()` instead of callbacks
4. **No window functions** - Some Arquero features not yet available
5. **Streaming** - Molniya can handle larger datasets due to streaming
6. **Type safety** - Leverage TypeScript for compile-time checks

## Performance Considerations

| Aspect | Arquero | Molniya |
|--------|---------|---------|
| Memory layout | Row objects | Columnar TypedArrays |
| Large files | Loads entirely | Streams efficiently |
| Type checking | Runtime | Compile-time |
| Bundle size | Small | Small |
| Execution model | Eager | Lazy |
