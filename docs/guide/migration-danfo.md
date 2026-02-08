# Migrating from Danfo.js

This guide helps you transition from Danfo.js (JavaScript DataFrame library inspired by pandas) to Molniya.

## Key Differences

| Aspect | Danfo.js | Molniya |
|--------|----------|---------|
| **Runtime** | Node.js/Browser | Bun only |
| **Backend** | TensorFlow.js | Native TypeScript |
| **Execution** | Eager | Lazy |
| **Memory** | Tensor-based | Columnar TypedArrays |
| **Schema** | Inferred | Required |
| **Streaming** | Limited | Native |
| **Type Safety** | Limited | Full TypeScript |

## Installation

::: code-group

```bash [Danfo.js]
npm install danfojs
# or
npm install danfojs-node  # For Node.js
```

```bash [Molniya]
bun add molniya
```

:::

## Creating DataFrames

### From Arrays

::: code-group

```javascript [Danfo.js]
import dfd from "danfojs";

const data = {
  "A": [1, 2, 3],
  "B": ["a", "b", "c"]
};
const df = new dfd.DataFrame(data);
```

```typescript [Molniya]
import { fromRecords, DType } from "molniya";

const data = [
  { A: 1, B: "a" },
  { A: 2, B: "b" },
  { A: 3, B: "c" }
];

const df = fromRecords(data, {
  A: DType.int32,
  B: DType.string
});
```

:::

### From CSV

::: code-group

```javascript [Danfo.js]
import dfd from "danfojs";

// Async read
const df = await dfd.read_csv("data.csv");

// With options
const df = await dfd.read_csv("data.csv", {
  delimiter: ",",
  headers: true
});
```

```typescript [Molniya]
import { readCsv, DType } from "molniya";

// Schema is required
const df = await readCsv("data.csv", {
  A: DType.int32,
  B: DType.string
});
```

:::

### From JSON

::: code-group

```javascript [Danfo.js]
import dfd from "danfojs";

const df = await dfd.read_json("data.json");
```

```typescript [Molniya]
// Molniya doesn't have direct JSON reader
// Parse JSON first, then use fromRecords
import { fromRecords } from "molniya";

const jsonData = await Bun.file("data.json").json();
const df = fromRecords(jsonData, schema);
```

:::

## Selecting Data

### Column Selection

::: code-group

```javascript [Danfo.js]
// Single column (returns Series)
df["column_name"];
df.column_name;

// Multiple columns
df.loc({ columns: ["col1", "col2"] });

// Drop columns
df.drop({ columns: ["col1"], axis: 1 });
```

```typescript [Molniya]
// Select columns
const result = df.select("col1", "col2");

// Drop columns
const result = df.drop("col1");
```

:::

### Row Selection

::: code-group

```javascript [Danfo.js]
// By index
df.iloc({ rows: [0, 1, 2] });
df.head(5);
df.tail(5);

// By condition
df.loc({ rows: df["age"].gt(25) });
```

```typescript [Molniya]
// Limit rows
const result = df.limit(5);

// Note: No tail() or iloc() - streaming model
// Filter instead
import { col } from "molniya";
const result = df.filter(col("age").gt(25));
```

:::

## Filtering

::: code-group

```javascript [Danfo.js]
// Boolean indexing
df.loc({ rows: df["age"].gt(25) });

// Query
df.query(df["age"].gt(25).and(df["dept"].eq("Engineering")));
```

```typescript [Molniya]
import { col, and } from "molniya";

// Filter
df.filter(col("age").gt(25));

// Multiple conditions
df.filter(and(
  col("age").gt(25),
  col("dept").eq("Engineering")
));
```

:::

## Adding Columns

::: code-group

```javascript [Danfo.js]
// Add column
df["new_col"] = df["col1"].add(df["col2"]);

// Using apply
df["new_col"] = df["col1"].apply(x => x * 2);
```

```typescript [Molniya]
import { col } from "molniya";

// Add column
df.withColumn("new_col", col("col1").add(col("col2")));

// Multiple columns
df.withColumns({
  new_col1: col("col1").mul(2),
  new_col2: col("col2").add(10)
});
```

:::

## Aggregation

::: code-group

```javascript [Danfo.js]
// Simple aggregation
df["col1"].sum();
df["col1"].mean();
df["col1"].count();

// GroupBy
const grouped = df.groupby(["dept"]);
grouped.col(["salary"]).sum();
grouped.col(["salary"]).mean();
```

```typescript [Molniya]
import { sum, avg, count } from "molniya";

// GroupBy
df.groupBy("dept", [
  { name: "total", expr: sum("salary") },
  { name: "average", expr: avg("salary") },
  { name: "count", expr: count() }
]);
```

:::

## Sorting

::: code-group

```javascript [Danfo.js]
// Sort by column
df.sort_values({ by: "age", ascending: true });
df.sort_values({ by: ["dept", "salary"], ascending: [true, false] });
```

```typescript [Molniya]
import { asc, desc } from "molniya";

// Sort
df.sort(asc("age"));
df.sort([asc("dept"), desc("salary")]);
```

:::

## Joining

::: code-group

```javascript [Danfo.js]
// Merge/Join
dfd.merge({ left: df1, right: df2, on: ["id"], how: "inner" });
dfd.merge({ left: df1, right: df2, on: ["id"], how: "left" });
```

```typescript [Molniya]
// Joins
await df1.innerJoin(df2, "id");
await df1.leftJoin(df2, "id");
```

:::

## Missing Values

::: code-group

```javascript [Danfo.js]
// Drop nulls
df.dropna({ axis: 1 });  // Drop columns with nulls
df.dropna({ axis: 0 });  // Drop rows with nulls

// Fill nulls
df.fillna(0);
df["col"].fillna(df["col"].mean());
```

```typescript [Molniya]
// Drop nulls
df.dropNulls();
df.dropNulls("column_name");

// Fill nulls
df.fillNull("column_name", 0);
```

:::

## Mathematical Operations

::: code-group

```javascript [Danfo.js]
// Column operations
df["a"].add(df["b"]);
df["a"].sub(df["b"]);
df["a"].mul(df["b"]);
df["a"].div(df["b"]);

// Statistical
df["col"].mean();
df["col"].std();
df["col"].min();
df["col"].max();
```

```typescript [Molniya]
import { col, avg, min, max, sum } from "molniya";

// Column operations
col("a").add(col("b"));
col("a").sub(col("b"));
col("a").mul(col("b"));
col("a").div(col("b"));

// In aggregations
df.groupBy("key", [
  { name: "mean_val", expr: avg("col") },
  { name: "min_val", expr: min("col") },
  { name: "max_val", expr: max("col") }
]);
```

:::

## String Operations

::: code-group

```javascript [Danfo.js]
// String methods
df["name"].str.split(" ");
df["name"].str.replace("old", "new");
df["name"].str.toLowerCase();
df["name"].str.toUpperCase();
df["name"].str.contains("pattern");
```

```typescript [Molniya]
import { col } from "molniya";

// Limited string operations
col("name").contains("pattern");
col("name").startsWith("prefix");
col("name").endsWith("suffix");

// Note: split, replace, case conversion not yet implemented
```

:::

## Exporting Data

::: code-group

```javascript [Danfo.js]
// To CSV
df.to_csv("output.csv");

// To JSON
df.to_json("output.json");

// To array
const data = df.values;
```

```typescript [Molniya]
// To array
const data = await df.toArray();

// Note: CSV/JSON export not yet implemented
// Use toArray and write manually
const data = await df.toArray();
await Bun.write("output.json", JSON.stringify(data));
```

:::

## Displaying Data

::: code-group

```javascript [Danfo.js]
// Print
df.print();
df.head().print();
df.tail().print();

// Summary
df.describe().print();
```

```typescript [Molniya]
// Print
await df.show();
await df.limit(10).show();

// Schema
await df.printSchema();

// Note: describe() not available
```

:::

## Data Types

Danfo.js uses TensorFlow.js types internally. Molniya has explicit types:

| Danfo.js | Molniya |
|----------|---------|
| `int32` | `DType.int32` |
| `float32` | `DType.float32` |
| `string` | `DType.string` |
| `boolean` | `DType.boolean` |

## Key API Differences

| Danfo.js | Molniya | Notes |
|----------|---------|-------|
| `new dfd.DataFrame(data)` | `fromRecords(data, schema)` | Schema required in Molniya |
| `dfd.read_csv(path)` | `readCsv(path, schema)` | Schema required |
| `dfd.read_json(path)` | Parse + `fromRecords()` | No direct JSON reader |
| `df.head(n)` | `df.limit(n)` | |
| `df.tail(n)` | Not available | Streaming limitation |
| `df.iloc()` | Not available | Use filter/limit |
| `df.loc()` | `df.select()` / `df.filter()` | |
| `df["col"]` | `df.select("col")` | No Series type |
| `df.drop({ axis: 1 })` | `df.drop("col")` | |
| `df.query()` | `df.filter()` | |
| `df.groupby()` | `df.groupBy()` | Different API |
| `df.sort_values()` | `df.sort()` | |
| `df.merge()` | `df.innerJoin()` / `df.leftJoin()` | |
| `df.dropna()` | `df.dropNulls()` | |
| `df.fillna()` | `df.fillNull()` | |
| `df.apply()` | Not available | Use expressions |
| `df.values` | `await df.toArray()` | Async |
| `df.print()` | `await df.show()` | Async |
| `df.describe()` | Not available | |

## Execution Model

The biggest difference is eager vs lazy:

::: code-group

```javascript [Danfo.js]
// Danfo.js executes immediately
const filtered = df.loc({ rows: df["age"].gt(25) });  // Executes now
const sorted = filtered.sort_values({ by: "name" });   // Executes now
```

```typescript [Molniya]
// Molniya builds a plan
const filtered = df.filter(col("age").gt(25));  // Just builds plan
const sorted = filtered.sort(asc("name"));       // Just builds plan

// Execute
await sorted.show();
```

:::

## Best Practices

1. **Define schemas explicitly** - Unlike Danfo.js, Molniya requires upfront schema definition
2. **Use async/await** - All terminal operations return Promises
3. **Filter early** - Take advantage of streaming by filtering before sorting
4. **No Series type** - Work with DataFrames directly
5. **Limited string ops** - String manipulation is more limited than Danfo.js
6. **Check feature availability** - Some Danfo.js features may not be available

## Performance Considerations

| Aspect | Danfo.js | Molniya |
|--------|----------|---------|
| Startup time | Slower (TF.js load) | Fast |
| Memory usage | Higher (tensors) | Lower (TypedArrays) |
| Large files | May OOM | Streams efficiently |
| Type safety | Runtime | Compile-time |
| Bundle size | Large (TF.js) | Small |
