# DataFrame Recipes

DataFrames are tables with typed columns. Think of them like a spreadsheet or SQL table in your code.

**When to use DataFrames:**

- Your data fits in memory (< 1GB typically)
- You need to do complex operations (sorts, transforms)
- File is already loaded or data comes from API

**When to use LazyFrame instead:**

- File > 10MB and you only need part of it
- You're filtering or selecting columns (optimized for performance)

## Creating DataFrames

### From arrays (column-wise)

Use this when you have data organized by columns (common when building data programmatically).

```typescript
import { DataFrame, DType } from "molniya";

const df = DataFrame.fromColumns(
  {
    name: ["Alice", "Bob", "Carol"],
    age: [25, 30, 35],
    salary: [50000, 60000, 75000],
  },
  {
    name: DType.String,
    age: DType.Int32,
    salary: DType.Float64,
  },
);
```

### From objects (row-wise)

```typescript
const df = DataFrame.fromRows(
  [
    { name: "Alice", age: 25, city: "NYC" },
    { name: "Bob", age: 30, city: "LA" },
  ],
  {
    name: DType.String,
    age: DType.Int32,
    city: DType.String,
  },
);
```

## Filtering

Filtering creates a new DataFrame with only rows that match your conditions. The original DataFrame is never modified.

### Single condition

**How it works:** Checks each row's value in the specified column against your condition.

```typescript
const adults = df.filter("age", ">=", 18);
const highEarners = df.filter("salary", ">", 70000);
```

### Multiple conditions (AND)

**Why chain filters:** Each `.filter()` call returns a new DataFrame, so you can chain them. All conditions must be true (AND logic).

```typescript
const result = df
  .filter("age", ">=", 25)
  .filter("age", "<=", 35)
  .filter("department", "==", "Engineering");
```

### String matching

```typescript
// Starts with
const aNames = df.filter("name", "startsWith", "A");

// Contains
const hasDot = df.filter("email", "contains", ".");

// Ends with
const dotCom = df.filter("email", "endsWith", ".com");
```

### Array membership

```typescript
const techDepts = df.filter("department", "in", [
  "Engineering",
  "Data Science",
  "DevOps",
]);
```

## Selecting Columns

### Pick specific columns

```typescript
const subset = df.select(["name", "email", "department"]);
```

### Drop columns

```typescript
const cleaned = df.drop(["temp_column", "debug_info"]);
```

### Columns by pattern

```typescript
const salesCols = df.columnOrder.filter((col) => col.startsWith("sales_"));
const salesData = df.select(salesCols);
```

## Sorting

### Sort ascending

```typescript
const byAge = df.sortBy("age", true);
```

### Sort descending

```typescript
const byRevenue = df.sortBy("revenue", false);
```

### Multi-level sort

```typescript
// Sort by dept, then salary within each dept
const sorted = df.sortBy("department", true).sortBy("salary", false);
```

## Slicing & Sampling

### First N rows

```typescript
const first10 = df.head(10);
```

### Last N rows

```typescript
const last5 = df.tail(5);
```

### Top N by value

```typescript
const topSalaries = df.sortBy("salary", false).head(10);
```

## Adding Columns

### Calculated column

```typescript
const prices = df.get("price").toArray();
const qty = df.get("quantity").toArray();
const totals = prices.map((p, i) => p * qty[i]);

const withTotal = DataFrame.fromColumns(
  {
    price: prices,
    quantity: qty,
    total: totals,
  },
  {
    price: DType.Float64,
    quantity: DType.Int32,
    total: DType.Float64,
  },
);
```

### Conditional column

```typescript
const ages = df.get("age").toArray();
const categories = ages.map((age) =>
  age < 18 ? "Minor" : age < 65 ? "Adult" : "Senior",
);

const withCategory = DataFrame.fromColumns(
  { ...df, category: categories },
  { ...schema, category: DType.String },
);
```

## Transformations

### Rename columns

```typescript
const nameMap = df.get("old_name").toArray();
const ageMap = df.get("old_age").toArray();

const renamed = DataFrame.fromColumns(
  { name: nameMap, age: ageMap },
  { name: DType.String, age: DType.Int32 },
);
```

### Map values

```typescript
const categories = df.get("category").toArray();
const cleaned = categories.map((cat) =>
  cat.toLowerCase().trim().replace(/\s+/g, "_"),
);
```

## Aggregations

Aggregations reduce your data to summary statistics. Since Series methods work on arrays, you often call `.toArray()` first.

### Count rows

**What this does:** Returns total number of rows in the DataFrame.

```typescript
const totalRows = df.count();
```

### Count unique values

**How it works:** JavaScript's `Set` automatically removes duplicates, so its size = unique count.

```typescript
const categories = df.get("category").toArray();
const uniqueCount = new Set(categories).size;
```

### Sum column

```typescript
const total = df
  .get("revenue")
  .toArray()
  .reduce((a, b) => a + b, 0);
```

### Average

```typescript
const values = df.get("score").toArray();
const avg = values.reduce((a, b) => a + b, 0) / values.length;
```

### Min/Max

```typescript
const prices = df.get("price").toArray();
const min = Math.min(...prices);
const max = Math.max(...prices);
```

## Group By

### Group and sum

```typescript
const categories = df.get("category").toArray();
const amounts = df.get("amount").toArray();

const totals = new Map();
categories.forEach((cat, i) => {
  totals.set(cat, (totals.get(cat) || 0) + amounts[i]);
});

console.log(Object.fromEntries(totals));
```

### Group and count

```typescript
const counts = new Map();
categories.forEach((cat) => {
  counts.set(cat, (counts.get(cat) || 0) + 1);
});
```

### Group multiple aggregations

```typescript
const groups = new Map();
categories.forEach((cat, i) => {
  if (!groups.has(cat)) {
    groups.set(cat, { sum: 0, count: 0 });
  }
  const g = groups.get(cat);
  g.sum += amounts[i];
  g.count += 1;
});

// Calculate averages
for (const [cat, stats] of groups) {
  console.log(`${cat}: avg = ${stats.sum / stats.count}`);
}
```

## Combining DataFrames

### Concat rows (same columns)

```typescript
import { concat } from "molniya";

const jan = await readCsv("jan.csv", schema);
const feb = await readCsv("feb.csv", schema);

if (jan.ok && feb.ok) {
  const combined = concat([jan.data, feb.data]);
}
```

### Merge (join)

```typescript
const userIds = users.get("id").toArray();
const names = users.get("name").toArray();
const orderUserIds = orders.get("user_id").toArray();

// Create lookup
const nameMap = new Map(userIds.map((id, i) => [id, names[i]]));

// Add names to orders
const enriched = orderUserIds.map((uid) => ({
  user_id: uid,
  name: nameMap.get(uid) || "Unknown",
}));
```

## Inspection

### Shape

```typescript
const [rows, cols] = df.shape;
console.log(`${rows} × ${cols}`);
```

### Column names

```typescript
const columns = df.columnOrder;
console.log(`Columns: ${columns.join(", ")}`);
```

### Data types

```typescript
const types = df.dtypes;
console.log(types);
```

### Preview

```typescript
console.log(df.toString()); // First few rows
console.log(df.head(20).toString()); // First 20 rows
```

### Check if empty

```typescript
if (df.isEmpty()) {
  console.log("No data!");
}
```

## Export

### To CSV

```typescript
import { writeCsv } from "molniya";

await writeCsv(df, "output.csv");
```

### To array of objects

```typescript
const rows = df.toArray();
console.log(rows);
// [{ name: "Alice", age: 25 }, ...]
```

### To JSON string

```typescript
const json = JSON.stringify(df.toArray(), null, 2);
console.log(json);
```

## Hot Tips

**Chain everything:**

```typescript
const result = df
  .filter("status", "==", "active")
  .select(["id", "name", "score"])
  .sortBy("score", false)
  .head(100);
```

**Reuse filtered results:**

```typescript
const active = df.filter("active", "==", true);
const topActive = active.sortBy("score", false).head(10);
const avgScore =
  active
    .get("score")
    .toArray()
    .reduce((a, b) => a + b, 0) / active.count();
```

**Check before operating:**

```typescript
if (!df.isEmpty() && df.columnOrder.includes("score")) {
  const top = df.sortBy("score", false).head(10);
}
```
