# From Arquero to Molniya

Arquero is a popular JavaScript data table library. This guide shows you how Arquero operations map to Molniya.

## Key Differences

| Aspect             | Arquero           | Molniya                            |
| ------------------ | ----------------- | ---------------------------------- |
| **Type System**    | Runtime inference | Explicit schemas required          |
| **Filtering**      | Function-based    | Operator-based                     |
| **Error Handling** | Exceptions        | Result type                        |
| **Performance**    | Good              | Better for large files (LazyFrame) |
| **API Style**      | Verb-based        | Method chaining                    |
| **Null Safety**    | JavaScript nulls  | Explicit null handling             |
| **Data Loading**   | Automatic parsing | Schema-driven parsing              |

## Common Operations

### Loading CSV

**Arquero:**

```javascript
import { fromCSV } from "arquero";

const dt = await fromCSV("data.csv");
```

**Molniya:**

```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("data.csv", {
  id: DType.Int32,
  name: DType.String,
  value: DType.Float64,
});

const df = result.ok ? result.data : null;
```

**What changed:**

- Must provide explicit schema
- Returns Result type
- More type safety

---

### Filtering

**Arquero:**

```javascript
// Function-based filtering
const filtered = dt.filter((d) => d.age > 25);

// Multiple conditions
const filtered = dt.filter((d) => d.age > 25 && d.status === "active");

// String operations
const filtered = dt.filter((d) => d.name.startsWith("A"));
```

**Molniya:**

```typescript
// Operator-based filtering
const filtered = df.filter("age", ">", 25);

// Multiple conditions (chained)
const filtered = df.filter("age", ">", 25).filter("status", "==", "active");

// String operations
const filtered = df.filter("name", "startsWith", "A");
```

**What changed:**

- No functions - use operators as strings
- Chain for multiple conditions
- Built-in string operators

---

### Selecting Columns

**Arquero:**

```javascript
// Select with varargs
const subset = dt.select("name", "age", "email");

// Select with not() to exclude
const subset = dt.select(not("internal_id"));
```

**Molniya:**

```typescript
// Select with array
const subset = df.select(["name", "age", "email"]);

// Exclude by filtering column list
const cols = df.columnOrder.filter((c) => c !== "internal_id");
const subset = df.select(cols);
```

**What changed:**

- Array instead of varargs
- No built-in `not()` helper (use array methods)

---

### Deriving Columns

**Arquero:**

```javascript
const derived = dt.derive({
  total: (d) => d.price * d.quantity,
  category: (d) => (d.age >= 18 ? "Adult" : "Minor"),
});
```

**Molniya:**

```typescript
const prices = df.get("price").toArray();
const quantities = df.get("quantity").toArray();
const ages = df.get("age").toArray();

const totals = prices.map((p, i) => p * quantities[i]);
const categories = ages.map((age) => (age >= 18 ? "Adult" : "Minor"));

const derived = DataFrame.fromColumns(
  {
    price: prices,
    quantity: quantities,
    age: ages,
    total: totals,
    category: categories,
  },
  {
    price: DType.Float64,
    quantity: DType.Int32,
    age: DType.Int32,
    total: DType.Float64,
    category: DType.String,
  },
);
```

**What changed:**

- More explicit: get arrays, map them, create new DataFrame
- Must specify types for new columns
- More verbose but full JavaScript power

---

### Sorting

**Arquero:**

```javascript
// Ascending
const sorted = dt.orderby("age");

// Descending
const sorted = dt.orderby(desc("age"));

// Multiple columns
const sorted = dt.orderby("dept", desc("salary"));
```

**Molniya:**

```typescript
// Ascending
const sorted = df.sortBy("age", true);

// Descending
const sorted = df.sortBy("age", false);

// Multiple columns (chain)
const sorted = df.sortBy("dept", true).sortBy("salary", false);
```

**What changed:**

- `sortBy()` instead of `orderby()`
- Boolean for direction
- Chain for multiple columns

---

### Aggregation

**Arquero:**

```javascript
const summary = dt.rollup({
  total: (d) => op.sum(d.revenue),
  avg: (d) => op.mean(d.price),
  count: (d) => op.count(),
});
```

**Molniya:**

```typescript
const revenues = df.get("revenue").toArray();
const prices = df.get("price").toArray();

const summary = {
  total: revenues.reduce((a, b) => a + b, 0),
  avg: prices.reduce((a, b) => a + b, 0) / prices.length,
  count: df.count(),
};
```

**What changed:**

- Manual aggregation with reduce/Series methods
- More explicit, leverages JavaScript
- No op.\* functions needed

---

### Group By

**Arquero:**

```javascript
const grouped = dt.groupby("category").rollup({
  total: (d) => op.sum(d.revenue),
  count: (d) => op.count(),
});
```

**Molniya:**

```typescript
const categories = df.get("category").toArray();
const revenues = df.get("revenue").toArray();

const grouped = new Map();
categories.forEach((cat, i) => {
  if (!grouped.has(cat)) {
    grouped.set(cat, { total: 0, count: 0 });
  }
  const g = grouped.get(cat);
  g.total += revenues[i];
  g.count += 1;
});

const result = Object.fromEntries(grouped);
```

**What changed:**

- Manual grouping with Map
- More control over aggregation logic
- Can use LazyFrame for simpler syntax:

```typescript
const result = await LazyFrame.scanCsv("data.csv", schema)
  .groupby(["category"], [{ column: "revenue", agg: "sum", alias: "total" }])
  .collect();
```

---

### Joining Tables

**Arquero:**

```javascript
const joined = dt1.join(dt2, "id");
```

**Molniya:**

```typescript
// Manual join (join API coming soon)
const ids1 = df1.get("id").toArray();
const ids2 = df2.get("id").toArray();
const data2 = df2.get("value").toArray();

// Create lookup
const lookup = new Map(ids2.map((id, i) => [id, data2[i]]));

// Map values
const joined = ids1.map((id) => ({
  id,
  value: lookup.get(id),
}));
```

**Note:** Join API is planned for future release.

---

### String Operations

**Arquero:**

```javascript
const cleaned = dt.derive({
  name_lower: (d) => op.lower(d.name),
  email_clean: (d) => op.trim(d.email),
});
```

**Molniya:**

```typescript
const names = df.get("name").str.toLowerCase();
const emails = df.get("email").str.trim();

const cleaned = DataFrame.fromColumns(
  {
    name: df.get("name").toArray(),
    email: df.get("email").toArray(),
    name_lower: names.toArray(),
    email_clean: emails.toArray(),
  },
  schema,
);
```

**What changed:**

- Series has `.str` accessor with methods
- Manual DataFrame creation
- More explicit transformations

---

### Head/Tail

**Arquero:**

```javascript
const first = dt.slice(0, 10);
const last = dt.slice(-5);
```

**Molniya:**

```typescript
const first = df.head(10);
const last = df.tail(5);
```

**Similar!** Molniya has dedicated head/tail methods.

## Type Safety Comparison

**Arquero:**

```javascript
// No schema needed
const dt = await fromCSV("data.csv");

// Access any property
dt.filter((d) => d.anything); // No error at compile time
```

**Molniya:**

```typescript
// Schema required
const result = await readCsv("data.csv", {
  name: DType.String,
  age: DType.Int32,
});

// TypeScript knows the types
const names = df.get("name"); // Series<string>
const ages = df.get("age"); // Series<number>

// This would error at compile time:
// df.get("nonexistent");
```

**Trade-off:** Molniya is more verbose but catches errors earlier.

## Error Handling

**Arquero:**

```javascript
try {
  const dt = await fromCSV("data.csv");
  // Use dt
} catch (error) {
  console.error(error);
}
```

**Molniya:**

```typescript
const result = await readCsv("data.csv", schema);

if (!result.ok) {
  console.error(result.error.message);
  return;
}

const df = result.data;
// Use df (guaranteed to be valid here)
```

**Molniya makes errors explicit** - you can't forget to check.

## Performance: Large Files

**Arquero:**

```javascript
// Loads entire file
const dt = await fromCSV("huge.csv");
const filtered = dt.filter((d) => d.status === "active");
```

**Molniya (LazyFrame):**

```typescript
// Filters during CSV scan - much faster!
const result = await LazyFrame.scanCsv("huge.csv", schema)
  .filter("status", "==", "active")
  .collect();
```

**For files > 10MB, Molniya can be 10-100x faster** thanks to predicate pushdown.

## Migration Checklist

- ☐ Create schemas for all your data
- ☐ Replace `filter(d => ...)` with `.filter("col", "op", val)`
- ☐ Change `select('col1', 'col2')` to `.select(['col1', 'col2'])`
- ☐ Rewrite `derive()` as map operations + DataFrame creation
- ☐ Replace `orderby()` with `sortBy()`
- ☐ Update `rollup()` to use reduce/Series aggregations
- ☐ Add Result type checks to all I/O operations
- ☐ Use LazyFrame for large files

## Common Patterns

### Data Cleaning Pipeline

**Arquero:**

```javascript
const cleaned = dt
  .filter((d) => d.age > 0)
  .derive({ name: (d) => d.name.toLowerCase() })
  .dedupe("id");
```

**Molniya:**

```typescript
const filtered = df.filter("age", ">", 0);
const names = filtered.get("name").str.toLowerCase();

// Dedupe manually
const seen = new Set();
const rows = filtered.toArray();
const unique = rows.filter((row) => {
  if (seen.has(row.id)) return false;
  seen.add(row.id);
  return true;
});

const cleaned = DataFrame.fromRows(unique, schema);
```

### Aggregation Summary

**Arquero:**

```javascript
const summary = dt.groupby("category").rollup({
  count: op.count(),
  total: (d) => op.sum(d.revenue),
});
```

**Molniya:**

```typescript
const categories = df.get("category").toArray();
const revenues = df.get("revenue").toArray();

const summary = new Map();
categories.forEach((cat, i) => {
  if (!summary.has(cat)) {
    summary.set(cat, { count: 0, total: 0 });
  }
  const s = summary.get(cat);
  s.count += 1;
  s.total += revenues[i];
});
```

## Need Help?

- [Cookbook](/cookbook/) - Common recipes
- [DataFrame API](/api/dataframe) - All methods
- [LazyFrame Guide](/guide/lazy-evaluation) - Performance tips
