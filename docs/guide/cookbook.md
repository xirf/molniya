# Cookbook

Quick recipes for common tasks. Copy, paste, and adapt!

## Quick Wins

### Load a CSV

```typescript
import { readCsv, DType } from "molniya";

const result = await readCsv("data.csv", {
  name: DType.String,
  age: DType.Int32,
  score: DType.Float64,
});

if (result.ok) {
  console.log(result.data.toString());
}
```

### Filter like a pro

```typescript
// Age 25-35 AND high salary
const adults = df
  .filter("age", ">=", 25)
  .filter("age", "<=", 35)
  .filter("salary", ">", 50000);
```

### Top 10 anything

```typescript
const top10 = df.sortBy("revenue", false).head(10);
```

## One-Liners

### Get unique values

```typescript
const unique = new Set(df.get("category").toArray());
console.log(`${unique.size} categories`);
```

### Quick stats

```typescript
const values = df.get("price").toArray();
const avg = values.reduce((a, b) => a + b, 0) / values.length;
console.log(`Average: $${avg.toFixed(2)}`);
```

### String cleanup

```typescript
const clean = df.get("name").str.toLowerCase().trim();
```

## Common Patterns

### Handle missing data

```typescript
const result = await readCsv("messy.csv", schema, {
  nullValues: ["NA", "NULL", "-", ""],
});

const validRows = df.filter("age", ">", 0); // Remove nulls
```

### Add calculated column

```typescript
const prices = df.get("price").toArray();
const qty = df.get("quantity").toArray();
const totals = prices.map((p, i) => p * qty[i]);

const withTotal = DataFrame.fromColumns(
  { ...df, total: totals },
  { ...schema, total: DType.Float64 },
);
```

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

## Speed Tricks

### Use LazyFrame for big files

```typescript
import { LazyFrame } from "molniya";

// Filters DURING scan
const result = await LazyFrame.scanCsv("huge.csv", schema)
  .filter("status", "==", "active")
  .select(["id", "name"])
  .collect();
```

### Load only what you need

```typescript
const result = await readCsv("wide.csv", {
  id: DType.Int32,
  name: DType.String,
  // Skip 47 other columns!
});
```

### Process in chunks

```typescript
const result = await LazyFrame.scanCsv("giant.csv", schema, {
  chunkSize: 5000, // Low memory mode
}).collect();
```

## Hot Tips

**Filter first, then select**

```typescript
// ✅ Good
df.filter("active", "==", true).select(["id", "name"]);

// ❌ Wasteful
df.select(["id", "name", "active"]).filter("active", "==", true);
```

**String searches are easy**

```typescript
df.filter("email", "endsWith", "@company.com");
df.filter("name", "contains", "smith");
df.filter("code", "startsWith", "ABC");
```

**Chain like crazy**

```typescript
const result = df
  .filter("status", "==", "active")
  .filter("age", ">=", 18)
  .sortBy("score", false)
  .head(100);
```
