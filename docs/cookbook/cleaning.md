# Data Cleaning Recipes

Real-world data is messy. These recipes show you how to handle common data quality issues.

**Why cleaning matters:**

- Null values break calculations
- Outliers skew statistics
- Inconsistent text causes duplicate categories
- Invalid data causes processing errors

**Strategy:** Clean early in your pipeline, validate after each step.

## Null Handling

Null handling is usually the first step. You need to decide: fill with defaults, remove rows, or keep nulls for later analysis.

### Load with null values

**What this does:** Tells the CSV parser which strings should become `null` instead of empty strings.

```typescript
const result = await readCsv("messy.csv", schema, {
  nullValues: ["NA", "NULL", "n/a", "-", ""],
});
```

### Count nulls per column

```typescript
const nullCounts = {};

df.columnOrder.forEach((col) => {
  const values = df.get(col).toArray();
  const count = values.filter((v) => v === null).length;
  nullCounts[col] = count;
});

console.log(nullCounts);
```

### Remove rows with any null

```typescript
let cleaned = df;

for (const col of df.columnOrder) {
  cleaned = cleaned.filter(col, "!=", null);
}
```

### Fill nulls with value

```typescript
const ages = df.get("age").toArray();
const filled = ages.map((age) => (age === null ? 0 : age));

const withDefaults = DataFrame.fromColumns({ ...df, age: filled }, schema);
```

### Fill nulls with mean

```typescript
const values = df.get("score").toArray();
const validValues = values.filter((v) => v !== null);
const mean = validValues.reduce((a, b) => a + b, 0) / validValues.length;

const filled = values.map((v) => (v === null ? mean : v));
```

## String Cleaning

### Trim whitespace

```typescript
const names = df.get("name").toArray();
const trimmed = names.map((name) => name.trim());

const cleaned = DataFrame.fromColumns({ ...df, name: trimmed }, schema);
```

### Lowercase everything

```typescript
const categories = df.get("category").str.toLowerCase();
```

### Remove special characters

```typescript
const codes = df.get("code").toArray();
const cleaned = codes.map((code) => code.replace(/[^a-zA-Z0-9]/g, ""));
```

### Normalize whitespace

```typescript
const text = df.get("description").toArray();
const normalized = text.map((t) => t.trim().replace(/\s+/g, " "));
```

### Fix encoding issues

```typescript
const names = df.get("name").toArray();
const fixed = names.map((name) =>
  name.replace(/â€™/g, "'").replace(/â€"/g, "—").replace(/Ã©/g, "é"),
);
```

## Numeric Cleaning

### Remove outliers (IQR method)

```typescript
const values = df.get("price").sort(true).toArray();
const q1 = values[Math.floor(values.length * 0.25)];
const q3 = values[Math.floor(values.length * 0.75)];
const iqr = q3 - q1;

const lower = q1 - 1.5 * iqr;
const upper = q3 + 1.5 * iqr;

const cleaned = df.filter("price", ">=", lower).filter("price", "<=", upper);
```

### Cap values

```typescript
const scores = df.get("score").toArray();
const capped = scores.map(
  (s) => Math.min(Math.max(s, 0), 100), // Between 0-100
);
```

### Round decimals

```typescript
const prices = df.get("price").toArray();
const rounded = prices.map(
  (p) => Math.round(p * 100) / 100, // 2 decimals
);
```

### Remove negative values

```typescript
const cleaned = df.filter("amount", ">=", 0);
```

## Date Cleaning

### Parse dates

```typescript
const dates = df.get("date_str").toArray();
const parsed = dates.map((d) => new Date(d));

// Check for invalid dates
const valid = parsed.filter((d) => !isNaN(d.getTime()));
```

### Standardize date format

```typescript
const dates = df.get("date").toArray();
const iso = dates.map((d) => new Date(d).toISOString().split("T")[0]);
```

### Filter by date range

```typescript
const start = new Date("2024-01-01");
const end = new Date("2024-12-31");

const filtered = df.filter("date", ">=", start).filter("date", "<=", end);
```

## Categorical Cleaning

### Standardize categories

```typescript
const categories = df.get("category").toArray();

const standardized = categories.map((cat) => {
  const lower = cat.toLowerCase().trim();

  // Map variations to standard values
  if (lower.includes("electron")) return "Electronics";
  if (lower.includes("cloth")) return "Clothing";
  return cat;
});
```

### Merge rare categories

```typescript
const categories = df.get("category").toArray();

// Count occurrences
const counts = new Map();
categories.forEach((cat) => {
  counts.set(cat, (counts.get(cat) || 0) + 1);
});

// Merge categories with < 10 occurrences
const cleaned = categories.map((cat) => (counts.get(cat) < 10 ? "Other" : cat));
```

### Remove invalid categories

```typescript
const validCategories = new Set(["A", "B", "C"]);

const cleaned = df.filter("category", "in", Array.from(validCategories));
```

## Deduplication

Duplicates happen when data is merged from multiple sources or when systems double-record events.

**Three strategies:**

1. **Exact match:** Remove rows that are 100% identical
2. **Key-based:** Keep first/last row per unique ID
3. **Fuzzy:** Match based on similarity (not shown here)

### Remove exact duplicates

**How it works:** Serialize each row to JSON and use a Set to track what we've seen. Only keep first occurrence of each unique row.

```typescript
const seen = new Set();
const unique = [];

const rows = df.toArray();
rows.forEach((row) => {
  const key = JSON.stringify(row);
  if (!seen.has(key)) {
    seen.add(key);
    unique.push(row);
  }
});

const deduped = DataFrame.fromRows(unique, schema);
```

### Remove duplicates by key

```typescript
const seen = new Set();
const indices = [];

const ids = df.get("id").toArray();
ids.forEach((id, i) => {
  if (!seen.has(id)) {
    seen.add(id);
    indices.push(i);
  }
});

// Create new DataFrame with unique rows
// (Row slicing coming soon)
```

### Keep first/last occurrence

```typescript
const seen = new Map();
const ids = df.get("id").toArray();
const dates = df.get("date").toArray();

ids.forEach((id, i) => {
  if (!seen.has(id) || dates[i] > seen.get(id).date) {
    seen.set(id, { index: i, date: dates[i] });
  }
});

const uniqueIndices = Array.from(seen.values()).map((v) => v.index);
```

## Complete Pipeline

### Full cleaning workflow

```typescript
import { readCsv, DataFrame, DType } from "molniya";

async function cleanData(path: string) {
  // 1. Load with null handling
  const result = await readCsv(path, schema, {
    nullValues: ["NA", "NULL", "", "-"],
  });

  if (!result.ok) {
    throw result.error;
  }

  let df = result.data;

  // 2. Remove rows with critical nulls
  df = df.filter("id", "!=", null);
  df = df.filter("date", "!=", null);

  // 3. Clean strings
  const names = df.get("name").toArray();
  const cleanNames = names.map((n) =>
    n.trim().toLowerCase().replace(/\s+/g, " "),
  );

  // 4. Remove outliers
  df = df
    .filter("age", ">", 0)
    .filter("age", "<", 120)
    .filter("salary", ">", 0);

  // 5. Standardize categories
  const categories = df.get("category").toArray();
  const stdCategories = categories.map((c) => c.toLowerCase().trim());

  // 6. Build cleaned DataFrame
  const cleaned = DataFrame.fromColumns(
    {
      id: df.get("id").toArray(),
      name: cleanNames,
      category: stdCategories,
      age: df.get("age").toArray(),
      salary: df.get("salary").toArray(),
    },
    schema,
  );

  return cleaned;
}

// Use it
const clean = await cleanData("messy.csv");
console.log(`Cleaned: ${clean.shape[0]} rows`);
```

## Validation

### Check for nulls

```typescript
function hasNulls(df: DataFrame, column: string): boolean {
  return df
    .get(column)
    .toArray()
    .some((v) => v === null);
}

if (hasNulls(df, "email")) {
  console.log("Email column has nulls!");
}
```

### Validate ranges

```typescript
function validateRange(
  df: DataFrame,
  column: string,
  min: number,
  max: number,
): boolean {
  const values = df.get(column).toArray();
  return values.every((v) => v >= min && v <= max);
}

if (!validateRange(df, "age", 0, 120)) {
  console.log("Age values out of range!");
}
```

### Check duplicates

```typescript
function hasDuplicates(df: DataFrame, column: string): boolean {
  const values = df.get(column).toArray();
  return new Set(values).size !== values.length;
}

if (hasDuplicates(df, "id")) {
  console.log("Duplicate IDs found!");
}
```

### Validate format

```typescript
function validateEmail(df: DataFrame): boolean {
  const emails = df.get("email").toArray();
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emails.every((e) => emailRegex.test(e));
}
```

## Hot Tips

**Handle nulls at load time:**

```typescript
const result = await readCsv("data.csv", schema, {
  nullValues: ["NA", "NULL", "n/a", "-", ""],
});
```

**Chain cleaning operations:**

```typescript
const cleaned = df
  .filter("age", ">", 0)
  .filter("email", "contains", "@")
  .filter("status", "in", ["active", "pending"]);
```

**Build cleaning functions:**

```typescript
function cleanString(s: string): string {
  return s.trim().toLowerCase().replace(/\s+/g, " ");
}

const names = df.get("name").toArray().map(cleanString);
```

**Validate before saving:**

```typescript
if (df.isEmpty()) {
  console.error("No data to save!");
} else if (hasNulls(df, "id")) {
  console.error("ID column has nulls!");
} else {
  await writeCsv(df, "clean.csv");
}
```

**Keep original data:**

```typescript
const original = df; // Keep reference
let cleaned = df.filter("age", ">", 0).filter("salary", ">", 0);

console.log(`Removed ${original.shape[0] - cleaned.shape[0]} rows`);
```
