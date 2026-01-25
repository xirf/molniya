# Series Recipes

A Series is a single typed column from a DataFrame. It's like an array but knows its data type and has domain-specific operations.

**Key difference from arrays:**

- Series has type info (DType)
- Special accessors like `.str` for string operations
- Methods like `.sum()`, `.mean()` for stats
- Immutable - operations return new Series

## Getting a Series

Use `.get()` on a DataFrame to extract a column as a Series.

```typescript
const df = DataFrame.fromColumns(
  {
    name: ["Alice", "Bob", "Carol"],
    age: [25, 30, 35],
  },
  {
    name: DType.String,
    age: DType.Int32,
  },
);

const names = df.get("name"); // Series<string>
const ages = df.get("age"); // Series<number>
```

## String Operations

String Series have a special `.str` accessor with text manipulation methods. These operations return new Series - the original is unchanged.

### Lowercase/Uppercase

**When to use:** Normalizing text for comparison, cleaning user input, standardizing data.

```typescript
const names = df.get("name");

const lower = names.str.toLowerCase();
// ["alice", "bob", "carol"]

const upper = names.str.toUpperCase();
// ["ALICE", "BOB", "CAROL"]
```

### Trim whitespace

```typescript
const messy = Series.from(["  Alice  ", " Bob ", "Carol"], DType.String);

const clean = messy.str.trim();
// ["Alice", "Bob", "Carol"]

const alsoClean = messy.str.strip(); // Alias for trim
```

### Replace text

```typescript
const emails = Series.from(["alice@old.com", "bob@old.com"], DType.String);

const updated = emails.str.replace("old.com", "new.com");
// ["alice@new.com", "bob@new.com"]
```

### Split strings

```typescript
const paths = Series.from(["a/b/c", "x/y/z"], DType.String);

const parts = paths.str.split("/");
// [["a", "b", "c"], ["x", "y", "z"]]

// Get first part
const first = parts.map((p) => p[0]); // ["a", "x"]
```

### Check string patterns

```typescript
const emails = Series.from(
  ["alice@company.com", "bob@other.org"],
  DType.String,
);

// Contains
const hasCompany = emails.str.contains("company");
// [true, false]

// Starts with
const startsWithAlice = emails.str.startsWith("alice");
// [true, false]

// Ends with
const isDotCom = emails.str.endsWith(".com");
// [true, false]
```

### String lengths

```typescript
const words = Series.from(["cat", "dog", "elephant"], DType.String);

const lengths = words.str.length();
// [3, 3, 8]
```

## Numeric Operations

### Sum

```typescript
const prices = Series.from([10.5, 20.3, 30.1], DType.Float64);

const total = prices.sum();
// 60.9
```

### Average

```typescript
const scores = Series.from([85, 90, 78, 92], DType.Int32);

const avg = scores.mean();
// 86.25
```

### Min/Max

```typescript
const values = Series.from([5, 2, 9, 1, 7], DType.Int32);

const min = values.min(); // 1
const max = values.max(); // 9
```

### Unique values

```typescript
const categories = Series.from(["A", "B", "A", "C", "B", "A"], DType.String);

const unique = categories.unique();
// ["A", "B", "C"]
```

## Transformations

### Filter

```typescript
const numbers = Series.from([1, 2, 3, 4, 5], DType.Int32);

const evens = numbers.filter((n) => n % 2 === 0);
// Series([2, 4])
```

### Sort

```typescript
const scores = Series.from([85, 92, 78, 90], DType.Int32);

const ascending = scores.sort(true);
// [78, 85, 90, 92]

const descending = scores.sort(false);
// [92, 90, 85, 78]
```

### Get/Set values

```typescript
const series = Series.from([1, 2, 3], DType.Int32);

const first = series.get(0); // 1
const last = series.get(series.length - 1); // 3

const updated = series.set(0, 10);
// Series([10, 2, 3])
```

## Null Handling

### Check for nulls

```typescript
const data = Series.from([1, null, 3, null, 5], DType.Int32);

const hasNull = data.isNull(1); // true
const noNull = data.isNull(0); // false
```

### Fill nulls

```typescript
const data = Series.from([1, null, 3, null, 5], DType.Int32);

const filled = data.fillNull(0);
// [1, 0, 3, 0, 5]
```

### Drop nulls

```typescript
const data = Series.from([1, null, 3, null, 5], DType.Int32);

const clean = data.dropNull();
// [1, 3, 5]
```

## Conversions

### To array

```typescript
const series = df.get("age");
const array = series.toArray();
// [25, 30, 35]
```

### Create from array

```typescript
const array = [1, 2, 3, 4, 5];
const series = Series.from(array, DType.Int32);
```

## Common Patterns

### Clean email domains

```typescript
const emails = df.get("email");

const domains = emails
  .toArray()
  .map((email) => email.split("@")[1])
  .map((domain) => domain.toLowerCase());

console.log(new Set(domains)); // Unique domains
```

### Normalize names

```typescript
const names = df.get("name");

const normalized = names.str.toLowerCase().trim().replace(/\s+/g, " "); // Replace multiple spaces with one
```

### Extract initials

```typescript
const names = Series.from(["Alice Smith", "Bob Jones"], DType.String);

const initials = names
  .toArray()
  .map((name) => name.split(" "))
  .map((parts) => parts.map((p) => p[0]).join(""));
// ["AS", "BJ"]
```

### Calculate percentages

```typescript
const scores = df.get("score");
const maxScore = scores.max();

const percentages = scores.toArray().map((score) => (score / maxScore) * 100);
```

### Flag outliers

```typescript
const values = df.get("value");
const mean = values.mean();
const max = values.max();

const outliers = values
  .toArray()
  .map((v) => (v > mean * 2 ? "outlier" : "normal"));
```

### Categorize by range

```typescript
const ages = df.get("age");

const categories = ages
  .toArray()
  .map((age) => (age < 18 ? "Minor" : age < 65 ? "Adult" : "Senior"));
```

## String Recipes

### Email validation

```typescript
const emails = df.get("email");

const valid = emails
  .toArray()
  .filter((email) => email.includes("@") && email.includes("."));
```

### Extract usernames

```typescript
const emails = df.get("email");

const usernames = emails.toArray().map((email) => email.split("@")[0]);
```

### Format phone numbers

```typescript
const phones = Series.from(["1234567890", "9876543210"], DType.String);

const formatted = phones
  .toArray()
  .map((p) => `(${p.slice(0, 3)}) ${p.slice(3, 6)}-${p.slice(6)}`);
// ["(123) 456-7890", "(987) 654-3210"]
```

### Remove special characters

```typescript
const text = Series.from(["Hello, World!", "Test@123"], DType.String);

const clean = text.toArray().map((s) => s.replace(/[^a-zA-Z0-9]/g, ""));
// ["HelloWorld", "Test123"]
```

## Stats Recipes

### Count occurrences

```typescript
const categories = df.get("category");

const counts = new Map();
categories.toArray().forEach((cat) => {
  counts.set(cat, (counts.get(cat) || 0) + 1);
});

console.log(Object.fromEntries(counts));
```

### Percentiles

```typescript
const values = df.get("score").sort(true).toArray();

const median = values[Math.floor(values.length / 2)];
const p25 = values[Math.floor(values.length * 0.25)];
const p75 = values[Math.floor(values.length * 0.75)];

console.log({ p25, median, p75 });
```

### Standard deviation

```typescript
const values = df.get("value").toArray();
const mean = values.reduce((a, b) => a + b, 0) / values.length;
const variance =
  values.map((v) => Math.pow(v - mean, 2)).reduce((a, b) => a + b, 0) /
  values.length;
const stdDev = Math.sqrt(variance);
```

### Running total

```typescript
const amounts = df.get("amount").toArray();

let cumsum = 0;
const runningTotal = amounts.map((amt) => (cumsum += amt));
// [10, 30, 60, 100, ...] for [10, 20, 30, 40, ...]
```

## Hot Tips

**Chain string operations:**

```typescript
const clean = df.get("name").str.toLowerCase().trim().replace("_", " ");
```

**Combine with DataFrame filters:**

```typescript
const emails = df.get("email");
const companyEmails = emails
  .toArray()
  .filter((_, i) => emails.str.endsWith("@company.com")[i]);
```

**Type-safe operations:**

```typescript
const strings = df.get("name"); // Series<string>
strings.str.toLowerCase(); // ✅ OK

const numbers = df.get("age"); // Series<number>
numbers.sum(); // ✅ OK
// numbers.str.toLowerCase();    // ❌ Type error
```

**Reuse series:**

```typescript
const scores = df.get("score");
const avg = scores.mean();
const max = scores.max();
const top = scores.filter((s) => s > avg);
```
