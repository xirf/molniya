# Loading Data

Getting data into Molniya is the first step of any analysis. We support CSV, JSON, and plain JavaScript objects.

## Reading CSV Files

The most common way to load data is from a CSV file.

```typescript
import { readCsv } from "molniya";

// 1. Basic load
const { df } = await readCsv("./transactions.csv");

// 2. Check what you loaded
df.print();
```

### Configuration Options

Real-world CSVs are rarely perfect. Molniya gives you fine-grained control over how they are parsed.

```typescript
const { df, errors } = await readCsv("./raw_export.csv", {
  // Parsing details
  delimiter: ";", // European-style CSVs often use semicolons
  hasHeader: true, // Set to false if the file starts directly with data
  encoding: "utf-8", // Default encoding

  // Performance & Safety
  maxRows: 10000, // Stop reading after N rows (great for previews)
  sampleRows: 100, // Browse 100 rows to guess the column types

  // Custom parsing
  nullValues: ["NA", "null", "-"], // Treat these strings as missing data

  // Datetime Handling (Crucial!)
  datetime: {
    defaultZone: "UTC", // Assume UTC if no timezone is in the string
    columns: {
      // Precise control per column
      timestamp: { format: "iso" }, // efficient ISO-8601 parsing
      audit_date: { format: "sql" }, // YYYY-MM-DD HH:mm:ss
      login_unix: { format: "unix-s" }, // Unix timestamp (seconds)
    },
  },
});
```

> [!WARNING]
> **Check for Errors!**
> The `readCsv` function returns an object `{ df, errors }`. Always check `errors` if your data looks weird. It contains parsing warnings (like malformed rows).

---

## Loading from JSON

JSON comes in many shapes. Molniya likes **arrays of objects**.

```typescript
const jsonData = [
  { id: 1, name: "Widget A", cost: 10.5 },
  { id: 2, name: "Widget B", cost: 20.0 },
  { id: 3, name: "Widget C", cost: null }, // Handles nulls gracefully
];

const df = DataFrame.fromObjects(jsonData);
```

If your JSON is nested (e.g., from an API response), map it to a flat structure first:

```typescript
const apiResponse = {
  status: "ok",
  data: [ ... ] // The actual array
};

const df = DataFrame.fromObjects(apiResponse.data);
```

---

## Creating Manually (Columnar)

For maximum performance or hardcoded data, create DataFrames column-by-column. This skips the row-parsing overhead.

```typescript
const df = DataFrame.fromColumns({
  labels: ["A", "B", "C"],
  values: [100, 200, 300],
  is_valid: [true, false, true],
});
```

---

## Validating Your Data

Once data is loaded, you should trust but verify.

### 1. Inspect the Shape

```typescript
console.log(df.shape); // [1000, 15] -> 1000 rows, 15 columns
```

### 2. Check Column Types

Before running math operations, ensure your 'price' column is actually numbers, not strings.

```typescript
df.printSchema();
// Output:
// name: string
// age: float64
// salary: float64
```

### 3. Peek at the Data

Don't print 1 million rows to your console.

```typescript
df.head(5).print(); // First 5 rows
df.tail(5).print(); // Last 5 rows
```

### 4. Handle Missing Data

Cleanup is often necessary immediately after simple loading.

```typescript
// Drop any row containing a null value
const clean = df.dropna();

// Or fill nulls with a default
const filled = df.fillna({
  score: 0, // Fill 'score' nulls with 0
  category: "Unknown", // Fill 'category' nulls with 'Unknown'
});
```

