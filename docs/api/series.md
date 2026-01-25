# Series

A Series represents a single column of typed data. It provides operations for searching, sorting, and transforming values.

## Overview

Series is created when you get a column from a DataFrame:

```typescript
const df = DataFrame.fromColumns(
  { name: ["Alice", "Bob"], age: [25, 30] },
  { name: DType.String, age: DType.Int32 },
);

const names = df.get("name"); // Series<string>
const ages = df.get("age"); // Series<number>
```

## Creating Series

### from

Create a Series from an array.

```typescript
import { Series, DType } from "molniya";

const numbers = Series.from([1, 2, 3, 4, 5], DType.Int32);
const texts = Series.from(["a", "b", "c"], DType.String);
```

**Parameters:**

- `data: T[]` - Array of values
- `dtype: DType` - Data type

**Returns:** `Series<T>`

## Properties

### length

Number of elements.

```typescript
const count = series.length;
```

### dtype

Data type of the series.

```typescript
const type = series.dtype;
// DType.String, DType.Int32, etc.
```

### name

Column name (if from DataFrame).

```typescript
const colName = series.name;
```

## Basic Operations

### toArray

Convert to JavaScript array.

```typescript
const values = series.toArray();
// [1, 2, 3, 4, 5]
```

**Returns:** `T[]` - Array of values

### get

Get value at index.

```typescript
const value = series.get(0); // First value
const last = series.get(series.length - 1); // Last value
```

**Parameters:**

- `index: number` - Position (0-based)

**Returns:** `T | null` - Value or null if out of bounds

### set

Set value at index (returns new Series).

```typescript
const updated = series.set(0, newValue);
```

**Parameters:**

- `index: number` - Position
- `value: T` - New value

**Returns:** `Series<T>` - New series with updated value

## Sorting & Filtering

### sort

Sort values.

```typescript
const ascending = series.sort(true);
const descending = series.sort(false);
```

**Parameters:**

- `ascending: boolean` - Sort direction

**Returns:** `Series<T>` - Sorted series

### filter

Filter by condition.

```typescript
const numbers = Series.from([1, 2, 3, 4, 5], DType.Int32);
const evens = numbers.filter((n) => n % 2 === 0);
```

**Parameters:**

- `predicate: (value: T) => boolean` - Filter function

**Returns:** `Series<T>` - Filtered series

## Null Handling

### isNull

Check for null values.

```typescript
const hasNull = series.isNull(index);
```

**Parameters:**

- `index: number` - Position to check

**Returns:** `boolean`

### fillNull

Replace null values.

```typescript
const filled = series.fillNull(0); // Replace with 0
```

**Parameters:**

- `value: T` - Replacement value

**Returns:** `Series<T>` - Series with nulls filled

### dropNull

Remove null values.

```typescript
const noNulls = series.dropNull();
```

**Returns:** `Series<T>` - Series without nulls

## String Operations

Series with `DType.String` have a `str` accessor:

### str.toLowerCase

Convert to lowercase.

```typescript
const names = Series.from(["Alice", "BOB"], DType.String);
const lower = names.str.toLowerCase();
// ["alice", "bob"]
```

### str.toUpperCase

Convert to uppercase.

```typescript
const upper = names.str.toUpperCase();
// ["ALICE", "BOB"]
```

### str.trim

Remove whitespace.

```typescript
const texts = Series.from(["  hello  ", " world "], DType.String);
const trimmed = texts.str.trim();
// ["hello", "world"]
```

### str.strip

Alias for trim.

```typescript
const stripped = texts.str.strip();
```

### str.contains

Check if strings contain substring.

```typescript
const emails = Series.from(
  ["alice@example.com", "bob@company.org"],
  DType.String,
);
const hasExample = emails.str.contains("example");
// [true, false]
```

**Parameters:**

- `substring: string` - Text to find

**Returns:** `boolean[]`

### str.startsWith

Check if strings start with prefix.

```typescript
const names = Series.from(["Alice", "Andrew", "Bob"], DType.String);
const startsWithA = names.str.startsWith("A");
// [true, true, false]
```

**Parameters:**

- `prefix: string` - Text to match

**Returns:** `boolean[]`

### str.endsWith

Check if strings end with suffix.

```typescript
const files = Series.from(["data.csv", "info.txt"], DType.String);
const isCsv = files.str.endsWith(".csv");
// [true, false]
```

**Parameters:**

- `suffix: string` - Text to match

**Returns:** `boolean[]`

### str.replace

Replace substring.

```typescript
const texts = Series.from(["hello world", "hello there"], DType.String);
const replaced = texts.str.replace("hello", "hi");
// ["hi world", "hi there"]
```

**Parameters:**

- `search: string` - Text to find
- `replacement: string` - Replacement text

**Returns:** `Series<string>` - Series with replacements

### str.split

Split strings.

```typescript
const paths = Series.from(["a/b/c", "x/y/z"], DType.String);
const parts = paths.str.split("/");
// [["a", "b", "c"], ["x", "y", "z"]]
```

**Parameters:**

- `delimiter: string` - Split character

**Returns:** `string[][]` - Array of split results

### str.length

Get string lengths.

```typescript
const words = Series.from(["cat", "dog", "bird"], DType.String);
const lengths = words.str.length();
// [3, 3, 4]
```

**Returns:** `number[]` - Length of each string

## Numeric Operations

For numeric Series (Int32, Int64, Float64):

### sum

Sum of all values.

```typescript
const numbers = Series.from([1, 2, 3, 4, 5], DType.Int32);
const total = numbers.sum();
// 15
```

**Returns:** `number` - Sum

### mean

Average value.

```typescript
const avg = numbers.mean();
// 3
```

**Returns:** `number` - Mean

### min

Minimum value.

```typescript
const smallest = numbers.min();
// 1
```

**Returns:** `number` - Minimum

### max

Maximum value.

```typescript
const largest = numbers.max();
// 5
```

**Returns:** `number` - Maximum

### unique

Get unique values.

```typescript
const values = Series.from([1, 2, 2, 3, 3, 3], DType.Int32);
const distinct = values.unique();
// [1, 2, 3]
```

**Returns:** `T[]` - Unique values

## Comparison

### equals

Check if two Series are equal.

```typescript
const s1 = Series.from([1, 2, 3], DType.Int32);
const s2 = Series.from([1, 2, 3], DType.Int32);
const same = s1.equals(s2);
// true
```

**Parameters:**

- `other: Series<T>` - Series to compare

**Returns:** `boolean`

## Examples

### String cleaning

```typescript
const names = df.get("name");
const cleaned = names.str.toLowerCase().trim().replace("_", " ");
```

### Filtering based on string pattern

```typescript
const emails = df.get("email");
const companyEmails = emails
  .toArray()
  .filter((_, i) => emails.str.endsWith("@company.com")[i]);
```

### Computing statistics

```typescript
const salaries = df.get("salary");
const stats = {
  total: salaries.sum(),
  average: salaries.mean(),
  min: salaries.min(),
  max: salaries.max(),
};
```

### Working with nulls

```typescript
const ages = df.get("age");
const validAges = ages.dropNull();
const avgAge = validAges.mean();
```

## Type-Safe Operations

Series is generic over its value type:

```typescript
const strings: Series<string> = df.get("name");
const numbers: Series<number> = df.get("age");
const bools: Series<boolean> = df.get("active");

// TypeScript knows the type
const upper = strings.str.toUpperCase(); // ✓ OK
const sum = numbers.sum(); // ✓ OK
// numbers.str.toUpperCase();             // ✗ Error
```

## See Also

- [DataFrame API](./dataframe.md) - Working with full tables
- [Data Types](/guide/data-types.md) - Understanding DType
