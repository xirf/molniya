# Series API

A `Series` represents a single column of typed data. It behaves like a smart array with built-in statistical capabilities.

## Accessing Data

### `at`
Get the value at a specific index.

```typescript
at(index: number): any
```

### `length`
The number of items in the Series.

```typescript
console.log(series.length);
```

### `toArray`
Convert the Series back to a standard JavaScript array.

```typescript
const numbers = series.toArray(); // number[]
```

---

## Statistics

### `sum`
Total sum of values (numeric only).
```typescript
console.log(prices.sum());
```

### `mean`
Average value (numeric only).
```typescript
console.log(scores.mean());
```

### `min` / `max`
Smallest and largest values.
```typescript
console.log(dates.min()); // Earliest date
```

### `std`
Standard deviation.
```typescript
console.log(volatility.std());
```

### `valueCounts`
Count occurrences of each unique value. Returns a new DataFrame.

```typescript
const popularity = brands.valueCounts();
popularity.print();
// ┌───────┬───────┐
// │ value │ count │
// ├───────┼───────┤
// │ Apple │ 50    │
// │ Sony  │ 30    │
// └───────┴───────┘
```

---

## Transformation

### `map`
Apply a function to every value. Returns a new Series.

```typescript
const upper = names.map(name => name.toUpperCase());
```

### `sort`
Sort the values.

```typescript
const sorted = values.sort(false); // Descending
```

### `unique`
Return a new Series with duplicate values removed.

```typescript
const categories = allCategories.unique();
```

---

## Missing Values

### `isna`
Returns a boolean Series indicating which values are null/undefined/NaN.

```typescript
const missingMask = data.isna();
```

### `fillna`
Replace missing values with a constant.

```typescript
const clean = data.fillna(0);
```

