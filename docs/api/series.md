# Series API

A `Series` is a typed 1D array backed by TypedArrays for numerics.

## Creation

```typescript
// Numeric types
const floats = Series.float64([1.1, 2.2, 3.3]);
const ints = Series.int32([1, 2, 3]);

// String and boolean
const names = Series.string(['Alice', 'Bob']);
const flags = Series.bool([true, false, true]);
```

## Properties

| Property | Type     | Description        |
| -------- | -------- | ------------------ |
| `length` | `number` | Number of elements |
| `dtype`  | `DType`  | Type descriptor    |

## Element Access

```typescript
series.at(0);           // First element
series.at(-1);          // undefined (no negative indexing)
```

## Slicing

```typescript
series.head(5);         // First 5 elements (zero-copy view)
series.tail(5);         // Last 5 elements
series.slice(1, 4);     // Elements 1 to 3
```

## Statistical Operations

| Method       | Description        | Returns  |
| ------------ | ------------------ | -------- |
| `sum()`      | Sum of values      | `number` |
| `mean()`     | Average            | `number` |
| `min()`      | Minimum            | `number` |
| `max()`      | Maximum            | `number` |
| `std()`      | Standard deviation | `number` |
| `var()`      | Variance           | `number` |
| `describe()` | Summary stats      | `object` |

```typescript
const s = Series.float64([1, 2, 3, 4, 5]);

s.sum();      // 15
s.mean();     // 3
s.min();      // 1
s.max();      // 5
s.describe(); // { count: 5, mean: 3, std: 1.41, min: 1, max: 5 }
```

## Transformations

```typescript
// Filter
s.filter(v => v > 2);        // [3, 4, 5]

// Map
s.map(v => v * 2);           // [2, 4, 6, 8, 10]

// Sort
s.sort();                    // Ascending
s.sort(false);               // Descending

// Unique
s.unique();                  // Remove duplicates

// Value counts
s.valueCounts();             // Map<value, count>

// Convert
s.toArray();                 // Plain array
```
