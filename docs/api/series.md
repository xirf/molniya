# Series

A single column of typed data.

## Properties

| Property | Type     | Description        |
| -------- | -------- | ------------------ |
| `dtype`  | `DType`  | Data type info     |
| `length` | `number` | Number of elements |

## Methods

### Access

| Method              | Description        |
| ------------------- | ------------------ |
| `at(index)`         | Get value at index |
| `head(n)`           | First n elements   |
| `tail(n)`           | Last n elements    |
| `slice(start, end)` | Slice of elements  |

### Statistics

| Method       | Description        |
| ------------ | ------------------ |
| `sum()`      | Sum of values      |
| `mean()`     | Average            |
| `min()`      | Minimum value      |
| `max()`      | Maximum value      |
| `std()`      | Standard deviation |
| `describe()` | Summary stats      |

### Transformation

| Method          | Description          |
| --------------- | -------------------- |
| `map(fn)`       | Transform each value |
| `filter(fn)`    | Keep matching values |
| `sort(asc?)`    | Sort values          |
| `unique()`      | Unique values        |
| `valueCounts()` | Count occurrences    |

### Missing Values

| Method          | Description           |
| --------------- | --------------------- |
| `isna()`        | Boolean Series of NaN |
| `fillna(value)` | Replace NaN           |
| `copy()`        | Deep copy             |

### Conversion

| Method      | Description            |
| ----------- | ---------------------- |
| `toArray()` | Convert to plain array |

## Iteration

Series are iterable:

```typescript
for (const value of series) {
  console.log(value);
}

const arr = [...series];
```
