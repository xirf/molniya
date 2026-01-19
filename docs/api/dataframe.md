# DataFrame

The main data structure for tabular data.

## Creating

### `DataFrame.fromColumns(data)`

Create from column objects:

```typescript
const df = DataFrame.fromColumns({
  name: ["Alice", "Bob"],
  age: [25, 30],
});
```

### `DataFrame.empty(schema)`

Create an empty DataFrame with a schema.

## Properties

| Property | Type               | Description       |
| -------- | ------------------ | ----------------- |
| `shape`  | `[number, number]` | `[rows, columns]` |
| `height` | `number`           | Row count         |
| `width`  | `number`           | Column count      |

## Methods

### Selection

| Method         | Description                 |
| -------------- | --------------------------- |
| `col(name)`    | Get a Series by column name |
| `columns()`    | Get column names            |
| `select(cols)` | Select specific columns     |
| `drop(cols)`   | Remove columns              |

### Filtering

| Method                | Description                     |
| --------------------- | ------------------------------- |
| `filter(fn)`          | Keep rows where fn returns true |
| `where(col, op, val)` | Filter by column condition      |
| `head(n)`             | First n rows                    |
| `tail(n)`             | Last n rows                     |

### Sorting

| Method            | Description    |
| ----------------- | -------------- |
| `sort(col, asc?)` | Sort by column |

### Grouping

| Method          | Description        |
| --------------- | ------------------ |
| `groupby(cols)` | Group by column(s) |

`GroupBy` aggregations: `agg({ col: op })`, `sum(cols)`, `mean(cols)`, `count()`.

Supported ops: `sum`, `mean`, `min`, `max`, `count`, `first`, `last`.

> [!NOTE]
> Aggregations return strictly typed `DataFrames`. For example, `mean()` will result in `float64` columns, while `count()` results in `int32`.

### Transformation

| Method                 | Description        |
| ---------------------- | ------------------ |
| `rename(mapping)`      | Rename columns     |
| `assign(name, values)` | Add new column     |
| `apply(fn)`            | Transform each row |
| `copy()`               | Deep copy          |

### Missing Values

| Method          | Description                        |
| --------------- | ---------------------------------- |
| `dropna()`      | Remove rows with NaN               |
| `fillna(value)` | Replace NaN values                 |
| `isna()`        | Boolean DataFrame of NaN positions |

### Combining

| Method               | Description            |
| -------------------- | ---------------------- |
| `concat(dfs)`        | Vertical concatenation |
| `merge(other, opts)` | SQL-like join          |
| `unique()`           | Remove duplicate rows  |

### Display

| Method       | Description        |
| ------------ | ------------------ |
| `print()`    | Print ASCII table  |
| `toString()` | Get as string      |
| `describe()` | Summary statistics |
| `info()`     | Column info        |
