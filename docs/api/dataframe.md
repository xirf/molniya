# DataFrame

DataFrame is the core data structure in Molniya. It represents a table with named, typed columns.

## Creating DataFrames

### fromColumns

Create a DataFrame from column data.

```typescript
import { DataFrame, DType } from "molniya";

const df = DataFrame.fromColumns(
  {
    name: ["Alice", "Bob", "Charlie"],
    age: [25, 30, 35],
    salary: [50000, 60000, 70000],
  },
  {
    name: DType.String,
    age: DType.Int32,
    salary: DType.Float64,
  },
);
```

**Parameters:**

- `data: Record<string, any[]>` - Column name to array mapping
- `schema: Schema` - Column types

**Returns:** `DataFrame`

### fromRows

Create a DataFrame from row objects.

```typescript
const df = DataFrame.fromRows(
  [
    { name: "Alice", age: 25, salary: 50000 },
    { name: "Bob", age: 30, salary: 60000 },
  ],
  {
    name: DType.String,
    age: DType.Int32,
    salary: DType.Float64,
  },
);
```

**Parameters:**

- `rows: Record<string, any>[]` - Array of row objects
- `schema: Schema` - Column types

**Returns:** `DataFrame`

## Properties

### columns

Map of column names to Column objects.

```typescript
const columns: Map<string, Column> = df.columns;
```

### columnOrder

Array of column names in order.

```typescript
const names: string[] = df.columnOrder;
console.log(names); // ['name', 'age', 'salary']
```

## Selection & Filtering

### select

Select specific columns.

```typescript
const subset = df.select(["name", "salary"]);
```

**Parameters:**

- `columns: string[]` - Column names to select

**Returns:** `DataFrame` with only selected columns

### filter

Filter rows based on a condition.

```typescript
const adults = df.filter("age", ">=", 18);
const highEarners = df.filter("salary", ">", 75000);
```

**Parameters:**

- `column: string` - Column name to filter on
- `operator: FilterOperator` - Comparison operator
- `value: any` - Value to compare against

**FilterOperator values:**

- `"=="` - Equal
- `"!="` - Not equal
- `">"` - Greater than
- `">="` - Greater than or equal
- `"<"` - Less than
- `"<="` - Less than or equal
- `"in"` - Value in array
- `"contains"` - String contains substring
- `"startsWith"` - String starts with
- `"endsWith"` - String ends with

**Returns:** `DataFrame` with filtered rows

**Example:**

```typescript
// Multiple filters (chaining)
const result = df
  .filter("age", ">=", 25)
  .filter("age", "<=", 65)
  .filter("salary", ">", 50000);
```

## Sorting

### sortBy

Sort DataFrame by a column.

```typescript
const sorted = df.sortBy("salary", false); // descending
const sorted2 = df.sortBy("age", true); // ascending
```

**Parameters:**

- `column: string` - Column to sort by
- `ascending: boolean` - Sort direction (true = ascending, false = descending)

**Returns:** `DataFrame` sorted by column

## Column Operations

### get

Get a column as a Series.

```typescript
const ages = df.get("age");
const names = df.get("name");
```

**Parameters:**

- `column: string` - Column name

**Returns:** `Series<T>` where T depends on column type

### drop

Remove columns.

```typescript
const withoutAge = df.drop(["age"]);
```

**Parameters:**

- `columns: string[]` - Columns to remove

**Returns:** `DataFrame` without specified columns

### rename

Rename columns.

```typescript
const renamed = df.rename({
  old_name: "new_name",
  age: "years",
});
```

**Parameters:**

- `mapping: Record<string, string>` - Old name to new name mapping

**Returns:** `DataFrame` with renamed columns

## Aggregation

### head

Get first N rows.

```typescript
const first10 = df.head(10);
```

**Parameters:**

- `n: number` - Number of rows (default: 5)

**Returns:** `DataFrame` with first N rows

### tail

Get last N rows.

```typescript
const last10 = df.tail(10);
```

**Parameters:**

- `n: number` - Number of rows (default: 5)

**Returns:** `DataFrame` with last N rows

### count

Count non-null values per column.

```typescript
const counts = df.count();
// { name: 100, age: 98, salary: 100 }
```

**Returns:** `Record<string, number>` - Column name to count

## Display

### toString

Get string representation.

```typescript
console.log(df.toString());
```

**Parameters:**

- `maxRows?: number` - Maximum rows to display (default: 10)

**Returns:** `string` - Formatted table

### toArray

Convert to array of row objects.

```typescript
const rows = df.toArray();
// [{ name: "Alice", age: 25, salary: 50000 }, ...]
```

**Returns:** `Record<string, any>[]` - Array of rows

## Utility

### shape

Get DataFrame dimensions.

```typescript
const [rows, cols] = df.shape;
console.log(`${rows} rows × ${cols} columns`);
```

**Returns:** `[number, number]` - [row count, column count]

### dtypes

Get column data types.

```typescript
const types = df.dtypes;
// { name: DType.String, age: DType.Int32, salary: DType.Float64 }
```

**Returns:** `Schema` - Column name to DType mapping

### isEmpty

Check if DataFrame is empty.

```typescript
if (df.isEmpty) {
  console.log("No data!");
}
```

**Returns:** `boolean`

## Chaining Operations

All transformation methods return a new DataFrame, enabling method chaining:

```typescript
const result = df
  .filter("age", ">", 25)
  .select(["name", "salary"])
  .sortBy("salary", false)
  .head(10);
```

## Examples

### Basic filtering and selection

```typescript
const result = df
  .filter("department", "==", "Engineering")
  .select(["name", "salary"])
  .sortBy("salary", false);
```

### String filtering

```typescript
const results = df
  .filter("email", "endsWith", "@company.com")
  .filter("name", "startsWith", "A");
```

### Complex filtering

```typescript
const californians = df
  .filter("state", "==", "CA")
  .filter("age", ">=", 21)
  .filter("salary", ">", 60000);
```

### Working with nulls

```typescript
const ages = df.get("age").toArray();
const validAges = ages.filter((age) => age !== null);
const nullCount = ages.filter((age) => age === null).length;
```

## See Also

- [LazyFrame API](./lazyframe.md) - For optimized query execution
- [Series API](./series.md) - For column operations
- [CSV Reading](./csv-reading.md) - Loading data into DataFrames
