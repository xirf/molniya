# Creating DataFrames

Molniya provides several ways to create DataFrames from various data sources, beyond just reading files.

## From Records

### fromRecords()

Create a DataFrame from an array of JavaScript objects:

```typescript
import { fromRecords, DType } from "Molniya";

const users = [
  { id: 1, name: "Alice", age: 30, active: true },
  { id: 2, name: "Bob", age: 25, active: false },
  { id: 3, name: "Charlie", age: 35, active: true }
];

const schema = {
  id: DType.int32,
  name: DType.string,
  age: DType.int32,
  active: DType.boolean
};

const df = fromRecords(users, schema);
await df.show();
```

### Nullable Values

Handle records with missing values:

```typescript
const data = [
  { id: 1, name: "Alice", email: "alice@example.com" },
  { id: 2, name: "Bob", email: null },  // null value
  { id: 3, name: "Charlie" }             // missing key
];

const schema = {
  id: DType.int32,
  name: DType.string,
  email: DType.nullable.string  // Nullable to allow nulls
};

const df = fromRecords(data, schema);
```

## From Columns

### fromColumns()

Create a DataFrame from column-oriented data:

```typescript
import { fromColumns, DType } from "Molniya";

const columns = {
  id: new Int32Array([1, 2, 3]),
  name: ["Alice", "Bob", "Charlie"],
  score: new Float64Array([95.5, 87.0, 92.3])
};

const schema = {
  id: DType.int32,
  name: DType.string,
  score: DType.float64
};

const df = fromColumns(columns, schema);
```

### Using TypedArrays

For best performance, use TypedArrays for numeric columns:

```typescript
import { fromColumns, DType } from "Molniya";

const data = {
  // Integer arrays
  id: new Int32Array([1, 2, 3, 4, 5]),
  count: new Uint32Array([10, 20, 30, 40, 50]),
  
  // Float arrays
  price: new Float64Array([9.99, 19.99, 29.99, 39.99, 49.99]),
  rating: new Float32Array([4.5, 3.8, 4.2, 4.9, 3.5]),
  
  // String array (regular array, dictionary-encoded internally)
  category: ["electronics", "books", "electronics", "clothing", "books"],
  
  // Boolean array
  in_stock: new Uint8Array([1, 0, 1, 1, 0])
};

const schema = {
  id: DType.int32,
  count: DType.uint32,
  price: DType.float64,
  rating: DType.float32,
  category: DType.string,
  in_stock: DType.boolean
};

const df = fromColumns(data, schema);
```

## Empty DataFrames

### DataFrame.empty()

Create an empty DataFrame with a defined schema:

```typescript
import { DataFrame, DType } from "Molniya";

const schema = {
  id: DType.int32,
  name: DType.string,
  amount: DType.float64
};

const emptyDf = DataFrame.empty(schema);

console.log(emptyDf.columnNames);  // ["id", "name", "amount"]
console.log(await emptyDf.count()); // 0
```

### Use Cases for Empty DataFrames

```typescript
// Initialize accumulator
let result = DataFrame.empty(schema);

// Append data in a loop
for (const batch of batches) {
  const batchDf = fromRecords(batch, schema);
  result = result.union(batchDf);
}

// Type-safe schema template
function createUserDataFrame(records: UserRecord[]) {
  const schema = {
    id: DType.int32,
    name: DType.string,
    email: DType.nullable.string
  };
  
  if (records.length === 0) {
    return DataFrame.empty(schema);
  }
  
  return fromRecords(records, schema);
}
```

## From CSV Strings

### fromCsvString()

Create a DataFrame from a CSV string in memory:

```typescript
import { fromCsvString, DType } from "Molniya";

const csvData = `
id,name,age,city
1,Alice,30,NYC
2,Bob,25,LA
3,Charlie,35,Chicago
`.trim();

const schema = {
  id: DType.int32,
  name: DType.string,
  age: DType.int32,
  city: DType.string
};

const df = fromCsvString(csvData, schema);
await df.show();
```

### With Options

```typescript
const df = fromCsvString(csvData, schema, {
  delimiter: ";",        // Semicolon-separated
  hasHeader: false,      // No header row
  skipRows: 1,           // Skip first data row
  projection: [0, 1, 2]  // Only columns 0, 1, 2
});
```

## From Arrays

### fromArrays()

Create a DataFrame from arrays of values:

```typescript
import { fromArrays, DType } from "Molniya";

const df = fromArrays(
  ["id", "name", "score"],  // Column names
  [
    [1, 2, 3],               // id column
    ["Alice", "Bob", "Charlie"],  // name column
    [95.5, 87.0, 92.3]       // score column
  ],
  {
    id: DType.int32,
    name: DType.string,
    score: DType.float64
  }
);
```

## Creating DataFrames with Single Values

### Repeating Values

Create a DataFrame with repeated values:

```typescript
import { DataFrame, DType, lit } from "Molniya";

// Create a single-row DataFrame
const singleRow = fromRecords([{ id: 1, value: 100 }], {
  id: DType.int32,
  value: DType.int32
});

// Cross join to repeat
const df = existingDf.crossJoin(singleRow);
```

### Range of Numbers

```typescript
import { range } from "Molniya";

// Create DataFrame with sequence
const df = range(0, 100, {
  name: "index",
  dtype: DType.int32
});

// Creates: 0, 1, 2, ..., 99
```

## Combining DataFrames

### Union

Stack DataFrames vertically (must have same schema):

```typescript
const df1 = fromRecords([{ id: 1 }], { id: DType.int32 });
const df2 = fromRecords([{ id: 2 }], { id: DType.int32 });
const df3 = fromRecords([{ id: 3 }], { id: DType.int32 });

// Union all
const combined = df1.union(df2).union(df3);
```

### Union All

Include duplicates:

```typescript
const df1 = fromRecords([{ id: 1 }, { id: 2 }], { id: DType.int32 });
const df2 = fromRecords([{ id: 2 }, { id: 3 }], { id: DType.int32 });

// Result: 1, 2, 2, 3
const combined = df1.unionAll(df2);
```

## Creating DataFrames from Other Sources

### From JSON

```typescript
import { fromRecords, DType } from "Molniya";

const jsonData = '[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]';
const records = JSON.parse(jsonData);

const df = fromRecords(records, {
  id: DType.int32,
  name: DType.string
});
```

### From Database Results

```typescript
import { fromRecords, DType } from "Molniya";

// Assuming you have a database query result
const dbResults = await db.query("SELECT id, name, email FROM users");

const df = fromRecords(dbResults, {
  id: DType.int32,
  name: DType.string,
  email: DType.nullable.string
});
```

### From API Response

```typescript
import { fromRecords, DType } from "Molniya";

const response = await fetch("https://api.example.com/users");
const data = await response.json();

const df = fromRecords(data.users, {
  id: DType.int32,
  username: DType.string,
  email: DType.string
});
```

## Best Practices

1. **Use appropriate types**: Choose the smallest type that fits your data
2. **Handle nulls explicitly**: Use nullable types for optional columns
3. **Prefer TypedArrays**: For numeric data, TypedArrays are more efficient
4. **Validate schemas**: Ensure your schema matches the data structure
5. **Use empty DataFrames**: For type-safe initialization and accumulation

## Performance Tips

- `fromColumns()` is faster than `fromRecords()` for large datasets
- Reuse schemas when creating multiple DataFrames with the same structure
- Use `range()` for generating sequences instead of creating arrays manually

## Complete Example

```typescript
import { 
  fromRecords, 
  fromColumns, 
  DataFrame,
  DType, 
  col, 
  sum,
  desc 
} from "Molniya";

// Create sample data
const salesData = [
  { product: "Laptop", category: "Electronics", amount: 1299.99, quantity: 2 },
  { product: "Mouse", category: "Electronics", amount: 29.99, quantity: 5 },
  { product: "Desk", category: "Furniture", amount: 599.99, quantity: 1 }
];

const schema = {
  product: DType.string,
  category: DType.string,
  amount: DType.float64,
  quantity: DType.int32
};

// Create DataFrame
const df = fromRecords(salesData, schema);

// Analyze
const summary = await df
  .groupBy("category", [
    { name: "total_revenue", expr: sum(col("amount").mul(col("quantity"))) },
    { name: "total_units", expr: sum("quantity") }
  ])
  .sort(desc("total_revenue"))
  .show();
```
