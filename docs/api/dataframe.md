# DataFrame Class

The `DataFrame` class is the core data structure in Molniya. It represents a collection of data organized into named columns.

## Overview

```typescript
class DataFrame<T = Record<string, unknown>> {
  // Properties
  readonly schema: Schema;
  readonly columnNames: string[];
  
  // Methods
  filter(expr: Expr): DataFrame<T>;
  select(...columns: string[]): DataFrame<Pick<T, K>>;
  // ... and more
}
```

The generic type parameter `T` represents the row type and enables TypeScript type inference through transformations.

## Properties

### schema

Returns the current schema of the DataFrame.

```typescript
const df = await readCsv("data.csv", schema);
console.log(df.schema);
// { columns: [...], columnMap: Map, rowSize: 24, columnCount: 4 }
```

### columnNames

Returns an array of column names.

```typescript
const df = await readCsv("data.csv", schema);
console.log(df.columnNames);
// ['id', 'name', 'age', 'salary']
```

## Static Methods

### DataFrame.empty()

Creates an empty DataFrame with a given schema.

```typescript
import { DataFrame, createSchema, unwrap, DType } from "molniya";

const schema = unwrap(createSchema({
  id: DType.int32,
  name: DType.string
}));

const empty = DataFrame.empty(schema, null);
```

### DataFrame.fromChunks()

Creates a DataFrame from existing chunks (advanced use).

```typescript
import { DataFrame } from "molniya";

const df = DataFrame.fromChunks(chunks, schema, dictionary);
```

### DataFrame.fromStream()

Creates a DataFrame from an async iterable of chunks (advanced use).

```typescript
import { DataFrame } from "molniya";

const df = DataFrame.fromStream(asyncIterable, schema, dictionary);
```

## Transformation Methods

These methods return a new DataFrame with the transformation applied. They use lazy evaluation - no data is processed until an action is called.

### filter()

Select rows matching a condition.

```typescript
filter(expr: Expr): DataFrame<T>
```

**Example:**

```typescript
df.filter(col("age").gte(18))
df.filter(and(col("status").eq("active"), col("balance").gt(0)))
```

### where()

Alias for `filter()`.

```typescript
where(expr: Expr): DataFrame<T>
```

### select()

Select specific columns.

```typescript
select<K extends keyof T>(...columns: (K & string)[]): DataFrame<Pick<T, K>>
```

**Example:**

```typescript
df.select("id", "name")
df.select("id")  // TypeScript knows this returns DataFrame<{id: number}>
```

### drop()

Drop specific columns (keeps all others).

```typescript
drop<K extends keyof T>(...columns: (K & string)[]): DataFrame<Omit<T, K>>
```

**Example:**

```typescript
df.drop("temp_column", "internal_id")
```

### rename()

Rename columns.

```typescript
rename(mapping: Partial<Record<keyof T, string>>): DataFrame<Record<string, unknown>>
```

**Example:**

```typescript
df.rename({ firstName: "first_name", lastName: "last_name" })
```

### withColumn()

Add a computed column.

```typescript
withColumn<K extends string>(name: K, expr: Expr): DataFrame<T & Record<K, unknown>>
```

**Example:**

```typescript
df.withColumn("full_name", col("first").add(" ").add(col("last")))
df.withColumn("discounted", col("price").mul(0.9))
```

### withColumns()

Add multiple computed columns.

```typescript
withColumns(columns: ComputedColumn[] | Record<string, Expr>): DataFrame
```

**Example:**

```typescript
df.withColumns([
  { name: "tax", expr: col("amount").mul(0.1) },
  { name: "total", expr: col("amount").add(col("tax")) }
])

// Or with object syntax:
df.withColumns({
  tax: col("amount").mul(0.1),
  total: col("amount").add(col("tax"))
})
```

### cast()

Cast a column to a different type.

```typescript
cast(column: keyof T, targetDType: DType): DataFrame<T>
```

**Example:**

```typescript
df.cast("id", DType.int64)
df.cast("price", DType.float64)
```

### fillNull()

Replace null values in a column.

```typescript
fillNull(column: keyof T, fillValue: number | bigint | string | boolean): DataFrame<T>
```

**Example:**

```typescript
df.fillNull("email", "unknown@example.com")
df.fillNull("age", 0)
```

### dropNull()

Remove rows with null values.

```typescript
dropNull(columns?: keyof T | (keyof T)[]): DataFrame<T>
```

**Example:**

```typescript
df.dropNull()  // Drop rows with any null
df.dropNull("email")  // Drop rows where email is null
df.dropNull(["firstName", "lastName"])  // Drop rows where either is null
```

## Sorting Methods

### sort()

Sort by one or more columns.

```typescript
sort(keys: string | string[] | SortKey[]): DataFrame<T>
```

**Example:**

```typescript
df.sort("name")  // Ascending
df.sort(desc("amount"))  // Descending
df.sort(["lastName", "firstName"])  // Multiple columns
df.sort([{ column: "amount", descending: true }])  // Explicit syntax
```

### orderBy()

Alias for `sort()`.

```typescript
orderBy(keys: string | string[] | SortKey[]): DataFrame<T>
```

## Limiting Methods

### limit()

Limit the number of rows.

```typescript
limit(n: number): DataFrame<T>
```

**Example:**

```typescript
df.limit(100)  // First 100 rows
```

### head()

Alias for `limit()` with default of 5.

```typescript
head(n?: number): DataFrame<T>
```

**Example:**

```typescript
df.head()     // First 5 rows
df.head(10)   // First 10 rows
```

### slice()

Skip rows and then limit.

```typescript
slice(start: number, count: number): DataFrame<T>
```

**Example:**

```typescript
df.slice(100, 50)  // Skip 100, take 50 (rows 101-150)
```

## Aggregation Methods

### groupBy()

Group rows by key columns for aggregation.

```typescript
groupBy(keyColumns: string | string[]): RelationalGroupedDataset<T>
```

**Example:**

```typescript
const grouped = df.groupBy("category")
const result = grouped.agg([
  { name: "total", expr: sum("amount") }
])
```

See [GroupBy](./groupby) for more details.

### agg()

Aggregate without grouping (produces single row).

```typescript
agg(specs: TypedAggSpec[]): DataFrame
```

**Example:**

```typescript
const totals = await df.agg([
  { name: "total_sales", expr: sum("amount") },
  { name: "avg_price", expr: avg("price") },
  { name: "count", expr: count() }
]).collect();
```

### min() / max() / mean()

Shortcut aggregation methods on DataFrame.

```typescript
min(column: keyof T): Promise<number | string | null>
max(column: keyof T): Promise<number | string | null>
mean(column: keyof T): Promise<number | null>
```

**Example:**

```typescript
const minAge = await df.min("age")
const maxAge = await df.max("age")
const avgSalary = await df.mean("salary")
```

## Join Methods

### innerJoin()

Inner join with another DataFrame.

```typescript
innerJoin<U>(other: DataFrame<U>, leftOn: keyof T, rightOn?: keyof U, suffix?: string): Promise<DataFrame<T & U>>
```

**Example:**

```typescript
const result = await orders.innerJoin(customers, "customerId", "id")
```

### leftJoin()

Left join with another DataFrame.

```typescript
leftJoin<U>(other: DataFrame<U>, leftOn: keyof T, rightOn?: keyof U, suffix?: string): Promise<DataFrame<T & U>>
```

### semiJoin()

Semi join (returns rows from left where match exists in right).

```typescript
semiJoin<U>(other: DataFrame<U>, on: keyof T | (keyof T)[]): Promise<DataFrame<T>>
```

### antiJoin()

Anti join (returns rows from left where no match exists in right).

```typescript
antiJoin<U>(other: DataFrame<U>, on: keyof T | (keyof T)[]): Promise<DataFrame<T>>
```

### crossJoin()

Cross join (Cartesian product).

```typescript
crossJoin<U>(other: DataFrame<U>, suffix?: string): Promise<DataFrame<T & U>>
```

## Execution Methods

See [Execution Methods](./execution) for details on `collect()`, `show()`, `toArray()`, etc.

## Inspection Methods

See [Inspection Methods](./inspection) for details on `printSchema()`, `explain()`, etc.
