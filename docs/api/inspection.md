# Inspection Methods

Methods for examining DataFrame structure and execution plans.

## schema

Property that returns the current schema.

```typescript
readonly schema: Schema
```

**Example:**

```typescript
const df = await readCsv("data.csv", {
  id: DType.int32,
  name: DType.string,
  amount: DType.float64
});

console.log(df.schema);
// {
//   columns: [
//     { name: "id", dtype: { kind: 2, nullable: false }, offset: 0 },
//     { name: "name", dtype: { kind: 11, nullable: false }, offset: 4 },
//     { name: "amount", dtype: { kind: 9, nullable: false }, offset: 8 }
//   ],
//   columnMap: Map(3) { "id" => 0, "name" => 1, "amount" => 2 },
//   rowSize: 16,
//   columnCount: 3
// }
```

## columnNames

Property that returns an array of column names.

```typescript
readonly columnNames: string[]
```

**Example:**

```typescript
console.log(df.columnNames);
// ["id", "name", "amount"]

// Useful for dynamic operations
for (const col of df.columnNames) {
  console.log(`Column: ${col}`);
}
```

## printSchema()

Print a formatted schema to the console.

```typescript
printSchema(): void
```

**Example:**

```typescript
df.printSchema();
```

**Output:**

```
Schema:
  id: Int32 (non-nullable)
  name: String (non-nullable)
  amount: Float64 (non-nullable)
```

::: tip Debugging
Use `printSchema()` when developing to verify your transformations produce the expected schema.
:::

## explain()

Return a string representation of the execution plan.

```typescript
explain(): string
```

**Example:**

```typescript
const df = await readCsv("data.csv", schema);
const plan = df
  .filter(col("status").eq("active"))
  .select("id", "name")
  .limit(100);

console.log(plan.explain());
```

**Output:**

```
DataFrame [id, name]
Execution Plan:
└─ Limit
  └─ Project
    └─ Filter
```

## Schema After Transformations

The schema updates as you transform the DataFrame:

```typescript
const df = await readCsv("data.csv", {
  id: DType.int32,
  name: DType.string,
  amount: DType.float64
});

// Original schema
console.log(df.columnNames);  // ["id", "name", "amount"]

// After select
const selected = df.select("id", "name");
console.log(selected.columnNames);  // ["id", "name"]

// After withColumn
const enriched = df.withColumn("doubled", col("amount").mul(2));
console.log(enriched.columnNames);  // ["id", "name", "amount", "doubled"]

// After drop
const dropped = df.drop("amount");
console.log(dropped.columnNames);  // ["id", "name"]
```

## Type Inference

Molniya tracks types through transformations:

```typescript
// TypeScript knows the resulting type
const filtered = df.filter(col("id").gt(0));
// filtered is DataFrame<{ id: number, name: string, amount: number }>

const selected = df.select("id");
// selected is DataFrame<{ id: number }>
```

## Checking for Columns

```typescript
// Check if column exists
const hasEmail = df.columnNames.includes("email");

// Get column index
const nameIndex = df.columnNames.indexOf("name");

// Conditional operations
if (df.columnNames.includes("created_at")) {
  // Do date-based analysis
}
```

## Schema Comparison

```typescript
// Compare schemas of two DataFrames
function schemasEqual(df1: DataFrame, df2: DataFrame): boolean {
  const names1 = df1.columnNames;
  const names2 = df2.columnNames;
  
  if (names1.length !== names2.length) return false;
  
  return names1.every((name, i) => name === names2[i]);
}

// Usage
const df1 = await readCsv("file1.csv", schema);
const df2 = await readCsv("file2.csv", schema);

if (schemasEqual(df1, df2)) {
  const combined = await df1.concat(df2);
}
```

## Debugging Workflow

A typical debugging workflow:

```typescript
const df = await readCsv("data.csv", schema);

// 1. Check initial schema
df.printSchema();

// 2. Add transformation
const filtered = df.filter(col("status").eq("active"));

// 3. Check plan
console.log(filtered.explain());

// 4. Preview data
await filtered.show(5);

// 5. Check resulting schema
filtered.printSchema();

// 6. Continue with confidence
const result = await filtered.toArray();
```

## Advanced: Accessing Schema Details

```typescript
// Get detailed column information
const schema = df.schema;

for (const col of schema.columns) {
  console.log(`
    Name: ${col.name}
    Type Kind: ${col.dtype.kind}
    Nullable: ${col.dtype.nullable}
    Offset: ${col.offset}
  `);
}

// Get column by name
const nameCol = schema.columns[schema.columnMap.get("name")!];
console.log(nameCol.dtype.kind === DTypeKind.String);  // true
```

## See Also

- [Schema API](./schema) - Schema creation and manipulation
- [DType](./dtype) - Data type reference
- [Core Concepts](../guide/core-concepts) - Understanding lazy evaluation
