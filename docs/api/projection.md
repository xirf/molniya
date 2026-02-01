# Projection API

API reference for selecting, dropping, and renaming columns.

## select()

Select specific columns from the DataFrame.

```typescript
select(...columns: string[]): DataFrame<Pick<T, K>>
```

**Parameters:**
- `columns` - Column names to select

**Returns:** DataFrame with only the specified columns

**Example:**

```typescript
df.select("id", "name", "email")
df.select("product", "price")
```

### Type Inference

`select()` preserves TypeScript type information:

```typescript
// Original: DataFrame<{ id: number, name: string, email: string }>
const subset = df.select("id", "name");
// Result: DataFrame<{ id: number, name: string }>
```

## drop()

Remove specific columns from the DataFrame.

```typescript
drop(...columns: string[]): DataFrame<Omit<T, K>>
```

**Parameters:**
- `columns` - Column names to remove

**Returns:** DataFrame without the specified columns

**Example:**

```typescript
df.drop("temp_column", "internal_id")
df.drop("password")  // Remove sensitive data
```

## rename()

Rename columns.

```typescript
rename(mapping: Record<string, string>): DataFrame<T>
```

**Parameters:**
- `mapping` - Object with old names as keys and new names as values

**Returns:** DataFrame with renamed columns

**Example:**

```typescript
df.rename({
  "old_name": "new_name",
  "user_id": "id",
  "first": "first_name"
})
```

## withColumnRenamed()

Rename a single column (convenience method).

```typescript
withColumnRenamed(oldName: string, newName: string): DataFrame<T>
```

**Parameters:**
- `oldName` - Current column name
- `newName` - New column name

**Example:**

```typescript
df.withColumnRenamed("user_id", "id")
  .withColumnRenamed("fname", "first_name")
```

## Column Selection Patterns

### Select All Except

```typescript
// Select all columns except specific ones
const allExcept = df.drop("password", "secret_key");
```

### Dynamic Selection

```typescript
// Select columns matching pattern
const numericCols = df.columnNames.filter(name => 
  ["price", "amount", "quantity", "total"].includes(name)
);
const numericDf = df.select(...numericCols);
```

### Conditional Selection

```typescript
// Select columns that exist
const desiredCols = ["id", "name", "email", "phone"];
const existingCols = desiredCols.filter(col => 
  df.columnNames.includes(col)
);
const result = df.select(...existingCols);
```

## Projection with Expressions

### Select with Aliases

```typescript
import { col } from "Molniya";

// Select with computed columns
df.select("id")
  .withColumn("full_name", col("first").add(" ").add(col("last")))
  .select("id", "full_name")
```

### Select Distinct

```typescript
// Get unique combinations
df.select("category", "subcategory").distinct()
```

## Chaining Projections

Projections can be chained for complex transformations:

```typescript
df.select("id", "name", "email", "temp_data", "internal_id")
  .drop("temp_data", "internal_id")           // Remove temp columns
  .withColumnRenamed("email", "email_address") // Rename email
  .select("id", "name", "email_address")       // Reorder columns
```

## Performance Notes

- `select()` is a zero-copy operation when possible
- `drop()` creates a new logical plan without the columns
- Projection pushdown filters columns at the source for file reads
- Selecting fewer columns reduces memory usage

## Common Patterns

### Select Public Columns

```typescript
const publicCols = ["id", "name", "email", "created_at"];
const publicDf = df.select(...publicCols);
```

### Remove Internal Columns

```typescript
const internalCols = ["_version", "_created_by", "_internal_id"];
const cleanDf = df.drop(...internalCols);
```

### Standardize Column Names

```typescript
const standardized = df.rename({
  "UserID": "user_id",
  "FirstName": "first_name",
  "LastName": "last_name",
  "EmailAddr": "email"
});
```

### Reorder Columns

```typescript
// Put important columns first
const reordered = df.select(
  "id", "name", "status",  // Important first
  ...df.columnNames.filter(c => !["id", "name", "status"].includes(c))
);
```

## Type Safety

### Preserving Types

```typescript
interface User {
  id: number;
  name: string;
  email: string;
  age: number;
}

const df: DataFrame<User> = ...;

// TypeScript knows the result type
const subset = df.select("id", "name");
// subset is DataFrame<Pick<User, "id" | "name">>
```

### Type Changes with Rename

```typescript
// Rename doesn't change types, just column names
const renamed = df.rename({ "user_id": "id" });
// Types remain the same
```
