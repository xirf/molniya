# Schema API

API reference for schema definition and inspection.

## Overview

A Schema defines the structure of a DataFrame, including column names, data types, and nullability.

## Schema Interface

```typescript
interface Schema {
  columns: ColumnDef[];           // Column definitions
  columnMap: Map<string, number>; // Name to index mapping
  rowSize: number;                // Bytes per row
  columnCount: number;            // Number of columns
}

interface ColumnDef {
  name: string;                   // Column name
  dtype: DType;                   // Data type
  offset: number;                 // Byte offset in row
}
```

## Creating Schemas

### Inline Schema Object

Define schemas as plain objects:

```typescript
import { DType } from "molniya";

const schema = {
  id: DType.int32,
  name: DType.string,
  age: DType.int32,
  email: DType.nullable.string,
  active: DType.boolean
};
```

### createSchema()

Create a Schema object with validation:

```typescript
createSchema(spec: Record<string, DType>): Result<Schema, Error>
```

**Parameters:**
- `spec` - Object mapping column names to DType values

**Returns:** Result containing Schema or error

**Example:**

```typescript
import { createSchema, DType, unwrap } from "molniya";

const schema = unwrap(createSchema({
  id: DType.int32,
  name: DType.string,
  amount: DType.float64
}));

console.log(schema.columnCount);  // 3
console.log(schema.rowSize);      // 16 (4 + 4 + 8)
```

## Schema Properties

### columnNames

Get array of column names from a DataFrame:

```typescript
readonly columnNames: string[]
```

**Example:**

```typescript
console.log(df.columnNames);
// ["id", "name", "email", "created_at"]
```

### schema

Get the full schema object:

```typescript
readonly schema: Schema
```

**Example:**

```typescript
const schema = df.schema;

for (const col of schema.columns) {
  console.log(`${col.name}: ${col.dtype}`);
}
```

## Schema Inspection

### Get Column Info

```typescript
const schema = df.schema;

// Get column by name
const nameCol = schema.columns.find(c => c.name === "name");

// Check if column exists
const hasEmail = schema.columnMap.has("email");

// Get column index
const nameIndex = schema.columnMap.get("name");

// Get column count
console.log(schema.columnCount);

// Get row size in bytes
console.log(schema.rowSize);
```

### Iterate Columns

```typescript
// Iterate all columns
for (const column of df.schema.columns) {
  console.log(`Column: ${column.name}`);
  console.log(`  Type: ${column.dtype.kind}`);
  console.log(`  Nullable: ${column.dtype.nullable}`);
  console.log(`  Offset: ${column.offset}`);
}
```

## Schema Comparison

### Compare Schemas

```typescript
import { DTypeKind } from "molniya";

function schemasEqual(a: Schema, b: Schema): boolean {
  if (a.columnCount !== b.columnCount) return false;
  
  for (let i = 0; i < a.columnCount; i++) {
    const colA = a.columns[i];
    const colB = b.columns[i];
    
    if (colA.name !== colB.name) return false;
    if (colA.dtype.kind !== colB.dtype.kind) return false;
    if (colA.dtype.nullable !== colB.dtype.nullable) return false;
  }
  
  return true;
}
```

### Check Compatibility

```typescript
function isCompatible(source: Schema, target: Schema): boolean {
  // Target must have all columns from source
  for (const col of source.columns) {
    if (!target.columnMap.has(col.name)) return false;
    
    const targetCol = target.columns[target.columnMap.get(col.name)!];
    
    // Types must match
    if (col.dtype.kind !== targetCol.dtype.kind) return false;
    
    // Source nullability must fit target
    if (col.dtype.nullable && !targetCol.dtype.nullable) return false;
  }
  
  return true;
}
```

## Schema Evolution

### Adding Columns

```typescript
// Create new schema with additional columns
const extendedSchema = unwrap(createSchema({
  ...originalSchema.columns.reduce((acc, col) => ({
    ...acc,
    [col.name]: col.dtype
  }), {}),
  new_column: DType.string
}));
```

### Removing Columns

```typescript
// Create schema without specific columns
const columnsToRemove = ["temp", "internal_id"];
const filteredSchema = unwrap(createSchema(
  Object.fromEntries(
    originalSchema.columns
      .filter(col => !columnsToRemove.includes(col.name))
      .map(col => [col.name, col.dtype])
  )
));
```

### Renaming Columns

```typescript
const renameMap = {
  "old_name": "new_name",
  "user_id": "id"
};

const renamedSchema = unwrap(createSchema(
  Object.fromEntries(
    originalSchema.columns.map(col => [
      renameMap[col.name] || col.name,
      col.dtype
    ])
  )
));
```

## Schema Validation

### Validate Data Against Schema

```typescript
function validateRecord(record: Record<string, unknown>, schema: Schema): string[] {
  const errors: string[] = [];
  
  for (const col of schema.columns) {
    const value = record[col.name];
    
    // Check nullability
    if (value === null && !col.dtype.nullable) {
      errors.push(`Column ${col.name} cannot be null`);
      continue;
    }
    
    // Check type (simplified)
    if (value !== null) {
      const expectedType = getExpectedType(col.dtype.kind);
      if (typeof value !== expectedType) {
        errors.push(`Column ${col.name} expected ${expectedType}, got ${typeof value}`);
      }
    }
  }
  
  return errors;
}

function getExpectedType(kind: DTypeKind): string {
  switch (kind) {
    case DTypeKind.Int8:
    case DTypeKind.Int16:
    case DTypeKind.Int32:
    case DTypeKind.Float32:
    case DTypeKind.Float64:
      return "number";
    case DTypeKind.Int64:
    case DTypeKind.UInt64:
      return "bigint";
    case DTypeKind.Boolean:
      return "boolean";
    case DTypeKind.String:
      return "string";
    case DTypeKind.Date:
    case DTypeKind.Timestamp:
      return "object"; // Date
    default:
      return "unknown";
  }
}
```

## Schema Serialization

### To JSON

```typescript
function schemaToJSON(schema: Schema): object {
  return {
    columns: schema.columns.map(col => ({
      name: col.name,
      dtype: {
        kind: col.dtype.kind,
        nullable: col.dtype.nullable
      },
      offset: col.offset
    })),
    rowSize: schema.rowSize,
    columnCount: schema.columnCount
  };
}
```

### From JSON

```typescript
import { DTypeKind, createSchema, DType } from "molniya";

function schemaFromJSON(json: any): Schema {
  const spec: Record<string, DType> = {};
  
  for (const col of json.columns) {
    const dtype = createDTypeFromKind(col.dtype.kind, col.dtype.nullable);
    spec[col.name] = dtype;
  }
  
  return unwrap(createSchema(spec));
}

function createDTypeFromKind(kind: DTypeKind, nullable: boolean): DType {
  const typeMap: Record<DTypeKind, DType> = {
    [DTypeKind.Int8]: DType.int8,
    [DTypeKind.Int16]: DType.int16,
    [DTypeKind.Int32]: DType.int32,
    [DTypeKind.Int64]: DType.int64,
    [DTypeKind.UInt8]: DType.uint8,
    [DTypeKind.UInt16]: DType.uint16,
    [DTypeKind.UInt32]: DType.uint32,
    [DTypeKind.UInt64]: DType.uint64,
    [DTypeKind.Float32]: DType.float32,
    [DTypeKind.Float64]: DType.float64,
    [DTypeKind.Boolean]: DType.boolean,
    [DTypeKind.String]: DType.string,
    [DTypeKind.Date]: DType.date,
    [DTypeKind.Timestamp]: DType.timestamp,
    [DTypeKind.Null]: DType.nullable.int32  // Fallback
  };
  
  const baseType = typeMap[kind];
  return nullable ? DType.nullable[kindToProperty(kind)] : baseType;
}
```

## Common Patterns

### Schema Builder

```typescript
class SchemaBuilder {
  private columns: Array<{ name: string; dtype: DType }> = [];
  
  addInt32(name: string, nullable = false): this {
    this.columns.push({ name, dtype: nullable ? DType.nullable.int32 : DType.int32 });
    return this;
  }
  
  addString(name: string, nullable = false): this {
    this.columns.push({ name, dtype: nullable ? DType.nullable.string : DType.string });
    return this;
  }
  
  addFloat64(name: string, nullable = false): this {
    this.columns.push({ name, dtype: nullable ? DType.nullable.float64 : DType.float64 });
    return this;
  }
  
  build(): Schema {
    const spec = Object.fromEntries(
      this.columns.map(c => [c.name, c.dtype])
    );
    return unwrap(createSchema(spec));
  }
}

// Usage
const schema = new SchemaBuilder()
  .addInt32("id")
  .addString("name")
  .addString("email", true)  // nullable
  .addFloat64("amount")
  .build();
```

### Schema from Sample Data

```typescript
function inferSchema(records: Record<string, unknown>[]): Schema {
  if (records.length === 0) {
    throw new Error("Cannot infer schema from empty records");
  }
  
  const spec: Record<string, DType> = {};
  const sample = records[0];
  
  for (const [key, value] of Object.entries(sample)) {
    spec[key] = inferType(value);
  }
  
  return unwrap(createSchema(spec));
}

function inferType(value: unknown): DType {
  if (value === null) return DType.nullable.string;
  
  switch (typeof value) {
    case "number":
      return Number.isInteger(value) ? DType.int32 : DType.float64;
    case "string":
      return DType.string;
    case "boolean":
      return DType.boolean;
    case "bigint":
      return DType.int64;
    default:
      return DType.nullable.string;
  }
}
```
