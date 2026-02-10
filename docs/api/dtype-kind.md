# DTypeKind API

API reference for the `DTypeKind` enum, which represents the fundamental data type categories in Molniya.

## Overview

`DTypeKind` is an enumeration that identifies the underlying storage type of a column. It is used internally for type checking and serialization.

## Enum Values

| Value | Numeric Value | Description |
|-------|---------------|-------------|
| `Int8` | 0 | 8-bit signed integer |
| `Int16` | 1 | 16-bit signed integer |
| `Int32` | 2 | 32-bit signed integer |
| `Int64` | 3 | 64-bit signed integer |
| `UInt8` | 4 | 8-bit unsigned integer |
| `UInt16` | 5 | 16-bit unsigned integer |
| `UInt32` | 6 | 32-bit unsigned integer |
| `UInt64` | 7 | 64-bit unsigned integer |
| `Float32` | 8 | 32-bit floating point |
| `Float64` | 9 | 64-bit floating point |
| `Boolean` | 10 | Boolean value |
| `String` | 11 | Dictionary-encoded string |
| `Date` | 12 | Date (days since epoch) |
| `Timestamp` | 13 | Timestamp (milliseconds since epoch) |
| `Null` | 14 | Null type |

## Usage

### Checking Column Types

```typescript
import { DTypeKind } from "molniya";

const schema = df.schema;

for (const column of schema.columns) {
  switch (column.dtype.kind) {
    case DTypeKind.Int32:
    case DTypeKind.Int64:
      console.log(`${column.name} is an integer`);
      break;
    case DTypeKind.Float32:
    case DTypeKind.Float64:
      console.log(`${column.name} is a float`);
      break;
    case DTypeKind.String:
      console.log(`${column.name} is a string`);
      break;
  }
}
```

### Type Guards

```typescript
import { DTypeKind } from "molniya";

function isNumeric(kind: DTypeKind): boolean {
  return [
    DTypeKind.Int8,
    DTypeKind.Int16,
    DTypeKind.Int32,
    DTypeKind.Int64,
    DTypeKind.UInt8,
    DTypeKind.UInt16,
    DTypeKind.UInt32,
    DTypeKind.UInt64,
    DTypeKind.Float32,
    DTypeKind.Float64
  ].includes(kind);
}

function isInteger(kind: DTypeKind): boolean {
  return [
    DTypeKind.Int8,
    DTypeKind.Int16,
    DTypeKind.Int32,
    DTypeKind.Int64,
    DTypeKind.UInt8,
    DTypeKind.UInt16,
    DTypeKind.UInt32,
    DTypeKind.UInt64
  ].includes(kind);
}
```

### Schema Inspection

```typescript
import { DTypeKind } from "molniya";

// Find all string columns
const stringColumns = df.schema.columns
  .filter(col => col.dtype.kind === DTypeKind.String)
  .map(col => col.name);

// Find all numeric columns
const numericColumns = df.schema.columns
  .filter(col => isNumeric(col.dtype.kind))
  .map(col => col.name);
```

## DTypeKind vs DType

| Aspect | DTypeKind | DType |
|--------|-----------|-------|
| **Purpose** | Category identifier | Complete type descriptor |
| **Includes nullability** | No | Yes |
| **Used for** | Type checking, serialization | Schema definition |
| **Example** | `DTypeKind.Int32` | `DType.int32`, `DType.nullable.int32` |

```typescript
import { DType, DTypeKind } from "molniya";

// DType includes nullability
const intType = DType.int32;           // non-nullable
const nullableInt = DType.nullable.int32;  // nullable

// Both have the same DTypeKind
console.log(intType.kind === DTypeKind.Int32);        // true
console.log(nullableInt.kind === DTypeKind.Int32);    // true

// But different nullability
console.log(intType.nullable);        // false
console.log(nullableInt.nullable);    // true
```

## Serialization

DTypeKind values are serialized as numbers in JSON:

```typescript
// Schema serialization
{
  "columns": [
    {
      "name": "id",
      "dtype": {
        "kind": 2,        // DTypeKind.Int32
        "nullable": false
      }
    },
    {
      "name": "name",
      "dtype": {
        "kind": 11,       // DTypeKind.String
        "nullable": false
      }
    }
  ]
}
```

## Helper Functions

### getTypeName()

Get human-readable type name:

```typescript
function getTypeName(kind: DTypeKind): string {
  const names: Record<DTypeKind, string> = {
    [DTypeKind.Int8]: "int8",
    [DTypeKind.Int16]: "int16",
    [DTypeKind.Int32]: "int32",
    [DTypeKind.Int64]: "int64",
    [DTypeKind.UInt8]: "uint8",
    [DTypeKind.UInt16]: "uint16",
    [DTypeKind.UInt32]: "uint32",
    [DTypeKind.UInt64]: "uint64",
    [DTypeKind.Float32]: "float32",
    [DTypeKind.Float64]: "float64",
    [DTypeKind.Boolean]: "boolean",
    [DTypeKind.String]: "string",
    [DTypeKind.Date]: "date",
    [DTypeKind.Timestamp]: "timestamp",
    [DTypeKind.Null]: "null"
  };
  return names[kind];
}
```

### getTypeSize()

Get storage size in bytes:

```typescript
function getTypeSize(kind: DTypeKind): number {
  switch (kind) {
    case DTypeKind.Int8:
    case DTypeKind.UInt8:
    case DTypeKind.Boolean:
      return 1;
    case DTypeKind.Int16:
    case DTypeKind.UInt16:
      return 2;
    case DTypeKind.Int32:
    case DTypeKind.UInt32:
    case DTypeKind.Float32:
    case DTypeKind.String:  // Dictionary index
    case DTypeKind.Date:
      return 4;
    case DTypeKind.Int64:
    case DTypeKind.UInt64:
    case DTypeKind.Float64:
    case DTypeKind.Timestamp:
      return 8;
    default:
      return 0;
  }
}
```

## Type Compatibility

Check if types are compatible for operations:

```typescript
import { DTypeKind } from "molniya";

function canAdd(left: DTypeKind, right: DTypeKind): boolean {
  // Numeric types can be added together
  return isNumeric(left) && isNumeric(right);
}

function canCompare(left: DTypeKind, right: DTypeKind): boolean {
  // Same types or both numeric
  return left === right || (isNumeric(left) && isNumeric(right));
}
```
