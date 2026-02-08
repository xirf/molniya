# DType

`DType` is the factory object for creating data type descriptors.

## Overview

```typescript
import { DType } from "molniya";

// Non-nullable types
DType.int32
DType.float64
DType.string

// Nullable types
DType.nullable.int32
DType.nullable.string
```

## Non-Nullable Types

| Property | Storage | JavaScript Type |
|----------|---------|-----------------|
| `DType.int8` | 1 byte | `number` |
| `DType.int16` | 2 bytes | `number` |
| `DType.int32` | 4 bytes | `number` |
| `DType.int64` | 8 bytes | `bigint` |
| `DType.uint8` | 1 byte | `number` |
| `DType.uint16` | 2 bytes | `number` |
| `DType.uint32` | 4 bytes | `number` |
| `DType.uint64` | 8 bytes | `bigint` |
| `DType.float32` | 4 bytes | `number` |
| `DType.float64` | 8 bytes | `number` |
| `DType.boolean` | 1 byte | `boolean` |
| `DType.string` | 4 bytes* | `string` |
| `DType.date` | 4 bytes | `Date` |
| `DType.timestamp` | 8 bytes | `bigint` |

\* String columns store a 4-byte dictionary index.

## Nullable Types

Access nullable variants through `DType.nullable`:

```typescript
DType.nullable.int32
DType.nullable.float64
DType.nullable.string
DType.nullable.boolean
// ... etc for all types
```

Nullable types allow `null` values in the column and use an additional null bitmap.

## Usage in Schemas

```typescript
import { DType } from "molniya";

const schema = {
  // Required fields
  id: DType.int32,
  name: DType.string,
  price: DType.float64,
  
  // Optional fields (can be null)
  description: DType.nullable.string,
  discount: DType.nullable.float64
};
```

## Type Interface

```typescript
interface DType<K extends DTypeKind = DTypeKind> {
  readonly kind: K;
  readonly nullable: boolean;
}
```

## Helper Functions

### isNumericDType()

Check if a type is numeric (supports arithmetic).

```typescript
import { isNumericDType, DType } from "molniya";

isNumericDType(DType.int32)    // true
isNumericDType(DType.float64)  // true
isNumericDType(DType.string)   // false
```

### isIntegerDType()

Check if a type is an integer (not floating point).

```typescript
import { isIntegerDType, DType } from "molniya";

isIntegerDType(DType.int32)    // true
isIntegerDType(DType.float64)  // false
isIntegerDType(DType.string)   // false
```

### isBigIntDType()

Check if a type uses BigInt representation.

```typescript
import { isBigIntDType, DType } from "molniya";

isBigIntDType(DType.int64)      // true
isBigIntDType(DType.uint64)     // true
isBigIntDType(DType.timestamp)  // true
isBigIntDType(DType.int32)      // false
```

### getDTypeSize()

Get the byte size of a type.

```typescript
import { getDTypeSize, DType } from "molniya";

getDTypeSize(DType.int32)    // 4
getDTypeSize(DType.float64)  // 8
getDTypeSize(DType.string)   // 4 (dictionary index)
```

### toNullable()

Convert any type to its nullable variant.

```typescript
import { toNullable, DType } from "molniya";

const nullableInt = toNullable(DType.int32);
// Equivalent to DType.nullable.int32
```

## Type Mapping

The `DTypeToTS` type maps DType kinds to TypeScript types:

```typescript
import type { DTypeToTS, DTypeKind } from "molniya";

type Int32Type = DTypeToTS<DTypeKind.Int32>  // number
type Int64Type = DTypeToTS<DTypeKind.Int64>  // bigint
type StringType = DTypeToTS<DTypeKind.String> // string
```

## See Also

- [DTypeKind](./dtype-kind) - Type identifiers enum
- [Data Types Guide](../guide/data-types) - When to use each type
