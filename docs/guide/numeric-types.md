# Numeric Types

Molniya provides a comprehensive set of numeric types optimized for different use cases, from small integers to high-precision floating point numbers.

## Type Overview

| Type | Storage | Range | JavaScript Type |
|------|---------|-------|-----------------|
| `int8` | 1 byte | -128 to 127 | `number` |
| `int16` | 2 bytes | -32,768 to 32,767 | `number` |
| `int32` | 4 bytes | -2,147,483,648 to 2,147,483,647 | `number` |
| `int64` | 8 bytes | ±9,223,372,036,854,775,807 | `bigint` |
| `uint8` | 1 byte | 0 to 255 | `number` |
| `uint16` | 2 bytes | 0 to 65,535 | `number` |
| `uint32` | 4 bytes | 0 to 4,294,967,295 | `number` |
| `uint64` | 8 bytes | 0 to 18,446,744,073,709,551,615 | `bigint` |
| `float32` | 4 bytes | ~7 decimal digits | `number` |
| `float64` | 8 bytes | ~15 decimal digits | `number` |

## Integer Types

### Signed Integers

Use signed integers when you need to represent both positive and negative values:

```typescript
import { DType } from "Molniya";

const schema = {
  // Small counters, flags (-128 to 127)
  status_code: DType.int8,
  
  // Medium range values (-32K to 32K)
  quantity: DType.int16,
  
  // General purpose integers (-2B to 2B)
  user_id: DType.int32,
  
  // Large integers (returns bigint)
  transaction_id: DType.int64
};
```

### Unsigned Integers

Use unsigned integers when you only need positive values - they provide twice the positive range:

```typescript
const schema = {
  // Small positive values (0 to 255)
  age: DType.uint8,
  
  // Medium positive values (0 to 65K)
  year: DType.uint16,
  
  // Large positive values (0 to 4B)
  record_count: DType.uint32,
  
  // Very large positive values (returns bigint)
  total_bytes: DType.uint64
};
```

## Floating Point Types

### float32 (Single Precision)

Use for memory-efficient storage when ~7 decimal digits of precision is sufficient:

```typescript
const schema = {
  // Good for: sensor readings, ratings, percentages
  temperature: DType.float32,
  rating: DType.float32,
  progress_percent: DType.float32
};
```

### float64 (Double Precision)

Use when you need maximum precision (~15 decimal digits):

```typescript
const schema = {
  // Good for: financial calculations, scientific data
  price: DType.float64,
  latitude: DType.float64,
  longitude: DType.float64,
  account_balance: DType.float64
};
```

## Type Selection Guide

### By Data Characteristics

| Data Type | Recommended Type | Reason |
|-----------|-----------------|--------|
| IDs (auto-increment) | `int32` or `int64` | Sufficient range, efficient storage |
| Age | `uint8` | 0-255 covers all human ages |
| Year | `uint16` | 0-65535 covers all historical dates |
| Price/Amount | `float64` | Precision for currency calculations |
| Counts | `int32` | Handles most counting scenarios |
| Percentages | `float32` | 7 digits is plenty for 0-100% |
| Coordinates | `float64` | GPS needs high precision |

### By Memory Constraints

For large datasets, choose the smallest type that fits your data:

```typescript
// Memory-efficient schema for a large dataset
const compactSchema = {
  // Use smallest types that fit the data
  age: DType.uint8,        // vs int32 (75% savings)
  score: DType.uint16,     // vs int32 (50% savings)
  rating: DType.float32,   // vs float64 (50% savings)
  active: DType.boolean    // vs int8 (87% savings)
};
```

## Nullable Numeric Types

All numeric types support nullable variants:

```typescript
const schema = {
  // Non-nullable (required)
  id: DType.int32,
  
  // Nullable (may be null)
  age: DType.nullable.uint8,
  salary: DType.nullable.float64,
  score: DType.nullable.float32
};
```

## Arithmetic Operations

Numeric columns support standard arithmetic:

```typescript
import { col, add, sub, mul, div } from "Molniya";

// Basic arithmetic
df.withColumn("total", col("price").mul(col("quantity")))
  .withColumn("discount", col("total").mul(0.1))
  .withColumn("final", col("total").sub(col("discount")))

// Using operator functions
df.withColumn("avg", div(col("sum"), col("count")))
```

## Type Coercion

When combining different numeric types in expressions, Molniya follows these rules:

1. **int + float** → float (widest type wins)
2. **int32 + int64** → int64 (largest storage wins)
3. **float32 + float64** → float64 (highest precision wins)

```typescript
// Result is float64
const total = col("int_price").add(col("float_tax"))  // float64

// Result is int64
const combined = col("int32_id").add(col("int64_big"))  // int64
```

## Performance Considerations

### SIMD Optimization

Molniya uses SIMD instructions for batch operations on numeric columns. This is most effective with:
- `int32` and `float32` (128-bit vectors process 4 values at once)
- `float64` (128-bit vectors process 2 values at once)

### Cache Efficiency

Smaller types improve cache locality:
- `int8` arrays fit 4x more values in cache than `int32`
- `float32` arrays fit 2x more values than `float64`

Choose smaller types when precision requirements allow.

## Overflow Behavior

Integer arithmetic wraps on overflow (standard JavaScript behavior for TypedArrays):

```typescript
// int8 overflow example
// 127 + 1 = -128 (wraps around)
```

For safe arithmetic with large numbers, use `int64` or `float64`.

## Best Practices

1. **Start with int32/float64**: Use these for prototyping, optimize later if needed
2. **Consider data range**: Choose the smallest type that safely holds your data
3. **Use unsigned for counts**: `uint32` for row counts, `uint64` for file sizes
4. **Prefer float64 for money**: Financial calculations need the precision
5. **Document type choices**: Add comments explaining why unusual types were chosen
