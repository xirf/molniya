# CSV I/O API

Fast CSV reading with automatic type inference.

## readCsv

The primary CSV reader, optimized for performance.

```typescript
import { readCsv, m } from 'mornye';

// Auto-infer types
const df = await readCsv('./data.csv');

// With explicit schema
const df2 = await readCsv('./data.csv', {
  schema: {
    id: m.int32(),
    name: m.string(),
    price: m.float64(),
  }
});
```

## Options

| Option       | Type      | Default    | Description                       |
| ------------ | --------- | ---------- | --------------------------------- |
| `schema`     | `Schema`  | -          | Explicit column types             |
| `delimiter`  | `number`  | `44` (`,`) | Field delimiter byte              |
| `hasHeader`  | `boolean` | `true`     | First row is header               |
| `sampleRows` | `number`  | `100`      | Rows to sample for type inference |
| `maxRows`    | `number`  | `Infinity` | Maximum rows to read              |

```typescript
const df = await readCsv('./data.csv', {
  delimiter: 59,     // semicolon (;)
  hasHeader: false,
  maxRows: 1000,
});
```

## Type Inference

Mornye automatically detects types from data:

| Detected Pattern       | Type      |
| ---------------------- | --------- |
| Integer numbers        | `int32`   |
| Decimal numbers        | `float64` |
| `true`/`false`/`1`/`0` | `bool`    |
| Everything else        | `string`  |

## Performance

| File Size | Rows | Time   |
| --------- | ---- | ------ |
| 10MB      | 100K | ~55ms  |
| 100MB     | 1M   | ~550ms |

**Optimizations:**
- SIMD-accelerated line finding (`Buffer.indexOf`)
- Byte-level CSV parsing (no string splits)
- Pre-allocated TypedArrays
- Direct columnar construction

## readCsv (Streaming)

For files with complex quoting (newlines in fields):

```typescript
import { readCsv } from 'mornye';

const df = await readCsv('./complex.csv');
```

This uses a state-machine parser that handles:
- Quoted fields with embedded commas
- Escaped quotes (`""`)
- Newlines inside quoted fields (RFC 4180)
