# I/O

Functions for reading and writing data.

## Reading

### `readCsv(path, options?)`

Read a CSV file into a DataFrame.

```typescript
const { df } = await readCsv('./data.csv');
```

**Options:**

| Option       | Type      | Default    | Description                       |
| ------------ | --------- | ---------- | --------------------------------- |
| `delimiter`  | `string`  | `","`      | Field delimiter                   |
| `hasHeader`  | `boolean` | `true`     | First row is header               |
| `maxRows`    | `number`  | `Infinity` | Maximum rows to read              |
| `sampleRows` | `number`  | `100`      | Rows to sample for type inference |
| `schema`     | `Schema`  | auto       | Explicit column types             |

### `scanCsv(path, options?)`

Create a LazyFrame for large files (on-demand loading).

```typescript
const lazy = await scanCsv('./huge_file.csv');
const first10 = await lazy.head(10);
```

## Writing

### `toCsv(df)`

Convert DataFrame to CSV string.

```typescript
const csv = toCsv(df);
```

### `writeCsv(df, path)`

Write DataFrame to CSV file.

```typescript
await writeCsv(df, './output.csv');
```

### `df.toJson()`

Get DataFrame as JSON string.

```typescript
const json = df.toJson();
```

### `df.toJsonRecords()`

Get DataFrame as array of row objects.

```typescript
const records = df.toJsonRecords();
// [{ name: 'Alice', age: 25 }, ...]
```
