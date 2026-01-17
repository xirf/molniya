# Loading Data

## From CSV

```typescript
import { readCsv } from 'mornye';

const { df } = await readCsv('./sales.csv');
```

### Options

```typescript
const { df } = await readCsv('./data.csv', {
  delimiter: ',',    // comma (default), or ';' or '\t'
  hasHeader: true,   // first row is header
  maxRows: 1000,     // limit rows
});
```

## From Objects

Create a DataFrame from column arrays:

```typescript
const df = DataFrame.fromColumns({
  id: [1, 2, 3],
  name: ['A', 'B', 'C'],
  value: [10.5, 20.3, 15.8]
});
```

## Writing Data

### To CSV

```typescript
import { writeCsv, toCsv } from 'mornye';

// Write to file
await writeCsv(df, './output.csv');

// Or get as string
const csvString = toCsv(df);
```

### To JSON

```typescript
// As JSON string
const jsonStr = df.toJson();

// As array of objects
const records = df.toJsonRecords();
// [{ id: 1, name: 'A' }, { id: 2, name: 'B' }, ...]
```
