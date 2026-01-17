<p align="center">
  <img src="docs/public/logo.png" width="64" height="64" />
</p>
<h1 align="center">Mornye</h1>
<p align="center"><b>Ergonomic data analysis for TypeScript.</b></p>

<br />

## Install

```bash
bun add mornye
```

## Quick Start

```typescript
import { readCsv, DataFrame } from 'mornye';

// Load from CSV
const { df } = await readCsv('./data.csv');

// Or create manually
const df2 = DataFrame.fromColumns({
  name: ['Alice', 'Bob', 'Carol'],
  age: [25, 30, 22],
  score: [95.5, 87.2, 91.8]
});

// Inspect
df2.print();

// Analyze
const highScores = df2
  .filter(row => row.score > 90)
  .select(['name', 'score']);
```

## Features

- 🧊 **Type-safe** — Full TypeScript support with inferred column types
- 🎯 **Familiar API** — If you know pandas or Polars, you'll feel at home
- 📦 **Zero dependencies** — Lightweight and portable

## Docs

📖 **[Read the documentation](https://xirf.github.io/mornye)**

## Development

```bash
bun install
bun test
bun run docs:dev  # Start docs server
```

## License

MIT