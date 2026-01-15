<p align="center">
<img src="https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Ice/Color/ice_color.svg" width="64" height="64" />
</p>
<h1 align="center">Mornye</h1>
<p align="center"><b>Ergonomic data analysis.</b></p>


<br />

## 📦 Installation

```bash
bun add mornye
```

## 🏁 Quick Start

```typescript
import { readCsv, DataFrame } from 'mornye';

// 1. Load data from CSV
const { df } = await readCsv('./bitcoin_history.csv');

// 2. Or create manually
const df2 = DataFrame.fromColumns({
  age: [25, 30, 22],
  name: ['Alice', 'Bob', 'Carol'],
  score: [95.5, 87.2, 91.8]
});

// 3. Inspect the data
df2.print();

// 4. Perform analysis
const highVolume = df
  .filter(row => row.Volume > 1000)
  .select(['Timestamp', 'Close', 'Volume']);

console.log(`Found ${highVolume.height} rows with high volume.`);
```

## 🛠 Development

To contribute or run the benchmarks locally:

```bash
# Install dependencies
bun install

# Run tests
bun test
```