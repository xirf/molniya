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
import { readCsv } from 'mornye';

// 1. Load data (Lazy parsing by default)
const { df } = await readCsv('./bitcoin_history.csv');

// 2. Inspect the first few rows
df.print(5);

// 3. Perform analysis
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