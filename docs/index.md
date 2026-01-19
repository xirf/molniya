---
title: Molniya - Ergonomic data analysis for TypeScript
layout: page
sidebar: false
---

<Hero />
<EYN>

```ts twoslash
// @noErrors
import { readCsv } from "molniya";
// ---cut---
const { df } = await readCsv("data.csv");
// df.col("invalid") ➔ Error!
// df.col("price").sum() ➔ Correctly typed
```

</EYN>

<section class="benchmarks">
  <h2>The "Server-Side First" Balance</h2>
  <p>
    We prioritize stability and memory efficiency for production servers. 
    While we can't beat Rust's raw speed yet, we deliver performance that 
    **outperforms optimized Python tools** while using <b>10x less memory</b> than high-performance engines.
  </p>


<BenchmarkComparison />

</section>

<Showcase>

<template #csv>

```ts twoslash
// @noErrors
// @noErrors
// ---cut---
import { readCsv } from "molniya";

// SIMD-accelerated reading
const { df } = await readCsv("bitcoin_7m_rows.csv", {
  delimiter: ",",
  hasHeader: true,
});

console.log(df.shape); // [7381118, 8]
console.log(df.head(5));
```

</template>

<template #filtering>

```ts twoslash
// @noErrors
import { DataFrame } from "molniya";
const df = DataFrame.fromColumns({
  timestamp: [1],
  price: [50000],
  volume: [1.2],
  category: ["A", "B", "C"],
});
// ---cut---
// Expressive filtering
const filtered = df
  .where((col) => col("price").gt(50000))
  .select("timestamp", "price", "volume");

console.log(filtered.shape);
filtered.print();
```

</template>

<template #groupby>

```ts twoslash
// @noErrors
import { DataFrame } from "molniya";
const df = DataFrame.fromColumns({
  product: ["Laptop", "Mouse", "Monitor", "Keyboard"],
  category: ["Electronics", "Accessories", "Electronics", "Accessories"],
  price: [999.99, 29.99, 199.99, 59.99],
  rating: [4.5, 4.2, 4.8, 3.9],
});
// ---cut---
// SQL-like aggregations
const summary = df.groupby("category").agg({
  price: "mean",
  rating: "mean",
});

summary.print();
```

</template>

<template #types>

```ts twoslash
// @errors: 2345
import { DataFrame } from "molniya";
// ---cut---
// Full IDE support
const df = DataFrame.fromColumns({
  product: ["Laptop", "Mouse", "Monitor", "Keyboard"],
  category: ["Electronics", "Accessories", "Electronics", "Accessories"],
  price: [999.99, 29.99, 199.99, 59.99],
  rating: [4.5, 4.2, 4.8, 3.9],
});

// Error: Column 'non_existent' not found in Schema
df.col("non_existent").mean();

// Success: Auto-complete working
df.col("price").std();
```

</template>

</Showcase>

<Footer />
