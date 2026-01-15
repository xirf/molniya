---
layout: home

hero:
  name: Mornye
  text: Ergonomic data analysis.
  # tagline: TypedArray-backed DataFrames with Pandas-like API for Bun.js
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: API Reference
      link: /api/series

features:
  - icon: ⚡
    title: Fast
    details: Built on TypedArrays with optimized CSV parsing for efficient data processing.
  - icon: 🔒
    title: Type-Safe
    details: Full TypeScript support with inferred types.
  - icon: 🐼
    title: Pandas-Like API
    details: Familiar operations - filter, groupby, sort, describe. Easy migration from Python.
  - icon: 📊
    title: Large File Support
    details: LazyFrame loads data on-demand, enabling work with files larger than available RAM.
  - icon: 🧩
    title: Bun-Native
    details: Leverages Bun's fast APIs for file I/O, streaming, and JSON parsing.
---

<style>
:root {
  --vp-home-hero-name-color: transparent;
  --vp-home-hero-name-background: -webkit-linear-gradient(120deg, #5c6bc0 30%, #7ACCFF);
}
</style>
