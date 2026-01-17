# Core Concepts

To use Molniya effectively, it helps to understand how it views your data. If you're coming from SQL or Excel, this will feel familiar. If you're coming from plain JavaScript arrays, it's a powerful upgrade.

## The Mental Model

Think of a **DataFrame** as a high-performance array of objects, but stored in a way that makes math and filtering incredibly fast.

### DataFrame
A `DataFrame` is a two-dimensional table with labeled columns.

| name (string) | age (int32) | active (bool) |
| :------------ | :---------- | :------------ |
| "Alice"       | 25          | true          |
| "Bob"         | 30          | false         |

It knows the **shape** of your data (rows × columns) and the **data type** of each column.

### Series
A `Series` is a single column from that table.

| age (int32) |
| :---------- |
| 25          |
| 30          |

In Molniya, a DataFrame is essentially a collection of aligned Series. When you select a column, you get a Series back.

---

## Type Inference

One of Molniya's strongest features is its ability to guess your data types so you don't have to type them manually.

When you load data (from CSV, JSON, or objects), Molniya scans the values to determine the best fit:

| Input Value           | Inferred Type | Description                                          |
| :-------------------- | :------------ | :--------------------------------------------------- |
| `"Hello"`, `"AX-102"` | `string`      | Text data                                            |
| `42`, `-10`           | `int32`       | Integers (if ALL values in column are whole numbers) |
| `42.5`, `10.0`        | `float64`     | Decimals (or mixed int/float)                        |
| `true`, `false`       | `bool`        | Boolean logic                                        |
| `null`, `undefined`   | `null`        | Missing values (handled gracefully)                  |

> [!IMPORTANT]
> **Why strict types?**
> By strictly enforcing types per column, Molniya prevents common JS bugs like adding a string `"10"` to a number `20` and getting `"1020"`. In Molniya, the math just works.

---

## Immutability by Design

Molniya follows a functional programming paradigm. **DataFrames are immutable.**

When you perform an operation like `.filter()` or `.sort()`, you are **not** changing the original variable. Instead, you get a brand new DataFrame returned to you.

### Why is this good?
1.  **Predictability**: You can pass `df` to ten different functions, and none of them can secretly mess up your data for the others.
2.  **Chaining**: It enables the elegant method chaining you saw in the Getting Started guide.
3.  **Debugging**: You can inspect the state of your data at any step in the pipeline.

```typescript
const original = DataFrame.fromColumns({ a: [1, 2, 3] });

// 'filtered' is a NEW DataFrame
const filtered = original.filter(row => row.a > 1);

// 'original' is completely untouched
console.log(original.height); // 3
console.log(filtered.height); // 2
```

---

## Memory & Performance

Molniya is designed to be efficient, but it's helpful to know what's happening under the hood.

- **Columnar Storage**: Data is stored by column, not by row. This is why calculating the `mean()` of a column is instant—the CPU can just zip through a single contiguous array of numbers.
- **Lazy vs Eager**: Most standard DataFrame methods (like `filter`, `sort`) are **eager**—they happen immediately. Iterating through rows is optimized, but for massive datasets (millions of rows), you should be mindful of memory usage.
- **Copy-on-Write**: While operations return new DataFrames, Molniya tries to share underlying data buffers where possible to save memory.

## Summary

- **DataFrame** = Table (collection of Series).
- **Series** = Column (typed array).
- **Types** are inferred but enforced.
- **Immutability** means operations return new copies, keeping your source data safe.

