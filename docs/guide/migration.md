# Migration Guide

Coming from pandas (Python), Polars, or SQL? Here is your cheat sheet.

## From pandas (Python)

| Operation      | pandas (Python)                   | Molniya (TypeScript)                       |
| :------------- | :-------------------------------- | :---------------------------------------- |
| **Read CSV**   | `pd.read_csv("data.csv")`         | `readCsv("data.csv")`                     |
| **Inspect**    | `df.head()`                       | `df.head().print()`                       |
| **Select Col** | `df["price"]`                     | `df.col("price")`                         |
| **Filter**     | `df[df["age"] > 18]`              | `df.filter(r => r.age > 18)`              |
| **Sort**       | `df.sort_values("date")`          | `df.sort("date")`                         |
| **New Column** | `df["total"] = df["a"] + df["b"]` | `df = df.assign("total", r => r.a + r.b)` |
| **GroupBy**    | `df.groupby("dept").sum()`        | `df.groupby("dept").sum("val")`           |
| **Map**        | `df["a"].map(fn)`                 | `df.col("a").map(fn)`                     |
| **Unique**     | `df["a"].unique()`                | `df.col("a").unique()`                    |

> [!NOTE]
> **Key Difference**: pandas modifies dataframes in-place for many operations (or has `inplace=True`). Molniya is **always immutable**. You must assign the result:
> `df = df.sort(...)`

## From SQL

| Concept      | SQL                      | Molniya                                |
| :----------- | :----------------------- | :------------------------------------ |
| **SELECT**   | `SELECT name, age`       | `.select('name', 'age')`              |
| **WHERE**    | `WHERE age > 18`         | `.filter(r => r.age > 18)`            |
| **ORDER BY** | `ORDER BY date DESC`     | `.sort('date', false)`                |
| **LIMIT**    | `LIMIT 5`                | `.head(5)`                            |
| **GROUP BY** | `GROUP BY dept`          | `.groupby('dept')`                    |
| **HAVING**   | `HAVING count > 10`      | `.groupby(...).count().filter(...)`   |
| **JOIN**     | `LEFT JOIN other ON ...` | `.merge(other, { how: 'left', ... })` |

## From Plain JavaScript (Arrays)

| Operation  | Array of Objects           | Molniya DataFrame              |
| :--------- | :------------------------- | :---------------------------- |
| **Memory** | Heavy (objects per row)    | Efficient (typed arrays)      |
| **Filter** | `arr.filter(fn)`           | `df.filter(fn)`               |
| **Map**    | `arr.map(fn)`              | `df.assign(name, fn)`         |
| **Sort**   | `arr.sort((a,b) => a - b)` | `df.sort('col')`              |
| **Stats**  | Manual (reduce loops)      | `.mean()`, `.sum()`, `.std()` |

## Type Safety

One major upgrade from pandas is strict typing.

**pandas**:
Columns can contain mixed types ("10", 10, None) silently.

**Molniya**:
Columns are strictly typed. If a column is `int32`, you cannot put a string in it. This prevents an entire class of "cleaning" bugs.

