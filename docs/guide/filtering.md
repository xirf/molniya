# Filtering & Sorting

Once your data is loaded, you'll need to slice and dice it to find insights.

## Filtering Rows

Molniya offers two main ways to filter data: the flexible JavaScript way, and the explicit structural way.

### 1. The Flexible Way (`.filter`)
Use a standard JavaScript arrow function. This is the most powerful method because you can write **any** logic you want.

```typescript
// Keep users who are adults AND have an active subscription
const activeAdults = df.filter((row) => {
  return row.age >= 18 && row.subscription_status === 'active';
});
```

You can even use external helpers or complex regex:
```typescript
const gmailUsers = df.filter(row => row.email.endsWith('@gmail.com'));
```

### 2. The Structural Way (`.where`)
If you have a simple condition, `.where()` is often more readable and shorter.

```typescript
// All rows where 'status' is 'pending'
const pending = df.where('status', 'eq', 'pending');

// All rows where 'score' is greater than 80
const highScorers = df.where('score', 'gt', 80);
```

> [!TIP]
> **Performance Note**: For extremely large datasets, chaining `.where()` conditions can sometimes be faster than a complex `.filter()` callback, but for most use cases, use whichever is more readable.

---

## Sorting Data

Sorting is straightforward. By default, it sorts in **ascending** order (A-Z, 0-9).

### Basic Sort
```typescript
// Sort by price, lowest to highest
const cheapest = products.sort('price');

// Sort by price, highest to lowest (descending)
const mostExpensive = products.sort('price', 'desc');
```

### Multi-Column Sort
Need to sort by Department first, then by Salary?
*Currently, Molniya optimizes for single-column sorts. To achieve multi-sort, chain them in **reverse** order of importance.* 

```typescript
// Sort by Dept (primary), then Salary (secondary)
const sorted = employees
  .sort('salary', 'desc') // Secondary sort first
  .sort('department');    // Primary sort last (stable sort preserves order)
```

---

## Selecting & Dropping Columns

Sometimes you have too much data (`width` is too high). Narrow it down to just what you need.

### Select
Create a new DataFrame with **only** the specified columns.

```typescript
// Create a "contact list" from a massive user table
const contacts = users.select('first_name', 'last_name', 'email');
```

### Drop
Create a new DataFrame **without** specific columns. Great for removing sensitive data or temporary calculation columns.

```typescript
// Remove internal IDs and password hashes
const publicView = users.drop('internal_id', 'password_hash');
```

---

## Slicing (Head & Tail)
Quickly grab a chunk of rows from the start or end.

```typescript
// Top 10 results
df.head(10).print();

// Final 5 entries
df.tail(5).print();
```

## Chaining It All Together

The true power comes from combining these methods into a pipeline.

```typescript
const report = sales
  .filter(r => r.region === 'EU')   // 1. Filter Region
  .sort('date', false)              // 2. Sort by Date (newest first)
  .drop('internal_code')            // 3. Clean up columns
  .head(5);                         // 4. Take top 5 recent sales

report.print();
```

