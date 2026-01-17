# Best Practices

To keep your data pipelines clean, maintainable, and robust.

## 1. Define Interfaces for Row Data

Molniya infers types, but explicit interfaces make your code clearer for your team.

```typescript
// Define what your data looks like
interface Transaction {
  id: number;
  amount: number;
  currency: string;
  is_verified: boolean;
}

// Use generic to enforce typing on callbacks
const { df } = await readCsv<Transaction>('./data.csv');

// Now TypeScript knows that 'row' is a Transaction
df.filter(row => row.amount > 100); 
```

## 2. Isolate Pipelines

Don't write one 200-line script. Break your analysis into "Transformations".

```typescript
// transform.ts
export function cleanUserData(raw: DataFrame): DataFrame {
  return raw
    .dropna()
    .assign('email', r => r.email.toLowerCase());
}

export function enrichUserData(clean: DataFrame): DataFrame {
  return clean
    .assign('is_vip', r => r.spend > 1000);
}

// main.ts
const raw = await readCsv('users.csv');
const final = enrichUserData(cleanUserData(raw));
```

## 3. Think in Columns, Not Loops

This is the biggest mental shift. Avoid `for` loops.

**Anti-Pattern**:
```typescript
const ids = [];
for (const row of df.rows()) {
  ids.push(row.id + 1);
}
```

**Best Practice**:
```typescript
const ids = df.col('id').map(id => id + 1);
```

## 4. Defensive Loading

Input files change. Columns get renamed. types break.
Always assert your schema immediately after loading in production scripts.

```typescript
const { df, errors } = await readCsv('data.csv');

if (errors.length > 0) {
  console.warn(`Dropped ${errors.length} malformed rows`);
}

// Check for required columns
if (!df.columns().includes('user_id')) {
  throw new Error("Missing required column 'user_id'");
}
```

## 5. Comment Your Assumptions

Data analysis code contains magic numbers. Explain them.

```typescript
const outliers = df.filter(r => r.load_time > 30000); // 30s timeout threshold defined in SLA
```

