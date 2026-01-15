/**
 * Mornye Demo - Quick showcase of library capabilities
 *
 * Usage: bun run examples/demo.ts
 */

import { DataFrame, Series, m, readCsv } from '../src';

console.log('╔════════════════════════════════════════╗');
console.log('║           Mornye Demo v0.0.1           ║');
console.log('╚════════════════════════════════════════╝\n');

// ─────────────────────────────────────────────────────────────
// 1. Type System (Elysia-style)
// ─────────────────────────────────────────────────────────────
console.log('📋 Type System (Elysia-style)');
console.log('─'.repeat(40));

const schema = {
  id: m.int32(),
  name: m.string(),
  score: m.float64(),
  active: m.bool(),
} as const;

console.log('Schema:', JSON.stringify(schema, null, 2));
console.log('');

// ─────────────────────────────────────────────────────────────
// 2. Series - Typed 1D Arrays
// ─────────────────────────────────────────────────────────────
console.log('📊 Series Examples');
console.log('─'.repeat(40));

const numbers = Series.float64([1.5, 2.5, 3.5, 4.5, 5.5]);
console.log('\nFloat64 Series:');
numbers.print();

const names = Series.string(['Alice', 'Bob', 'Carol']);
console.log('\nString Series:');
names.print();

console.log('\nSlicing (zero-copy):');
const sliced = numbers.slice(1, 4);
console.log(`Original: [${[...numbers].join(', ')}]`);
console.log(`Sliced:   [${[...sliced].join(', ')}]`);
console.log('');

// ─────────────────────────────────────────────────────────────
// 3. DataFrame - Typed Columnar Data
// ─────────────────────────────────────────────────────────────
console.log('📑 DataFrame Examples');
console.log('─'.repeat(40));

const df = DataFrame.from(schema, [
  { id: 1, name: 'Alice', score: 95.5, active: true },
  { id: 2, name: 'Bob', score: 87.2, active: false },
  { id: 3, name: 'Carol', score: 91.8, active: true },
  { id: 4, name: 'David', score: 79.3, active: true },
  { id: 5, name: 'Eve', score: 88.9, active: false },
]);

console.log('\nFull DataFrame:');
df.print();

console.log('\nSelect columns (name, score):');
df.select('name', 'score').print();

console.log('\nHead (first 2 rows):');
df.head(2).print();

console.log('\nIterating over rows:');
for (const row of df.rows()) {
  if (row.score > 90) {
    console.log(`  ${row.name}: ${row.score} ⭐`);
  }
}

// ─────────────────────────────────────────────────────────────
// 4. CSV I/O Demo
// ─────────────────────────────────────────────────────────────
console.log('\n📁 CSV I/O Demo');
console.log('─'.repeat(40));

// Create a test CSV file
const csvPath = './examples/demo.csv';
const csvContent = `name,age,salary,hired
Alice,25,75000.50,true
Bob,30,82000.00,true
Carol,28,78500.25,false
David,35,95000.00,true`;

await Bun.write(csvPath, csvContent);
console.log(`\nCreated test CSV at ${csvPath}`);

// Read with auto type inference
const { df: csvDf } = await readCsv(csvPath);
console.log('\nLoaded CSV with auto type inference:');
csvDf.print();

console.log('\nColumn types:');
for (const col of csvDf.columns()) {
  console.log(`  ${String(col)}: ${csvDf.col(col).dtype.kind}`);
}

// Cleanup
await Bun.$`rm ${csvPath}`;

console.log('\n✅ Demo complete!');
