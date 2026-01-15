import { DataFrame } from '../src';

// Manual DataFrame initialization - no schema needed!
const df = DataFrame.fromColumns({
  age: [25, 30, 22, 28],
  name: ['Alice', 'Bob', 'Carol', 'Dave'],
  score: [95.5, 87.2, 91.8, 88.0],
  active: [true, false, true, true],
});

console.log('DataFrame created from columns:');
df.print();

console.log('\nColumn types:');
for (const colName of df.columns()) {
  const col = df.col(colName);
  console.log(`  ${String(colName)}: ${col.dtype.kind}`);
}

console.log('\nFiltered (age > 25):');
df.filter((row) => row.age > 25).print();

console.log('\nSorted by score (descending):');
df.sort('score', false).print();
