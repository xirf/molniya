/**
 * Demo: Row Operations in Molniya
 * Shows append, duplicate, dropDuplicates, unique, and join
 */

import {
  DType,
  append,
  dropDuplicates,
  duplicate,
  formatDataFrame,
  from,
  join,
  unique,
} from '../src';

console.log('='.repeat(60));
console.log('Row Operations Demo');
console.log('='.repeat(60));

// Create sample data
const result = from({
  name: { data: ['Alice', 'Bob', 'Charlie'], dtype: DType.String },
  age: { data: [25, 30, 35], dtype: DType.Int32 },
  city: { data: ['NYC', 'LA', 'Chicago'], dtype: DType.String },
});

if (!result.ok) {
  console.error('Error:', result.error);
  process.exit(1);
}

const df = result.data;

console.log('\n📋 Original DataFrame:');
console.log(formatDataFrame(df));

// append()
console.log('\n\n➕ append() - Add new rows:');
const appendResult = append(df, [
  { name: 3, age: 28, city: 3 }, // Diana in Boston (dict IDs)
  { name: 4, age: 32, city: 4 }, // Eve in Seattle
]);
if (appendResult.ok) {
  console.log(formatDataFrame(appendResult.data));
}

// duplicate()
console.log('\n\n📑 duplicate() - Deep copy DataFrame:');
const dupResult = duplicate(df);
if (dupResult.ok) {
  console.log(formatDataFrame(dupResult.data));
  console.log('✅ Copy is identical but independent');
}

// dropDuplicates()
console.log('\n\n🗑️  dropDuplicates() - Remove duplicates:');
const dupData = from({
  name: { data: ['Alice', 'Bob', 'Alice', 'Charlie', 'Bob'], dtype: DType.String },
  age: { data: [25, 30, 25, 35, 30], dtype: DType.Int32 },
  city: { data: ['NYC', 'LA', 'NYC', 'Chicago', 'LA'], dtype: DType.String },
});

if (dupData.ok) {
  console.log('\nWith duplicates:');
  console.log(formatDataFrame(dupData.data));

  const deduped = dropDuplicates(dupData.data);
  if (deduped.ok) {
    console.log('\nAfter dropDuplicates() [keep=first]:');
    console.log(formatDataFrame(deduped.data));
  }

  const dedupedLast = dropDuplicates(dupData.data, { keep: 'last' });
  if (dedupedLast.ok) {
    console.log('\nAfter dropDuplicates() [keep=last]:');
    console.log(formatDataFrame(dedupedLast.data));
  }

  const dedupedSubset = dropDuplicates(dupData.data, { subset: ['name'] });
  if (dedupedSubset.ok) {
    console.log('\nAfter dropDuplicates() [subset=[name]]:');
    console.log(formatDataFrame(dedupedSubset.data));
  }
}

// unique()
console.log('\n\n✨ unique() - Get unique rows:');
const uniqueResult = unique(dupData.ok ? dupData.data : df);
if (uniqueResult.ok) {
  console.log(formatDataFrame(uniqueResult.data));
}

// join()
console.log('\n\n🔗 join() - Join on index:');
const left = from({
  name: { data: ['Alice', 'Bob', 'Charlie'], dtype: DType.String },
  age: { data: [25, 30, 35], dtype: DType.Int32 },
});

const right = from({
  city: { data: ['NYC', 'LA'], dtype: DType.String },
  salary: { data: [100000, 120000], dtype: DType.Int32 },
});

if (left.ok && right.ok) {
  console.log('\nLeft DataFrame:');
  console.log(formatDataFrame(left.data));

  console.log('\nRight DataFrame:');
  console.log(formatDataFrame(right.data));

  const joined = join(left.data, right.data, { how: 'left' });
  if (joined.ok) {
    console.log('\nAfter join() [how=left]:');
    console.log(formatDataFrame(joined.data));
    console.log('✅ Charlie has null city/salary (no match in right)');
  }

  const innerJoined = join(left.data, right.data, { how: 'inner' });
  if (innerJoined.ok) {
    console.log('\nAfter join() [how=inner]:');
    console.log(formatDataFrame(innerJoined.data));
    console.log('✅ Only rows 0 and 1 match (Charlie excluded)');
  }
}

console.log(`\n${'='.repeat(60)}`);
console.log('✨ Key Features:');
console.log('  • append() adds rows with automatic null handling');
console.log('  • duplicate() creates deep copies (optional shared dictionary)');
console.log('  • dropDuplicates() removes duplicates (keep first/last)');
console.log('  • unique() is shorthand for dropDuplicates on all columns');
console.log('  • join() performs index-based joins (wrapper around merge)');
console.log('='.repeat(60));
