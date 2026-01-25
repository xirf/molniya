/**
 * Demo: String Operations in Molniya
 * Shows dictionary-encoded string transformations
 */

import { DType, formatDataFrame, from, strContains, strLen, strLower, strUpper } from '../src';

console.log('='.repeat(60));
console.log('String Operations Demo');
console.log('='.repeat(60));

// Create sample data
const result = from({
  name: { data: ['Alice', 'BOB', 'Charlie'], dtype: DType.String },
  email: {
    data: ['alice@example.com', 'bob@test.com', 'charlie@example.com'],
    dtype: DType.String,
  },
});

if (!result.ok) {
  console.error('Error:', result.error);
  process.exit(1);
}

const df = result.data;

console.log('\n📋 Original:');
console.log(formatDataFrame(df));

console.log('\n🔤 strLower():');
const lower = strLower(df, 'name');
if (lower.ok) console.log(formatDataFrame(lower.data));

console.log('\n🔠 strUpper():');
const upper = strUpper(df, 'email');
if (upper.ok) console.log(formatDataFrame(upper.data));

console.log('\n🔍 strContains():');
const contains = strContains(df, 'email', 'example');
if (contains.ok) console.log(formatDataFrame(contains.data));

console.log('\n📏 strLen():');
const len = strLen(df, 'name');
if (len.ok) console.log(formatDataFrame(len.data));

console.log('\n✅ All string operations completed!');
