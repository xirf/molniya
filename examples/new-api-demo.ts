/**
 * Demo: New Type Inference and Throwing API
 *
 * Shows the simplified API with schema type inference
 */

import { filter, fromArrays, select } from '../src/index';
import type { InferSchemaType } from '../src/index';

console.log('=== New API Demo ===\n');

console.log('1. Creating DataFrame with type inference:');
const df = fromArrays({
  dept: ['Sales', 'Sales', 'Eng', 'Eng', 'HR'],
  salary: [60000, 65000, 90000, 95000, 55000],
  active: [true, true, true, false, true],
});

// Type is now: DataFrame<InferSchemaType<{ dept: "string", salary: "float64", active: "bool" }>>
console.log(df.toString());

console.log('\n2. Filtering (throws on error):');
const highEarners = filter(df, 'salary', '>', 70000);
console.log(highEarners.toString());

console.log('\n3. Selecting columns:');
const result = select(highEarners, ['dept', 'salary']);
console.log(result.toString());

console.log('\n4. Error handling with try/catch:');
try {
  const df2 = fromArrays({
    name: ['Alice', 'Bob'],
    age: [25, 30],
  });

  // This will throw if column doesn't exist
  filter(df2, 'nonexistent', '==', 10);
} catch (error) {
  console.log('✓ Caught error as expected:', (error as Error).message);
}

console.log('\n✅ All operations work with clean throwing API!');
console.log('   - No Result unwrapping needed');
console.log('   - Schema types inferred automatically');
console.log('   - Use try/catch for error handling');
