/**
 * Example demonstrating missing data handling and basic manipulation
 * All operations work directly on Uint8Array buffers for maximum performance
 */

import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  DType,
  addColumn,
  createDataFrame,
  drop,
  dropna,
  enableNullTracking,
  fillna,
  getColumn,
  getColumnNames,
  getRowCount,
  isna,
  notna,
  rename,
  setColumnValue,
  setNull,
} from '../src';
import { formatDataFrame } from '../src/dataframe/print';

console.log('='.repeat(70));
console.log('Missing Data Handling & Basic Manipulation Demo');
console.log('All operations work directly on Uint8Array buffers');
console.log('='.repeat(70));

// Example 1: Creating a DataFrame with missing values
console.log('\n📋 Example 1: Creating DataFrame with Missing Values');
console.log('─'.repeat(70));

const df = createDataFrame();
addColumn(df, 'name', DType.String, 5);
addColumn(df, 'age', DType.Int32, 5);
addColumn(df, 'salary', DType.Float64, 5);

// Set up string dictionary (both Maps)
df.dictionary!.stringToId.set('Alice', 0);
df.dictionary!.idToString.set(0, 'Alice');
df.dictionary!.stringToId.set('Bob', 1);
df.dictionary!.idToString.set(1, 'Bob');
df.dictionary!.stringToId.set('Charlie', 2);
df.dictionary!.idToString.set(2, 'Charlie');
df.dictionary!.stringToId.set('David', 3);
df.dictionary!.idToString.set(3, 'David');
df.dictionary!.stringToId.set('Eve', 4);
df.dictionary!.idToString.set(4, 'Eve');
df.dictionary!.nextId = 5;

const nameCol = getColumn(df, 'name');
const ageCol = getColumn(df, 'age');
const salaryCol = getColumn(df, 'salary');

if (!nameCol.ok || !ageCol.ok || !salaryCol.ok) {
  throw new Error('Failed to get columns');
}

// Enable null tracking
enableNullTracking(nameCol.data);
enableNullTracking(ageCol.data);
enableNullTracking(salaryCol.data);

// Set values and mark some as null
setColumnValue(nameCol.data, 0, 0); // Alice
setColumnValue(ageCol.data, 0, 30);
setColumnValue(salaryCol.data, 0, 75000.0);

setColumnValue(nameCol.data, 1, 1); // Bob
setNull(ageCol.data.nullBitmap!, 1); // Missing age
setColumnValue(salaryCol.data, 1, 80000.0);

setColumnValue(nameCol.data, 2, 2); // Charlie
setColumnValue(ageCol.data, 2, 35);
setNull(salaryCol.data.nullBitmap!, 2); // Missing salary

setNull(nameCol.data.nullBitmap!, 3); // Missing name
setColumnValue(ageCol.data, 3, 28);
setColumnValue(salaryCol.data, 3, 70000.0);

setColumnValue(nameCol.data, 4, 4); // Eve
setColumnValue(ageCol.data, 4, 32);
setColumnValue(salaryCol.data, 4, 90000.0);

console.log('Original DataFrame with missing values:\n');
console.log(formatDataFrame(df));

// Example 2: Detecting missing values with isna()
console.log('\n📊 Example 2: Detecting Missing Values with isna()');
console.log('─'.repeat(70));

const nullMask = isna(df);
if (nullMask.ok) {
  console.log('Null indicators (1 = null, 0 = not null):\n');
  console.log(formatDataFrame(nullMask.data));
}

// Example 3: Detecting non-null values with notna()
console.log('\n📊 Example 3: Detecting Non-Null Values with notna()');
console.log('─'.repeat(70));

const notNullMask = notna(df);
if (notNullMask.ok) {
  console.log('Non-null indicators (1 = not null, 0 = null):\n');
  console.log(formatDataFrame(notNullMask.data));
}

// Example 4: Drop rows with any missing values
console.log('\n🗑️  Example 4: Drop Rows with Any Missing Values (how="any")');
console.log('─'.repeat(70));

const cleanDf = dropna(df);
if (cleanDf.ok) {
  console.log(`Dropped rows with nulls. ${getRowCount(cleanDf.data)} rows remaining:\n`);
  console.log(formatDataFrame(cleanDf.data));
}

// Example 5: Drop rows only when all values are null
console.log('\n🗑️  Example 5: Drop Rows Only When All Values Are Null (how="all")');
console.log('─'.repeat(70));

const cleanAllDf = dropna(df, { how: 'all' });
if (cleanAllDf.ok) {
  console.log(
    `Dropped rows where all values are null. ${getRowCount(cleanAllDf.data)} rows remaining:\n`,
  );
  console.log(formatDataFrame(cleanAllDf.data));
}

// Example 6: Fill missing values
console.log('\n🔧 Example 6: Fill Missing Values');
console.log('─'.repeat(70));

const filledDf = fillna(df, { age: 999, salary: 0.0 });
if (filledDf.ok) {
  console.log('Filled missing age with 999 and salary with 0:\n');
  console.log(formatDataFrame(filledDf.data));
}

// Example 7: Drop columns
console.log('\n🗑️  Example 7: Drop Columns');
console.log('─'.repeat(70));

const droppedColDf = drop(df, { columns: ['salary'] });
if (droppedColDf.ok) {
  console.log('Dropped salary column:\n');
  console.log(formatDataFrame(droppedColDf.data));
}

// Example 8: Drop rows by index
console.log('\n🗑️  Example 8: Drop Rows by Index');
console.log('─'.repeat(70));

const droppedRowDf = drop(df, { index: [1, 3] });
if (droppedRowDf.ok) {
  console.log('Dropped rows 1 and 3:\n');
  console.log(formatDataFrame(droppedRowDf.data));
}

// Example 9: Rename columns
console.log('\n✏️  Example 9: Rename Columns');
console.log('─'.repeat(70));

const renamedDf = rename(df, {
  name: 'employee_name',
  age: 'employee_age',
  salary: 'annual_salary',
});
if (renamedDf.ok) {
  console.log('Renamed columns:\n');
  console.log(formatDataFrame(renamedDf.data));
}

// Example 10: Complex pipeline
console.log('\n🔄 Example 10: Complex Pipeline');
console.log('─'.repeat(70));

// Step 1: Fill missing values
let result = fillna(df, { age: 30, salary: 75000.0 });
if (!result.ok) {
  throw new Error('Fill failed');
}

// Step 2: Rename columns
result = rename(result.data, { name: 'employee', salary: 'pay' });
if (!result.ok) {
  throw new Error('Rename failed');
}

// Step 3: Drop a column
result = drop(result.data, { columns: ['age'] });
if (!result.ok) {
  throw new Error('Drop failed');
}

console.log('After fill -> rename -> drop pipeline:\n');
console.log(formatDataFrame(result.data));
console.log(`Final columns: ${getColumnNames(result.data).join(', ')}`);

console.log('\n✅ All operations completed successfully!');
console.log('All data was processed directly on Uint8Array buffers for maximum performance.');
