/**
 * Example demonstrating column pruning optimization
 * Shows how selecting few columns from wide tables saves memory
 */

import { unlinkSync, writeFileSync } from 'node:fs';
import { analyzeRequiredColumns } from '../src/lazyframe/column-analyzer';
import { scanCsvWithPruning } from '../src/lazyframe/csv-pruning';
import { QueryPlan } from '../src/lazyframe/plan';

console.log('=== Column Pruning Demo ===\n');

// Create a wide CSV file with employee data
const csvContent = `
id,first_name,last_name,email,phone,department,salary,hire_date,address,city,state,zip,country,manager_id,bonus,commission,notes,emergency_contact,emergency_phone,status
1,John,Doe,john.doe@example.com,555-0001,Engineering,120000,2020-01-15,123 Main St,Seattle,WA,98101,USA,5,10000,0,Top performer,Jane Doe,555-9001,active
2,Jane,Smith,jane.smith@example.com,555-0002,Sales,95000,2019-06-01,456 Oak Ave,Portland,OR,97201,USA,6,8000,5000,Excellent closer,John Smith,555-9002,active
3,Bob,Johnson,bob.j@example.com,555-0003,Marketing,85000,2021-03-10,789 Pine Rd,San Francisco,CA,94102,USA,7,5000,0,,Alice Johnson,555-9003,active
4,Alice,Williams,alice.w@example.com,555-0004,Engineering,115000,2018-11-20,321 Elm St,Austin,TX,78701,USA,5,12000,0,Senior engineer,Bob Williams,555-9004,active
5,Charlie,Brown,charlie.b@example.com,555-0005,Engineering,135000,2017-05-15,654 Maple Dr,Seattle,WA,98102,USA,null,15000,0,Team lead,David Brown,555-9005,active
`.trim();

const testFile = './employees.csv';
writeFileSync(testFile, csvContent);

try {
  console.log('Example 1: Selecting just name and salary from wide employee table\n');
  console.log('Query: SELECT first_name, last_name, salary FROM employees\n');

  // Method 1: Using query plan analysis
  const plan = QueryPlan.select(QueryPlan.scan(testFile, {}, []), [
    'first_name',
    'last_name',
    'salary',
  ]);

  const requiredColumns = analyzeRequiredColumns(plan);
  console.log('Columns analyzed from query plan:');
  console.log(`  Required: ${Array.from(requiredColumns).join(', ')}`);
  console.log(
    `  Skipped: ${20 - requiredColumns.size} columns (${(((20 - requiredColumns.size) / 20) * 100).toFixed(0)}%)\n`,
  );

  // Method 2: Direct pruned scan
  const result = scanCsvWithPruning(testFile, {
    requiredColumns: new Set(['first_name', 'last_name', 'salary']),
    schema: new Map([
      ['first_name', 'string'],
      ['last_name', 'string'],
      ['salary', 'int32'],
    ]),
  });

  if (result.ok) {
    console.log('Result DataFrame:');
    console.log(`  Columns: ${Array.from(result.data.columns.keys()).join(', ')}`);
    console.log(`  Rows: ${result.data.columns.get('first_name')?.length ?? 0}`);

    // Show first few rows
    console.log('\nData preview:');
    const firstNames = result.data.columns.get('first_name');
    const lastNames = result.data.columns.get('last_name');
    const salaries = result.data.columns.get('salary');

    if (firstNames && lastNames && salaries) {
      for (let i = 0; i < Math.min(3, firstNames.length); i++) {
        const firstName =
          result.data.dictionary?.idToString.get(firstNames.view.getInt32(i * 4, true)) ?? '?';
        const lastName =
          result.data.dictionary?.idToString.get(lastNames.view.getInt32(i * 4, true)) ?? '?';
        const salary = salaries.view.getInt32(i * 4, true);
        console.log(`  ${firstName} ${lastName}: $${salary.toLocaleString()}`);
      }
    }
  } else {
    console.error('Error:', result.error);
  }

  console.log('\n---\n');
  console.log('Example 2: Selecting single column for analysis\n');
  console.log('Query: SELECT department FROM employees\n');

  const deptResult = scanCsvWithPruning(testFile, {
    requiredColumns: new Set(['department']),
    schema: new Map([['department', 'string']]),
  });

  if (deptResult.ok) {
    console.log('Memory savings:');
    console.log('  Total columns: 20');
    console.log('  Loaded columns: 1');
    console.log('  Memory saved: 95%');
    console.log('  Columns skipped: id, first_name, last_name, email, phone, salary, ...');

    const depts = deptResult.data.columns.get('department');
    if (depts) {
      console.log('\nDepartments:');
      const uniqueDepts = new Set<string>();
      for (let i = 0; i < depts.length; i++) {
        const deptId = depts.view.getInt32(i * 4, true);
        const dept = deptResult.data.dictionary?.idToString.get(deptId) ?? '?';
        uniqueDepts.add(dept);
      }
      console.log(`  ${Array.from(uniqueDepts).join(', ')}`);
    }
  }

  console.log('\n---\n');
  console.log('Example 3: Complex query with filters\n');
  console.log('Query: SELECT first_name, salary FROM employees WHERE department = "Engineering"\n');

  const engPlan = QueryPlan.filter(
    QueryPlan.select(QueryPlan.scan(testFile, {}, []), ['first_name', 'salary', 'department']),
    'department',
    '==',
    'Engineering',
  );

  const engColumns = analyzeRequiredColumns(engPlan);
  console.log('Query plan analysis:');
  console.log(`  Columns needed: ${Array.from(engColumns).join(', ')}`);
  console.log(`  Note: Even though filter is on 'department', select limits us to 3 columns`);
  console.log(`  Memory saved: ${(((20 - 3) / 20) * 100).toFixed(0)}%\n`);

  console.log('✓ Column pruning automatically reduces memory usage');
  console.log('✓ Especially effective for wide tables with narrow selects');
  console.log('✓ Transparent optimization - no API changes needed\n');
} finally {
  unlinkSync(testFile);
}
