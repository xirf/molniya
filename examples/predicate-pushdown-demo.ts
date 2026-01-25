/**
 * Predicate Pushdown Demo
 *
 * Demonstrates how predicate pushdown optimizes queries by filtering data
 * during CSV parsing rather than loading everything into memory first.
 *
 * Run: bun run molniya/examples/predicate-pushdown-demo.ts
 */

import { unlinkSync, writeFileSync } from 'node:fs';
import { readCsv } from '../src/io';
import { QueryPlan } from '../src/lazyframe/plan';
import type { ScanPlan } from '../src/lazyframe/plan';
import {
  estimatePushdownSavings,
  extractPushdownPredicates,
  scanCsvWithPredicates,
} from '../src/lazyframe/predicate-pushdown';

console.log('🚀 Predicate Pushdown Demo\n');

// Create sample sales dataset
const csvData = `date,product,category,price,quantity,revenue
2024-01-01,Laptop,Electronics,1200,1,1200
2024-01-01,Mouse,Electronics,25,5,125
2024-01-02,Laptop,Electronics,1200,2,2400
2024-01-02,Keyboard,Electronics,75,3,225
2024-01-03,Laptop,Electronics,1200,1,1200
2024-01-03,Monitor,Electronics,800,1,800
2024-01-04,Laptop,Electronics,1200,3,3600
2024-01-04,Cable,Electronics,15,10,150`;

const testFile = 'molniya/examples/sales-data.csv';
writeFileSync(testFile, csvData);

// Define schema for LazyFrame queries
const schema = {
  date: 'str',
  product: 'str',
  category: 'str',
  price: 'f64',
  quantity: 'i32',
  revenue: 'f64',
} as const;

console.log('📊 Dataset Overview:');
const allData = readCsv(testFile);
console.log(allData.toString());
console.log(`Total rows: ${allData.height}\n`);

// =============================================================================
// Example 1: Simple numeric filter
// =============================================================================
console.log(`=${'='.repeat(79)}`);
console.log('Example 1: Filter high-value sales (price >= 1000)\n');

console.log('❌ Traditional approach (load all → filter in memory):');
const filtered1 = readCsv(testFile).filter('price', '>=', 1000);
console.log(`   Loaded ${allData.height} rows → Filtered to ${filtered1.height} rows`);

console.log('\n✅ Predicate Pushdown (filter during CSV scan):');
const scanPlan1: ScanPlan = {
  type: 'scan',
  path: testFile,
  schema,
  columnOrder: Object.keys(schema),
  options: {},
};
const plan1 = QueryPlan.filter(scanPlan1, 'price', '>=', 1000);
const predicates1 = extractPushdownPredicates(plan1);
const optimized1 = scanCsvWithPredicates(scanPlan1, plan1, predicates1);

console.log(`   Loaded only ${optimized1.height} matching rows directly`);
console.log(`   Memory saved: ${((1 - optimized1.height / allData.height) * 100).toFixed(1)}%\n`);
console.log('Result:');
console.log(optimized1.toString());

// =============================================================================
// Example 2: String filter
// =============================================================================
console.log(`\n${'='.repeat(80)}`);
console.log('Example 2: Filter by product name\n');

console.log("✅ Filter product == 'Laptop':");
const scanPlan2: ScanPlan = {
  type: 'scan',
  path: testFile,
  schema,
  columnOrder: Object.keys(schema),
  options: {},
};
const plan2 = QueryPlan.filter(scanPlan2, 'product', '==', 'Laptop');
const predicates2 = extractPushdownPredicates(plan2);
const laptops = scanCsvWithPredicates(scanPlan2, plan2, predicates2);

console.log(laptops.toString());
console.log(
  `\nLoaded ${laptops.height} of ${allData.height} total rows (${((laptops.height / allData.height) * 100).toFixed(1)}%)\n`,
);

// =============================================================================
// Example 3: Multiple filters (AND logic)
// =============================================================================
console.log(`=${'='.repeat(80)}`);
console.log('Example 3: Multiple filters combined with AND\n');

console.log("✅ Filter category == 'Electronics' AND price >= 1000:");
const scanPlan3: ScanPlan = {
  type: 'scan',
  path: testFile,
  schema,
  columnOrder: Object.keys(schema),
  options: {},
};
let plan3 = QueryPlan.filter(scanPlan3, 'category', '==', 'Electronics');
plan3 = QueryPlan.filter(plan3, 'price', '>=', 1000);
const predicates3 = extractPushdownPredicates(plan3);
const complex = scanCsvWithPredicates(scanPlan3, plan3, predicates3);

console.log(complex.toString());
console.log(`\nLoaded ${complex.height} matching rows\n`);

// =============================================================================
// Example 4: Estimate savings before execution
// =============================================================================
console.log(`=${'='.repeat(80)}`);
console.log('Example 4: Estimate memory savings before executing\n');

const estimatePlan: ScanPlan = {
  type: 'scan',
  path: testFile,
  schema,
  columnOrder: Object.keys(schema),
  options: {},
};
const filterForEstimate = QueryPlan.filter(estimatePlan, 'price', '>=', 1000);
const estimate = estimatePushdownSavings(filterForEstimate, allData.height);

console.log('📊 Savings estimate:');
console.log(`   Total rows: ${allData.height}`);
console.log(`   Estimated matches: ${allData.height - estimate.rowsSaved}`);
console.log(
  `   Rows filtered out: ${estimate.rowsSaved} (${(estimate.percentSaved * 100).toFixed(1)}%)`,
);
console.log(`   Memory saved: ${estimate.memorySavedMB.toFixed(3)} MB\n`);

// =============================================================================
// Example 5: All supported comparison operators
// =============================================================================
console.log(`=${'='.repeat(80)}`);
console.log('Example 5: All supported comparison operators\n');

const operators: Array<{ op: '==' | '!=' | '>' | '<' | '>=' | '<='; value: number; desc: string }> =
  [
    { op: '==', value: 1200, desc: 'Equal to 1200' },
    { op: '!=', value: 1200, desc: 'Not equal to 1200' },
    { op: '>', value: 100, desc: 'Greater than 100' },
    { op: '<', value: 100, desc: 'Less than 100' },
    { op: '>=', value: 800, desc: 'Greater or equal to 800' },
    { op: '<=', value: 100, desc: 'Less or equal to 100' },
  ];

console.log("Filter variations on 'price' column:\n");
for (const { op, value, desc } of operators) {
  const sp: ScanPlan = {
    type: 'scan',
    path: testFile,
    schema,
    columnOrder: Object.keys(schema),
    options: {},
  };
  const p = QueryPlan.filter(sp, 'price', op, value);
  const preds = extractPushdownPredicates(p);
  const result = scanCsvWithPredicates(sp, p, preds);
  console.log(`   ${desc.padEnd(30)} → ${result.height} rows`);
}

// =============================================================================
// Key Takeaways
// =============================================================================
console.log(`\n${'='.repeat(80)}`);
console.log('🎯 Key Benefits:\n');
console.log('1. Memory Efficiency');
console.log('   - Only allocates memory for rows that pass the filter');
console.log('   - Ideal for selective queries (< 10% of total data)\n');

console.log('2. Performance');
console.log('   - Avoids loading and then discarding filtered-out data');
console.log('   - Faster than load-all → filter approach\n');

console.log('3. Automatic Optimization');
console.log('   - LazyFrame executor applies predicate pushdown automatically');
console.log('   - No code changes needed - just build query plans\n');

console.log('4. Operator Support');
console.log('   - Supports: ==, !=, >, <, >=, <=');
console.log('   - Works with both numeric and string columns\n');

console.log('5. Composability');
console.log('   - Multiple filters combined with AND logic');
console.log('   - Works alongside column pruning for maximum savings\n');

console.log(`=${'='.repeat(80)}\n`);

// Cleanup
try {
  unlinkSync(testFile);
} catch {}
