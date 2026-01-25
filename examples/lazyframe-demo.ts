import { formatDataFrame } from '../src/dataframe/print';
import { LazyFrame } from '../src/lazyframe/lazyframe';
import { DType } from '../src/types/dtypes';

console.log('='.repeat(60));
console.log('LazyFrame Demo - Query Planning and Execution');
console.log('='.repeat(60));

// Example 1: Build a query plan without executing
console.log('\n📋 Example 1: Building a Query Plan');
console.log('─'.repeat(60));

const schema = {
  timestamp: DType.DateTime,
  symbol: DType.String,
  side: DType.String,
  price: DType.Float64,
  volume: DType.Int32,
};

const lazyQuery = LazyFrame.scanCsv('artifac/btcusd_1-min_data.csv', schema)
  .filter('symbol', '==', 'BTC')
  .filter('side', '==', 'buy')
  .filter('price', '>', 50000)
  .select(['symbol', 'side', 'price', 'volume'])
  .groupby(
    ['symbol', 'side'],
    [
      { col: 'volume', func: 'sum', outName: 'total_volume' },
      { col: 'price', func: 'mean', outName: 'avg_price' },
      { col: 'price', func: 'min', outName: 'min_price' },
      { col: 'price', func: 'max', outName: 'max_price' },
    ],
  );

console.log('\n🔍 Query Plan:');
console.log(lazyQuery.explain());

console.log('\n📊 Expected Output Schema:');
const outputSchema = lazyQuery.getSchema();
for (const [col, dtype] of Object.entries(outputSchema)) {
  console.log(`  ${col}: ${DType[dtype]}`);
}

console.log('\n📋 Expected Column Order:');
console.log(`  ${lazyQuery.getColumnOrder().join(', ')}`);

// Example 2: Simple filter and select
console.log('\n\n📋 Example 2: Simple Filter and Select');
console.log('─'.repeat(60));

const simpleSchema = {
  Timestamp: DType.Int32,
  Open: DType.Float64,
  High: DType.Float64,
  Low: DType.Float64,
  Close: DType.Float64,
  'Volume_(BTC)': DType.Float64,
  'Volume_(Currency)': DType.Float64,
  Weighted_Price: DType.Float64,
};

const simpleLazy = LazyFrame.scanCsv('artifac/btcusd_1-min_data.csv', simpleSchema)
  .filter('Close', '>', 10)
  .select(['Timestamp', 'Open', 'Close', 'Volume_(BTC)']);

console.log('\n🔍 Query Plan:');
console.log(simpleLazy.explain());

console.log('\n⚡ Executing query...');
const simpleResult = await simpleLazy.collect();

if (simpleResult.ok) {
  console.log('✅ Query executed successfully!');
  console.log('\n📊 Result (first 10 rows):');
  console.log(formatDataFrame(simpleResult.data, { maxRows: 10 }));
} else {
  console.error('❌ Error:', simpleResult.error.message);
}

// Example 3: GroupBy aggregation
console.log('\n\n📋 Example 3: GroupBy Aggregation');
console.log('─'.repeat(60));

// Create a simple test CSV for groupby demo
import { mkdirSync, writeFileSync } from 'node:fs';

mkdirSync('temp', { recursive: true });
writeFileSync(
  'temp/trades.csv',
  `symbol,side,price,volume
BTC,buy,50000,10
BTC,buy,50100,20
BTC,sell,50200,15
BTC,sell,50150,25
ETH,buy,3000,100
ETH,buy,3010,150
ETH,sell,3020,120
ETH,sell,3005,80`,
);

const tradesSchema = {
  symbol: DType.String,
  side: DType.String,
  price: DType.Float64,
  volume: DType.Int32,
};

const groupbyLazy = LazyFrame.scanCsv('temp/trades.csv', tradesSchema).groupby(
  ['symbol', 'side'],
  [
    { col: 'volume', func: 'sum', outName: 'total_volume' },
    { col: 'price', func: 'mean', outName: 'avg_price' },
    { col: 'price', func: 'min', outName: 'min_price' },
    { col: 'price', func: 'max', outName: 'max_price' },
    { col: 'symbol', func: 'count', outName: 'trade_count' },
  ],
);

console.log('\n🔍 Query Plan:');
console.log(groupbyLazy.explain());

console.log('\n⚡ Executing query...');
const groupbyResult = await groupbyLazy.collect();

if (groupbyResult.ok) {
  console.log('✅ Query executed successfully!');
  console.log('\n📊 Aggregated Results:');
  console.log(formatDataFrame(groupbyResult.data, { maxRows: 10 }));
} else {
  console.error('❌ Error:', groupbyResult.error.message);
}

// Example 4: Complex chained operations
console.log('\n\n📋 Example 4: Complex Chained Operations');
console.log('─'.repeat(60));

const complexLazy = LazyFrame.scanCsv('temp/trades.csv', tradesSchema)
  .filter('side', '==', 'buy')
  .filter('volume', '>', 50)
  .select(['symbol', 'price', 'volume'])
  .groupby(
    ['symbol'],
    [
      { col: 'volume', func: 'sum', outName: 'total_buy_volume' },
      { col: 'price', func: 'mean', outName: 'avg_buy_price' },
    ],
  );

console.log('\n🔍 Query Plan:');
console.log(complexLazy.explain());

console.log('\n⚡ Executing query...');
const complexResult = await complexLazy.collect();

if (complexResult.ok) {
  console.log('✅ Query executed successfully!');
  console.log('\n📊 Buy-side Aggregation:');
  console.log(formatDataFrame(complexResult.data, { maxRows: 10 }));
} else {
  console.error('❌ Error:', complexResult.error.message);
}

// Cleanup
import { rmSync } from 'node:fs';
rmSync('temp', { recursive: true, force: true });

console.log(`\n${'='.repeat(60)}`);
console.log('✅ LazyFrame Demo Complete!');
console.log('='.repeat(60));
console.log('\n🎯 Key Features Demonstrated:');
console.log('  ✓ Query plan construction without execution');
console.log('  ✓ Chain multiple operations (filter, select, groupby)');
console.log('  ✓ Inspect plans with explain()');
console.log('  ✓ Get output schema and column order before execution');
console.log('  ✓ Execute plans with collect()');
console.log('  ✓ Error handling with Result types');
console.log('\n💡 Benefits of Lazy Evaluation:');
console.log('  ✓ Deferred execution - no work until collect()');
console.log('  ✓ Query optimization opportunities');
console.log('  ✓ Composable queries - build incrementally');
console.log('  ✓ Reusable plans - same plan, different data');
