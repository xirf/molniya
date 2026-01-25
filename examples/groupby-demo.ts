import { setColumnValue } from '../src/core/column';
import { addColumn, createDataFrame, getColumn } from '../src/dataframe/dataframe';
import { type AggSpec, groupby } from '../src/dataframe/groupby';
import { formatDataFrame } from '../src/dataframe/print';
import { internString } from '../src/memory/dictionary';
import { DType } from '../src/types/dtypes';

console.log('='.repeat(60));
console.log('GroupBy Operation Demo');
console.log('='.repeat(60));

// Create sample trading data
const df = createDataFrame();
addColumn(df, 'symbol', DType.String, 10);
addColumn(df, 'side', DType.String, 10);
addColumn(df, 'price', DType.Float64, 10);
addColumn(df, 'volume', DType.Float64, 10);
addColumn(df, 'timestamp', DType.DateTime, 10);

const symbolCol = df.columns.get('symbol')!;
const sideCol = df.columns.get('side')!;
const priceCol = df.columns.get('price')!;
const volumeCol = df.columns.get('volume')!;
const timestampCol = df.columns.get('timestamp')!;

// Sample trading data: BTC and ETH with buy/sell sides
const data = [
  { symbol: 'BTC', side: 'buy', price: 50000, volume: 1.5, timestamp: 1000n },
  { symbol: 'BTC', side: 'buy', price: 50100, volume: 2.0, timestamp: 2000n },
  { symbol: 'BTC', side: 'sell', price: 50200, volume: 1.0, timestamp: 3000n },
  { symbol: 'ETH', side: 'buy', price: 3000, volume: 10.0, timestamp: 4000n },
  { symbol: 'ETH', side: 'buy', price: 3010, volume: 8.5, timestamp: 5000n },
  { symbol: 'ETH', side: 'sell', price: 3020, volume: 5.0, timestamp: 6000n },
  { symbol: 'BTC', side: 'buy', price: 50300, volume: 0.5, timestamp: 7000n },
  { symbol: 'ETH', side: 'sell', price: 3005, volume: 7.0, timestamp: 8000n },
  { symbol: 'BTC', side: 'sell', price: 50150, volume: 3.0, timestamp: 9000n },
  { symbol: 'ETH', side: 'buy', price: 3015, volume: 12.0, timestamp: 10000n },
];

for (let i = 0; i < data.length; i++) {
  const row = data[i];
  setColumnValue(symbolCol, i, internString(df.dictionary, row.symbol));
  setColumnValue(sideCol, i, internString(df.dictionary, row.side));
  setColumnValue(priceCol, i, row.price);
  setColumnValue(volumeCol, i, row.volume);
  setColumnValue(timestampCol, i, row.timestamp);
}

console.log('\n📊 Original Trading Data:');
console.log(formatDataFrame(df, { maxRows: 10 }));

// Example 1: Group by symbol
console.log(`\n${'='.repeat(60)}`);
console.log('Example 1: Group by Symbol');
console.log('='.repeat(60));

const symbolAggs: AggSpec[] = [
  { col: 'volume', func: 'sum', outName: 'total_volume' },
  { col: 'price', func: 'mean', outName: 'avg_price' },
  { col: 'price', func: 'min', outName: 'min_price' },
  { col: 'price', func: 'max', outName: 'max_price' },
  { col: 'symbol', func: 'count', outName: 'trade_count' },
];

const resultBySymbol = groupby(df, ['symbol'], symbolAggs);
if (resultBySymbol.ok) {
  console.log('\nAggregations by symbol (total volume, avg/min/max price, count):');
  console.log(formatDataFrame(resultBySymbol.data, { maxRows: 10 }));
} else {
  console.error('Error:', resultBySymbol.error.message);
}

// Example 2: Group by side
console.log(`\n${'='.repeat(60)}`);
console.log('Example 2: Group by Side (Buy vs Sell)');
console.log('='.repeat(60));

const sideAggs: AggSpec[] = [
  { col: 'volume', func: 'sum', outName: 'total_volume' },
  { col: 'price', func: 'mean', outName: 'avg_price' },
  { col: 'side', func: 'count', outName: 'trade_count' },
];

const resultBySide = groupby(df, ['side'], sideAggs);
if (resultBySide.ok) {
  console.log('\nAggregations by side:');
  console.log(formatDataFrame(resultBySide.data, { maxRows: 10 }));
} else {
  console.error('Error:', resultBySide.error.message);
}

// Example 3: Multi-column grouping (symbol + side)
console.log(`\n${'='.repeat(60)}`);
console.log('Example 3: Group by Symbol AND Side');
console.log('='.repeat(60));

const multiAggs: AggSpec[] = [
  { col: 'volume', func: 'sum', outName: 'total_volume' },
  { col: 'price', func: 'mean', outName: 'avg_price' },
  { col: 'timestamp', func: 'min', outName: 'first_trade_time' },
  { col: 'timestamp', func: 'max', outName: 'last_trade_time' },
];

const resultBySymbolSide = groupby(df, ['symbol', 'side'], multiAggs);
if (resultBySymbolSide.ok) {
  console.log('\nAggregations by symbol and side:');
  console.log(formatDataFrame(resultBySymbolSide.data, { maxRows: 10 }));
} else {
  console.error('Error:', resultBySymbolSide.error.message);
}

// Example 4: All aggregation functions
console.log(`\n${'='.repeat(60)}`);
console.log('Example 4: All Aggregation Functions');
console.log('='.repeat(60));

const allAggs: AggSpec[] = [
  { col: 'price', func: 'count', outName: 'count' },
  { col: 'price', func: 'sum', outName: 'sum' },
  { col: 'price', func: 'mean', outName: 'mean' },
  { col: 'price', func: 'min', outName: 'min' },
  { col: 'price', func: 'max', outName: 'max' },
  { col: 'price', func: 'first', outName: 'first' },
  { col: 'price', func: 'last', outName: 'last' },
];

const resultAllAggs = groupby(df, ['symbol'], allAggs);
if (resultAllAggs.ok) {
  console.log('\nAll aggregation functions on price grouped by symbol:');
  console.log(formatDataFrame(resultAllAggs.data, { maxRows: 10 }));
} else {
  console.error('Error:', resultAllAggs.error.message);
}

console.log(`\n${'='.repeat(60)}`);
console.log('✅ GroupBy Demo Complete!');
console.log('='.repeat(60));
console.log('\nKey Features:');
console.log('  ✓ Pre-sort strategy (sort → find boundaries → aggregate)');
console.log('  ✓ Zero object creation in hot loops');
console.log('  ✓ Single-column and multi-column grouping');
console.log('  ✓ 7 aggregation functions: count, sum, mean, min, max, first, last');
console.log('  ✓ Smart dtype handling (count→Int32, mean→Float64, others preserve)');
console.log('  ✓ Works with all dtypes (Float64, Int32, DateTime, String, Bool)');
