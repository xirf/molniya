import { DType } from '../src';
import { scanCsvFromString } from '../src/io/csv-scanner';

console.log('=== Simple CSV Example ===\n');

// Define schema
const schema = {
  id: DType.Int32,
  name: DType.String,
  price: DType.Float64,
};

// Create sample CSV data
const sampleCsv = `id,name,price
1,Apple,1.50
2,Banana,0.75
3,Orange,2.00
4,Mango,3.25
5,Grape,4.50`;

// Demonstrate both APIs
console.log('📝 Using scanCsvFromString (for in-memory data):');
const result1 = await scanCsvFromString(sampleCsv, { schema });

if (result1.ok) {
  const df = result1.data; // Result type uses .data, not .value
  const rowCount = df.columns.get('id')?.data.byteLength / 4 || 0; // Int32 = 4 bytes
  console.log(`✅ Loaded ${rowCount} rows`);
  console.log(`📊 Columns: ${df.columnOrder.join(', ')}\n`);
} else {
  console.error(`❌ Error: ${result1.error.message}`);
}

// If you have a file, you can use scanCsv directly:
console.log('📄 Using scanCsv (for file paths):');
console.log('   const result = await scanCsv("data.csv", { schema });');
console.log('   ↳ Molniya handles file reading automatically!\n');

console.log('=== Before vs After ===');
console.log('❌ Before: import fs from "fs";');
console.log('❌ Before: const data = fs.readFileSync("file.csv", "utf-8");');
console.log('❌ Before: const result = await scanCsv(data, { schema });');
console.log('');
console.log('✅ After: const result = await scanCsv("file.csv", { schema });');
console.log('');
console.log('=== Benefits ===');
console.log('✅ No need to import fs or readFileSync');
console.log("✅ Automatic file reading with Bun's optimized I/O");
console.log('✅ Consistent API with other data libraries (Pandas, Polars)');
console.log('✅ Error handling for file operations included');
console.log('✅ Both file and string inputs supported');
