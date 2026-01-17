/**
 * Benchmark setup - downloads test datasets.
 *
 * Uses Kaggle API for real-world datasets or generates synthetic data.
 *
 * For Kaggle downloads, set KAGGLE_USERNAME and KAGGLE_KEY environment variables.
 * Get your API token from: https://www.kaggle.com/settings/account
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { $ } from 'bun';

const DATA_DIR = path.join(process.cwd(), 'benchmarks', 'data');

/**
 * Download dataset from Kaggle using kaggle CLI.
 */
async function downloadFromKaggle(dataset: string, filename: string): Promise<string> {
  const filepath = path.join(DATA_DIR, filename);

  if (fs.existsSync(filepath)) {
    console.log(`✓ ${filename} already exists`);
    return filepath;
  }

  console.log(`📥 Downloading ${dataset} from Kaggle...`);

  // Check if kaggle CLI is available
  try {
    await $`kaggle --version`.quiet();
  } catch {
    console.log('  ⚠️  Kaggle CLI not found. Install with: pip install kaggle');
    return '';
  }

  // Download dataset
  try {
    await $`kaggle datasets download -d ${dataset} -p ${DATA_DIR} --unzip`.quiet();
    console.log(`✓ Downloaded ${filename}`);
    return filepath;
  } catch (error) {
    console.log(`  ⚠️  Kaggle download failed: ${error}`);
    console.log('  ⚠️  Make sure KAGGLE_USERNAME and KAGGLE_KEY are set');
    return '';
  }
}

/**
 * Generate a synthetic large CSV file for benchmarking.
 */
async function generateSyntheticDataset(rows: number, filepath: string): Promise<void> {
  console.log(`  Generating ${rows.toLocaleString()} rows...`);

  const headers =
    'InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n';
  const countries = [
    'United Kingdom',
    'France',
    'Germany',
    'EIRE',
    'Spain',
    'Netherlands',
    'Belgium',
  ];

  const lines: string[] = [headers];

  for (let i = 0; i < rows; i++) {
    const invoiceNo = 536365 + Math.floor(i / 5);
    const stockCode = `${10000 + (i % 1000)}`;
    const desc = 'WHITE HANGING HEART T-LIGHT HOLDER';
    const quantity = 1 + (i % 24);
    const date = '12/01/2010 08:26';
    const unitPrice = (0.5 + Math.random() * 20).toFixed(2);
    const customerID = 12346 + (i % 5000);
    const country = countries[i % countries.length];

    lines.push(
      `${invoiceNo},${stockCode},"${desc}",${quantity},${date},${unitPrice},${customerID},${country}\n`,
    );
  }

  const content = lines.join('');
  await Bun.write(filepath, content);

  const sizeMB = (content.length / 1024 / 1024).toFixed(2);
  console.log(`  Generated ${sizeMB}MB`);
}

export type DatasetName = 'retail-2010' | 'retail-2011' | 'synthetic-100k';

export async function ensureDataset(name: DatasetName): Promise<string> {
  // Create data directory
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  // Handle Kaggle real datasets
  if (name === 'retail-2010' || name === 'retail-2011') {
    const filename = name === 'retail-2010' ? 'Year 2009-2010.csv' : 'Year 2010-2011.csv';
    const filepath = path.join(DATA_DIR, filename);

    if (fs.existsSync(filepath)) {
      return filepath;
    }

    // Try to download if not exists
    const downloaded = await downloadFromKaggle(
      'mathchi/online-retail-ii-data-set-from-ml-repository',
      filename,
    );

    if (downloaded) return downloaded;

    // Fallback if download fails (shouldn't happen on local since we have files)
    console.log(`⚠️  Could not find real dataset ${filename}. Generating synthetic...`);
    const fallbackPath = path.join(DATA_DIR, `synthetic-${name}.csv`);
    await generateSyntheticDataset(500000, fallbackPath);
    return fallbackPath;
  }

  // Handle synthetic datasets
  if (name === 'synthetic-100k') {
    const rows = 100000;
    const filepath = path.join(DATA_DIR, 'synthetic-100k.csv');
    if (fs.existsSync(filepath)) return filepath;

    console.log(`📥 Generating ${rows.toLocaleString()} row synthetic dataset...`);
    await generateSyntheticDataset(rows, filepath);
    return filepath;
  }

  throw new Error(`Unknown dataset: ${name}`);
}

export async function ensureAllDatasets(): Promise<Map<string, string>> {
  console.log('\n📦 Setting up benchmark datasets...\n');
  const paths = new Map<string, string>();

  // Ensure we have at least one real or synthetic dataset
  paths.set('retail-2010', await ensureDataset('retail-2010'));
  paths.set('retail-2011', await ensureDataset('retail-2011'));

  console.log('\n✅ All datasets ready!\n');
  return paths;
}

// Run if called directly
if (import.meta.main) {
  await ensureAllDatasets();
}

export { DATA_DIR };
