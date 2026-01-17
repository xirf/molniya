import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { existsSync, mkdirSync, unlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { scanCsv } from '../../src/io/csv';

const TEST_DIR = join(import.meta.dir, '..', 'fixtures');
const LAZY_CSV_PATH = join(TEST_DIR, 'lazy-test.csv');

// Create test CSV file
beforeAll(() => {
  if (!existsSync(TEST_DIR)) {
    mkdirSync(TEST_DIR, { recursive: true });
  }

  // Create a larger test file with 1000 rows
  const headers = 'id,name,price,quantity,active\n';
  const rows: string[] = [];
  for (let i = 1; i <= 1000; i++) {
    rows.push(`${i},Product${i},${(i * 1.5).toFixed(2)},${i * 10},${i % 2 === 0}`);
  }
  writeFileSync(LAZY_CSV_PATH, headers + rows.join('\n'));
});

afterAll(() => {
  try {
    unlinkSync(LAZY_CSV_PATH);
  } catch {
    // Ignore if file doesn't exist
  }
});

describe('LazyFrame', () => {
  describe('scanCsv', () => {
    test('creates LazyFrame with correct shape', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      expect(lazy.shape[0]).toBe(1000);
      expect(lazy.shape[1]).toBe(5);
    });

    test('columns() returns column names', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      expect(lazy.columns()).toEqual(['id', 'name', 'price', 'quantity', 'active']);
    });

    test('info() returns metadata', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      const info = lazy.info();
      expect(info.rows).toBe(1000);
      expect(info.columns).toBe(5);
      expect(info.dtypes).toHaveProperty('id');
      expect(info.dtypes).toHaveProperty('name');
    });
  });

  describe('head/tail', () => {
    test('head() returns first n rows as DataFrame', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      const first5 = await lazy.head(5);

      expect(first5.shape[0]).toBe(5);
      expect(first5.shape[1]).toBe(5);
      expect(first5.col('id').at(0)).toBe(1);
      expect(first5.col('id').at(4)).toBe(5);
    });

    test('tail() returns last n rows as DataFrame', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      const last5 = await lazy.tail(5);

      expect(last5.shape[0]).toBe(5);
      expect(last5.col('id').at(0)).toBe(996);
      expect(last5.col('id').at(4)).toBe(1000);
    });

    test('head() defaults to 5 rows', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      const first = await lazy.head();
      expect(first.shape[0]).toBe(5);
    });
  });

  describe('select', () => {
    test('select() returns LazyFrame with fewer columns', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      const selected = lazy.select('id', 'price');

      expect(selected.columns()).toEqual(['id', 'price']);

      const df = await selected.head(3);
      expect(df.shape[1]).toBe(2);
      expect(df.col('id').at(0)).toBe(1);
    });
  });

  describe('filter', () => {
    test('filter() returns matching rows as DataFrame', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      const filtered = await lazy.filter((row) => (row as { id: number }).id <= 10);

      expect(filtered.shape[0]).toBe(10);
      expect(filtered.col('id').at(0)).toBe(1);
      expect(filtered.col('id').at(9)).toBe(10);
    });

    test('filter() with complex condition', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      const filtered = await lazy.filter((row) => {
        const r = row as { price: number; quantity: number };
        return r.price > 100 && r.quantity > 500;
      });

      // price > 100 means id > 66.67, quantity > 500 means id > 50
      // Both conditions: id > 67
      expect(filtered.shape[0]).toBeGreaterThan(0);
    });
  });

  describe('collect', () => {
    test('collect() returns full DataFrame', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      const df = await lazy.collect();

      expect(df.shape[0]).toBe(1000);
      expect(df.shape[1]).toBe(5);
    });

    test('collect(limit) returns limited DataFrame', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      const df = await lazy.collect(100);

      expect(df.shape[0]).toBe(100);
    });
  });

  describe('caching', () => {
    test('clearCache() resets cache', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);

      // Load some data to populate cache
      await lazy.head(100);
      expect(lazy.info().cached).toBeGreaterThan(0);

      lazy.clearCache();
      expect(lazy.info().cached).toBe(0);
    });

    test('repeated access uses cache', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);

      // First access
      const first = await lazy.head(10);
      const cachedBefore = lazy.info().cached;

      // Second access should use cache
      const second = await lazy.head(10);
      const cachedAfter = lazy.info().cached;

      expect(cachedAfter).toBe(cachedBefore);
      expect(first.shape).toEqual(second.shape);
    });
  });

  describe('type inference', () => {
    test('correctly infers column types', async () => {
      const lazy = await scanCsv(LAZY_CSV_PATH);
      const info = lazy.info();

      // All numeric columns default to float64
      expect(info.dtypes.id).toBe('float64');
      expect(info.dtypes.name).toBe('string');
      expect(info.dtypes.price).toBe('float64');
      expect(info.dtypes.quantity).toBe('float64');
      expect(info.dtypes.active).toBe('bool');
    });
  });
});
