import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { m } from '../../src/core/types';
import { readCsv } from '../../src/io/csv';

describe('CSV Reader', () => {
  const testCsvPath = './tests/fixtures/test.csv';
  const testCsvContent = `name,age,score,active
Alice,25,95.5,true
Bob,30,87.2,false
Carol,22,91.8,true`;

  beforeAll(async () => {
    // Create test fixture
    await Bun.write(testCsvPath, testCsvContent);
  });

  afterAll(async () => {
    // Cleanup
    const file = Bun.file(testCsvPath);
    if (await file.exists()) {
      await Bun.$`rm ${testCsvPath}`;
    }
  });

  describe('basic parsing', () => {
    test('reads CSV with auto-inferred types', async () => {
      const df = await readCsv(testCsvPath);

      expect(df.shape).toEqual([3, 4]);
      expect(df.columns()).toEqual(['name', 'age', 'score', 'active']);
    });

    test('infers string type for names', async () => {
      const df = await readCsv(testCsvPath);
      const names = df.col('name');

      expect(names.dtype.kind).toBe('string');
      expect([...names]).toEqual(['Alice', 'Bob', 'Carol']);
    });

    test('infers int32 type for integers', async () => {
      const df = await readCsv(testCsvPath);
      const ages = df.col('age');

      expect(ages.dtype.kind).toBe('int32');
      expect([...ages]).toEqual([25, 30, 22]);
    });

    test('infers float64 type for decimals', async () => {
      const df = await readCsv(testCsvPath);
      const scores = df.col('score');

      expect(scores.dtype.kind).toBe('float64');
      expect([...scores]).toEqual([95.5, 87.2, 91.8]);
    });

    test('infers bool type for true/false', async () => {
      const df = await readCsv(testCsvPath);
      const active = df.col('active');

      expect(active.dtype.kind).toBe('bool');
      expect([...active]).toEqual([true, false, true]);
    });
  });

  describe('explicit schema', () => {
    test('uses provided schema', async () => {
      const schema = {
        name: m.string(),
        age: m.float64(), // Force float instead of int
        score: m.float64(),
        active: m.bool(),
      } as const;

      const df = await readCsv(testCsvPath, { schema });
      const ages = df.col('age');

      expect(ages.dtype.kind).toBe('float64');
    });
  });

  describe('options', () => {
    test('respects maxRows', async () => {
      const df = await readCsv(testCsvPath, { maxRows: 2 });
      expect(df.shape[0]).toBe(2);
    });
  });
});

describe('CSV Parser Edge Cases', () => {
  const quotedCsvPath = './tests/fixtures/quoted.csv';
  const quotedContent = `name,description
"Alice","Hello, World"
"Bob","He said ""Hi"""
"Carol","Line1
Line2"`;

  beforeAll(async () => {
    await Bun.write(quotedCsvPath, quotedContent);
  });

  afterAll(async () => {
    const file = Bun.file(quotedCsvPath);
    if (await file.exists()) {
      await Bun.$`rm ${quotedCsvPath}`;
    }
  });

  test('handles quoted fields with commas', async () => {
    const df = await readCsv(quotedCsvPath);
    const descriptions = df.col('description');

    expect(descriptions.at(0)).toBe('Hello, World');
  });

  test('handles escaped quotes', async () => {
    const df = await readCsv(quotedCsvPath);
    const descriptions = df.col('description');

    expect(descriptions.at(1)).toBe('He said "Hi"');
  });

  // TODO: Supporting newlines in quoted fields requires streaming parser architecture
  // The current line-based reader splits on newlines before quote parsing
  test.skip('handles newlines in quoted fields', async () => {
    const df = await readCsv(quotedCsvPath);
    const descriptions = df.col('description');

    expect(descriptions.at(2)).toBe('Line1\nLine2');
  });
});
