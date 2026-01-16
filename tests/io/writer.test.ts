import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { DataFrame } from '../../src/core/dataframe';
import { readCsv, toCsv, writeCsv } from '../../src/io/csv';
import { toJson, toJsonRecords } from '../../src/io/json';

describe('CSV Writer', () => {
  const testPath = './tests/fixtures/write-test.csv';

  afterAll(async () => {
    const file = Bun.file(testPath);
    if (await file.exists()) {
      await Bun.$`rm ${testPath}`;
    }
  });

  test('toCsv produces valid CSV string', () => {
    const df = DataFrame.fromColumns({
      name: ['Alice', 'Bob'],
      age: [25, 30],
    });

    const csv = toCsv(df);

    expect(csv).toContain('name,age');
    expect(csv).toContain('Alice,25');
    expect(csv).toContain('Bob,30');
  });

  test('toCsv escapes commas in values', () => {
    const df = DataFrame.fromColumns({
      text: ['hello, world', 'simple'],
    });

    const csv = toCsv(df);

    expect(csv).toContain('"hello, world"');
  });

  test('toCsv escapes quotes in values', () => {
    const df = DataFrame.fromColumns({
      text: ['say "hello"'],
    });

    const csv = toCsv(df);

    expect(csv).toContain('"say ""hello"""');
  });

  test('writeCsv creates file that can be read back', async () => {
    const df = DataFrame.fromColumns({
      x: [1, 2, 3],
      y: ['a', 'b', 'c'],
    });

    await writeCsv(df, testPath);
    const { df: loaded } = await readCsv(testPath);

    expect(loaded.shape).toEqual(df.shape);
    expect([...loaded.col('x')]).toEqual([1, 2, 3]);
    expect([...loaded.col('y')]).toEqual(['a', 'b', 'c']);
  });
});

describe('JSON Writer', () => {
  test('toJson returns valid JSON string', () => {
    const df = DataFrame.fromColumns({
      a: [1, 2],
      b: ['x', 'y'],
    });

    const json = toJson(df);
    const parsed = JSON.parse(json);

    expect(parsed).toEqual([
      { a: 1, b: 'x' },
      { a: 2, b: 'y' },
    ]);
  });

  test('toJsonRecords returns array of objects', () => {
    const df = DataFrame.fromColumns({
      id: [1, 2, 3],
      active: [true, false, true],
    });

    const records = toJsonRecords(df);

    expect(records).toHaveLength(3);
    expect(records[0]).toEqual({ id: 1, active: true });
    expect(records[2]).toEqual({ id: 3, active: true });
  });

  test('handles empty DataFrame', () => {
    const df = DataFrame.fromColumns({ x: [] as number[] });
    const json = toJson(df);

    expect(json).toBe('[]');
  });
});
