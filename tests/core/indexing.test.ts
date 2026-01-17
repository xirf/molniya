import { describe, expect, test } from 'bun:test';
import { DataFrame } from '../../src/core/dataframe';
import { m } from '../../src/core/types';

describe('Indexing Edge Cases', () => {
  const schema = {
    A: m.int32(),
    B: m.string(),
  } as const;

  const data = [
    { A: 1, B: 'a' },
    { A: 2, B: 'b' },
    { A: 3, B: 'c' },
    { A: 4, B: 'd' },
    { A: 5, B: 'e' },
  ];

  const df = DataFrame.from(schema, data);

  describe('iloc numeric slicing', () => {
    test('iloc(index) returns single row', () => {
      const row = df.iloc(1);
      expect(row).toEqual({ A: 2, B: 'b' });
    });

    test('iloc(start, end) returns DataFrame subset', () => {
      const subset = df.iloc(1, 3);
      expect(subset.shape).toEqual([2, 2]);
      expect([...subset.col('A')]).toEqual([2, 3]);
    });

    test('iloc(start, end) with end > length', () => {
      const subset = df.iloc(3, 10);
      expect(subset.shape).toEqual([2, 2]);
      expect([...subset.col('A')]).toEqual([4, 5]);
    });
  });

  describe('iloc string slicing', () => {
    test('iloc("start:end") slice', () => {
      // @ts-ignore
      const subset = df.iloc('1:3');
      expect(subset.shape).toEqual([2, 2]);
      expect([...subset.col('A')]).toEqual([2, 3]);
    });

    test('iloc("start:") slice to end', () => {
      // @ts-ignore
      const subset = df.iloc('3:');
      expect(subset.shape).toEqual([2, 2]);
      expect([...subset.col('A')]).toEqual([4, 5]);
    });

    test('iloc(":end") slice from start', () => {
      // @ts-ignore
      const subset = df.iloc(':2');
      expect(subset.shape).toEqual([2, 2]);
      expect([...subset.col('A')]).toEqual([1, 2]);
    });

    test('iloc(":") slice all', () => {
      // @ts-ignore
      const subset = df.iloc(':');
      expect(subset.shape).toEqual([5, 2]);
      expect([...subset.col('A')]).toEqual([1, 2, 3, 4, 5]);
    });
  });

  describe('loc indexing', () => {
    test('loc(indices) reorders rows', () => {
      const subset = df.loc([2, 0, 4]);
      expect(subset.shape).toEqual([3, 2]);
      expect([...subset.col('A')]).toEqual([3, 1, 5]);
    });

    test('loc(indices) with duplicates', () => {
      const subset = df.loc([0, 0]);
      expect(subset.shape).toEqual([2, 2]);
      expect([...subset.col('A')]).toEqual([1, 1]);
    });
  });
});
