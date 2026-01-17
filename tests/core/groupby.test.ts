import { describe, expect, test } from 'bun:test';
import { DataFrame } from '../../src/core/dataframe';
import { m } from '../../src/core/types';

describe('GroupBy Edge Cases', () => {
  const schema = {
    A: m.int32(),
    B: m.int32(),
    C: m.int32(),
  } as const;

  const data = [
    { A: 1, B: 2, C: 3 },
    { A: 4, B: 5, C: 6 },
    { A: 1, B: 5, C: 6 }, // Repeated A=1
    { A: 4, B: 2, C: 8 }, // Repeated A=4
  ];

  describe('Basic Grouping', () => {
    test('groupby single column counts', () => {
      const df = DataFrame.from(schema, data);
      const groupDf = df.groupby('A');
      const counts = groupDf.count();

      // Groups: A=1 (2 rows), A=4 (2 rows)
      expect(counts.length).toBe(2);

      const g1 = counts.find((r) => r.A === 1);
      expect(g1?.count).toBe(2);

      const g4 = counts.find((r) => r.A === 4);
      expect(g4?.count).toBe(2);
    });

    test('groupby two columns', () => {
      const df = DataFrame.from(schema, data);
      const groupDf = df.groupby('A', 'B');
      const counts = groupDf.count();

      // Groups: 1-2, 4-5, 1-5, 4-2 -> All unique combinations in data
      // 1-2, 4-5, 1-5, 4-2
      // Wait, data:
      // { A: 1, B: 2 }
      // { A: 4, B: 5 }
      // { A: 1, B: 5 }
      // { A: 4, B: 2 }
      // All are unique (count 1 each)
      expect(counts.length).toBe(4);
      expect(counts[0]?.count).toBe(1);
    });
  });

  describe('Aggregations', () => {
    test('sum column element in group', () => {
      const df = DataFrame.from(schema, data);
      const groupDf = df.groupby('A');
      // A=1 -> C is [3, 6] -> sum 9
      // A=4 -> C is [6, 8] -> sum 14
      const sums = groupDf.sum('C');

      const g1 = sums.find((r) => r.A === 1);
      expect(g1?.C).toBe(9);

      const g4 = sums.find((r) => r.A === 4);
      expect(g4?.C).toBe(14);
    });

    test('mean column element in group', () => {
      const df = DataFrame.from(schema, data);
      const groupDf = df.groupby('A');
      // A=1 -> B is [2, 5] -> mean 3.5
      const means = groupDf.mean('B');

      const g1 = means.find((r) => r.A === 1);
      expect(g1?.B).toBe(3.5);
    });

    test('min and max in group', () => {
      const df = DataFrame.from(schema, data);
      const results = df.groupby('A').agg({ C: 'min', B: 'max' });

      // A=1: C=[3,6] (min 3), B=[2,5] (max 5)
      const g1 = results.find((r) => r.A === 1);
      expect(g1?.C).toBe(3);
      expect(g1?.B).toBe(5);
    });

    test('first and last in group', () => {
      const df = DataFrame.from(schema, data);
      const results = df.groupby('A').agg({ C: 'first', B: 'last' });

      // A=1 rows: [1,2,3], [1,5,6] -> first C=3, last B=5
      const g1 = results.find((r) => r.A === 1);
      expect(g1?.C).toBe(3);
      expect(g1?.B).toBe(5);
    });
  });

  describe('Edge Cases', () => {
    test('groupby on column with all same values', () => {
      const sameData = [
        { A: 1, B: 10 },
        { A: 1, B: 20 },
      ];
      const df = DataFrame.from({ A: m.int32(), B: m.int32() }, sameData);
      const groups = df.groupby('A').count();
      expect(groups.length).toBe(1);
      expect(groups[0]?.count).toBe(2);
    });

    test('aggregation on empty dataframe', () => {
      const df = DataFrame.empty(schema);
      const groups = df.groupby('A').count();
      expect(groups.length).toBe(0);
    });

    // Note: Aggregating non-numeric columns for sum/mean usually results in NaN or concatenation depending on implementation.
    // Molniya implementation for 'sum' creates 0 for reduce seed and adds.
    // invalid kinds might need check, but currently it tries to cast/filter to numbers.
  });
});
