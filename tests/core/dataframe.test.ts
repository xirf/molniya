import { describe, expect, test } from 'bun:test';
import { DataFrame } from '../../src/core/dataframe';
import { m } from '../../src/core/types';

describe('DataFrame', () => {
  const schema = {
    age: m.int32(),
    name: m.string(),
    score: m.float64(),
  } as const;

  const sampleData = [
    { age: 25, name: 'Alice', score: 95.5 },
    { age: 30, name: 'Bob', score: 87.2 },
    { age: 22, name: 'Carol', score: 91.8 },
  ];

  describe('creation', () => {
    test('from() creates DataFrame', () => {
      const df = DataFrame.from(schema, sampleData);
      expect(df.shape).toEqual([3, 3]);
    });

    test('empty() creates empty DataFrame', () => {
      const df = DataFrame.empty(schema);
      expect(df.shape).toEqual([0, 3]);
    });

    test('fromColumns() creates DataFrame with manual initialization', () => {
      const df = DataFrame.fromColumns({
        age: [25, 30, 22],
        name: ['Alice', 'Bob', 'Carol'],
        score: [95.5, 87.2, 91.8],
      });

      expect(df.shape).toEqual([3, 3]);
      expect([...df.col('age')]).toEqual([25, 30, 22]);
      expect([...df.col('name')]).toEqual(['Alice', 'Bob', 'Carol']);
      expect([...df.col('score')]).toEqual([95.5, 87.2, 91.8]);
    });

    test('fromColumns() infers int32 for integers', () => {
      const df = DataFrame.fromColumns({
        age: [25, 30, 22],
      });
      expect(df.col('age').dtype.kind).toBe('int32');
    });

    test('fromColumns() infers float64 for decimals', () => {
      const df = DataFrame.fromColumns({
        score: [95.5, 87.2, 91.8],
      });
      expect(df.col('score').dtype.kind).toBe('float64');
    });

    test('fromColumns() throws on mismatched lengths', () => {
      expect(() => {
        DataFrame.fromColumns({
          age: [25, 30],
          name: ['Alice', 'Bob', 'Carol'],
        });
      }).toThrow();
    });
  });

  describe('column access', () => {
    test('col() returns typed Series', () => {
      const df = DataFrame.from(schema, sampleData);

      const ages = df.col('age');
      expect(ages.dtype.kind).toBe('int32');
      expect([...ages]).toEqual([25, 30, 22]);

      const names = df.col('name');
      expect(names.dtype.kind).toBe('string');
      expect([...names]).toEqual(['Alice', 'Bob', 'Carol']);
    });

    test('columns() returns column names', () => {
      const df = DataFrame.from(schema, sampleData);
      expect(df.columns()).toEqual(['age', 'name', 'score']);
    });
  });

  describe('row operations', () => {
    test('head() returns first n rows', () => {
      const df = DataFrame.from(schema, sampleData);
      const head = df.head(2);
      expect(head.shape).toEqual([2, 3]);
      expect([...head.col('name')]).toEqual(['Alice', 'Bob']);
    });

    test('tail() returns last n rows', () => {
      const df = DataFrame.from(schema, sampleData);
      const tail = df.tail(1);
      expect(tail.shape).toEqual([1, 3]);
      expect([...tail.col('name')]).toEqual(['Carol']);
    });

    test('select() picks specific columns', () => {
      const df = DataFrame.from(schema, sampleData);
      const selected = df.select('name', 'age');
      expect(selected.shape).toEqual([3, 2]);
      expect(selected.columns()).toEqual(['name', 'age']);
    });
  });

  describe('iteration', () => {
    test('rows() yields row objects', () => {
      const df = DataFrame.from(schema, sampleData);
      const rows = [...df.rows()];

      expect(rows.length).toBe(3);
      expect(rows[0]).toEqual({ age: 25, name: 'Alice', score: 95.5 });
    });
  });

  describe('display', () => {
    test('toString() produces ASCII table', () => {
      const df = DataFrame.from(schema, sampleData);
      const str = df.toString();

      expect(str).toContain('age');
      expect(str).toContain('name');
      expect(str).toContain('Alice');
      expect(str).toContain('[3 rows × 3 columns]');
    });
  });

  describe('filtering', () => {
    test('filter() returns matching rows', () => {
      const df = DataFrame.from(schema, sampleData);
      const filtered = df.filter((row) => row.age > 23);
      expect(filtered.shape[0]).toBe(2);
      expect([...filtered.col('name')]).toEqual(['Alice', 'Bob']);
    });

    test('where() filters by column condition', () => {
      const df = DataFrame.from(schema, sampleData);
      const filtered = df.where('age', '>', 23);
      expect(filtered.shape[0]).toBe(2);
    });

    test('where() supports equality', () => {
      const df = DataFrame.from(schema, sampleData);
      const filtered = df.where('name', '=', 'Bob');
      expect(filtered.shape[0]).toBe(1);
      expect([...filtered.col('age')]).toEqual([30]);
    });
  });

  describe('sorting', () => {
    test('sort() sorts by column ascending', () => {
      const df = DataFrame.from(schema, sampleData);
      const sorted = df.sort('age');
      expect([...sorted.col('age')]).toEqual([22, 25, 30]);
    });

    test('sort() sorts descending when false', () => {
      const df = DataFrame.from(schema, sampleData);
      const sorted = df.sort('score', false);
      expect([...sorted.col('name')]).toEqual(['Alice', 'Carol', 'Bob']);
    });
  });

  describe('groupby', () => {
    test('groupby().count() counts groups', () => {
      const df = DataFrame.from({ category: m.string(), value: m.int32() }, [
        { category: 'A', value: 10 },
        { category: 'B', value: 20 },
        { category: 'A', value: 30 },
      ]);
      const counts = df.groupby('category').count();
      expect(counts.length).toBe(2);
    });

    test('groupby().sum() sums by group', () => {
      const df = DataFrame.from({ category: m.string(), value: m.int32() }, [
        { category: 'A', value: 10 },
        { category: 'B', value: 20 },
        { category: 'A', value: 30 },
      ]);
      const sums = df.groupby('category').sum('value');
      const aRow = sums.find((r) => r.category === 'A');
      expect(aRow?.value).toBe(40);
    });
  });

  describe('apply and info', () => {
    test('apply() transforms rows', () => {
      const df = DataFrame.from(schema, sampleData);
      const results = df.apply((row) => `${row.name}: ${row.age}`);
      expect(results).toEqual(['Alice: 25', 'Bob: 30', 'Carol: 22']);
    });

    test('describe() returns numeric stats', () => {
      const df = DataFrame.from(schema, sampleData);
      const desc = df.describe();
      expect(desc.age).toBeDefined();
      expect(desc.age.mean).toBeCloseTo(25.67, 1);
    });

    test('info() returns DataFrame metadata', () => {
      const df = DataFrame.from(schema, sampleData);
      const info = df.info();
      expect(info.rows).toBe(3);
      expect(info.columns).toBe(3);
      expect(info.dtypes.age).toBe('int32');
    });

    test('toArray() returns row objects', () => {
      const df = DataFrame.from(schema, sampleData);
      const arr = df.toArray();
      expect(arr.length).toBe(3);
      expect(arr[0]).toEqual({ age: 25, name: 'Alice', score: 95.5 });
    });
  });

  describe('column manipulation', () => {
    test('drop() removes specified columns', () => {
      const df = DataFrame.from(schema, sampleData);
      const dropped = df.drop('score');

      expect(dropped.shape).toEqual([3, 2]);
      expect(dropped.columns()).toEqual(['age', 'name']);
    });

    test('drop() removes multiple columns', () => {
      const df = DataFrame.from(schema, sampleData);
      const dropped = df.drop('age', 'score');

      expect(dropped.shape).toEqual([3, 1]);
      expect(dropped.columns()).toEqual(['name']);
    });

    test('rename() renames columns by mapping', () => {
      const df = DataFrame.from(schema, sampleData);
      const renamed = df.rename({ age: 'years', name: 'fullName' });

      expect(renamed.columns()).toEqual(['years', 'fullName', 'score']);
    });

    test('assign() adds new column from array', () => {
      const df = DataFrame.from(schema, sampleData);
      const assigned = df.assign('grade', ['A', 'B', 'A']);

      expect(assigned.columns().length).toBe(4);
      expect([...assigned.col('grade')]).toEqual(['A', 'B', 'A']);
    });

    test('assign() adds new column from function', () => {
      const df = DataFrame.from(schema, sampleData);
      const assigned = df.assign('doubled', (row) => row.age * 2);

      expect([...assigned.col('doubled')]).toEqual([50, 60, 44]);
    });
  });

  describe('missing value operations', () => {
    test('dropna() removes rows with NaN', () => {
      const dfWithNaN = DataFrame.from(
        { value: m.float64() },
        [{ value: 1 }, { value: Number.NaN }, { value: 3 }],
      );
      const cleaned = dfWithNaN.dropna();

      expect(cleaned.shape[0]).toBe(2);
      expect([...cleaned.col('value')]).toEqual([1, 3]);
    });

    test('fillna() replaces NaN with value', () => {
      const dfWithNaN = DataFrame.from(
        { value: m.float64() },
        [{ value: 1 }, { value: Number.NaN }, { value: 3 }],
      );
      const filled = dfWithNaN.fillna(0);

      expect([...filled.col('value')]).toEqual([1, 0, 3]);
    });

    test('isna() returns boolean DataFrame', () => {
      const dfWithNaN = DataFrame.from(
        { value: m.float64() },
        [{ value: 1 }, { value: Number.NaN }, { value: 3 }],
      );
      const mask = dfWithNaN.isna();

      expect([...mask.col('value')]).toEqual([false, true, false]);
    });
  });

  describe('copy and sample', () => {
    test('copy() creates independent copy', () => {
      const df = DataFrame.from(schema, sampleData);
      const dfCopy = df.copy();

      expect(dfCopy.shape).toEqual(df.shape);
      expect(dfCopy.toArray()).toEqual(df.toArray());
    });

    test('sample() returns n random rows', () => {
      const df = DataFrame.from(schema, sampleData);
      const sampled = df.sample(2);

      expect(sampled.shape[0]).toBe(2);
    });

    test('sample() returns all if n > length', () => {
      const df = DataFrame.from(schema, sampleData);
      const sampled = df.sample(10);

      expect(sampled.shape[0]).toBe(3);
    });
  });

  describe('indexing', () => {
    test('iloc() returns single row as object', () => {
      const df = DataFrame.from(schema, sampleData);
      const row = df.iloc(1);

      expect(row).toEqual({ age: 30, name: 'Bob', score: 87.2 });
    });

    test('iloc() returns slice as DataFrame', () => {
      const df = DataFrame.from(schema, sampleData);
      const sliced = df.iloc(0, 2);

      expect(sliced.shape[0]).toBe(2);
      expect([...sliced.col('name')]).toEqual(['Alice', 'Bob']);
    });

    test('loc() selects rows by indices', () => {
      const df = DataFrame.from(schema, sampleData);
      const selected = df.loc([0, 2]);

      expect(selected.shape[0]).toBe(2);
      expect([...selected.col('name')]).toEqual(['Alice', 'Carol']);
    });
  });
});
