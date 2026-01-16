import { describe, expect, test } from 'bun:test';
import { DataFrame } from '../../src/core/dataframe';

describe('DataFrame.concat()', () => {
  test('concatenates two DataFrames vertically', () => {
    const df1 = DataFrame.fromColumns({ a: [1, 2], b: ['x', 'y'] });
    const df2 = DataFrame.fromColumns({ a: [3, 4], b: ['z', 'w'] });

    const result = DataFrame.concat(df1, df2);

    expect(result.shape).toEqual([4, 2]);
    expect([...result.col('a')]).toEqual([1, 2, 3, 4]);
    expect([...result.col('b')]).toEqual(['x', 'y', 'z', 'w']);
  });

  test('concatenates multiple DataFrames', () => {
    const df1 = DataFrame.fromColumns({ x: [1] });
    const df2 = DataFrame.fromColumns({ x: [2] });
    const df3 = DataFrame.fromColumns({ x: [3] });

    const result = DataFrame.concat(df1, df2, df3);

    expect(result.shape).toEqual([3, 1]);
    expect([...result.col('x')]).toEqual([1, 2, 3]);
  });

  test('returns single DataFrame unchanged', () => {
    const df = DataFrame.fromColumns({ a: [1, 2, 3] });
    const result = DataFrame.concat(df);

    expect(result).toBe(df);
  });

  test('throws on mismatched column count', () => {
    const df1 = DataFrame.fromColumns({ a: [1], b: [2] });
    const df2 = DataFrame.fromColumns({ a: [3] });

    // @ts-expect-error Testing runtime schema validation
    expect(() => DataFrame.concat(df1, df2)).toThrow();
  });

  test('throws on mismatched column names', () => {
    const df1 = DataFrame.fromColumns({ a: [1] });
    const df2 = DataFrame.fromColumns({ b: [2] });

    // @ts-expect-error Testing runtime schema validation
    expect(() => DataFrame.concat(df1, df2)).toThrow();
  });
});

describe('DataFrame.merge()', () => {
  test('inner join matches rows', () => {
    const users = DataFrame.fromColumns({ id: [1, 2, 3], name: ['Alice', 'Bob', 'Carol'] });
    const orders = DataFrame.fromColumns({
      userId: [1, 1, 2],
      product: ['Apple', 'Banana', 'Cherry'],
    });

    const result = users.merge(orders, { left: 'id', right: 'userId', how: 'inner' });

    expect(result.shape[0]).toBe(3);
    expect([...result.col('name')]).toEqual(['Alice', 'Alice', 'Bob']);
    expect([...result.col('product')]).toEqual(['Apple', 'Banana', 'Cherry']);
  });

  test('left join keeps unmatched left rows', () => {
    const users = DataFrame.fromColumns({ id: [1, 2, 3], name: ['Alice', 'Bob', 'Carol'] });
    const orders = DataFrame.fromColumns({ userId: [1], product: ['Apple'] });

    const result = users.merge(orders, { left: 'id', right: 'userId', how: 'left' });

    expect(result.shape[0]).toBe(3);
    expect([...result.col('name')]).toEqual(['Alice', 'Bob', 'Carol']);
  });

  test('right join keeps unmatched right rows', () => {
    const users = DataFrame.fromColumns({ id: [1], name: ['Alice'] });
    const orders = DataFrame.fromColumns({ userId: [1, 2], product: ['Apple', 'Banana'] });

    const result = users.merge(orders, { left: 'id', right: 'userId', how: 'right' });

    expect(result.shape[0]).toBe(2);
    expect([...result.col('product')]).toEqual(['Apple', 'Banana']);
  });

  test('defaults to inner join', () => {
    const df1 = DataFrame.fromColumns({ key: [1, 2] });
    const df2 = DataFrame.fromColumns({ key: [2, 3] });

    const result = df1.merge(df2, { left: 'key', right: 'key' });

    expect(result.shape[0]).toBe(1);
  });
});

describe('DataFrame.unique()', () => {
  test('removes duplicate rows', () => {
    const df = DataFrame.fromColumns({
      a: [1, 1, 2, 2, 3],
      b: ['x', 'x', 'y', 'y', 'z'],
    });

    const result = df.unique();

    expect(result.shape[0]).toBe(3);
    expect([...result.col('a')]).toEqual([1, 2, 3]);
  });
});
