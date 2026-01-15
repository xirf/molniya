import { describe, expect, test } from 'bun:test';
import { Series } from '../../src/core/series';

describe('Series', () => {
  describe('float64', () => {
    test('creates from array', () => {
      const s = Series.float64([1.1, 2.2, 3.3]);
      expect(s.length).toBe(3);
      expect(s.dtype.kind).toBe('float64');
    });

    test('at() returns correct values', () => {
      const s = Series.float64([1.5, 2.5, 3.5]);
      expect(s.at(0)).toBe(1.5);
      expect(s.at(1)).toBe(2.5);
      expect(s.at(2)).toBe(3.5);
    });

    test('at() returns undefined for out of bounds', () => {
      const s = Series.float64([1, 2, 3]);
      expect(s.at(-1)).toBeUndefined();
      expect(s.at(3)).toBeUndefined();
    });

    test('head() returns first n elements', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      const h = s.head(3);
      expect(h.length).toBe(3);
      expect([...h]).toEqual([1, 2, 3]);
    });

    test('tail() returns last n elements', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      const t = s.tail(2);
      expect(t.length).toBe(2);
      expect([...t]).toEqual([4, 5]);
    });

    test('slice() creates view', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      const sl = s.slice(1, 4);
      expect(sl.length).toBe(3);
      expect([...sl]).toEqual([2, 3, 4]);
    });

    test('is iterable', () => {
      const s = Series.float64([1, 2, 3]);
      expect([...s]).toEqual([1, 2, 3]);
    });
  });

  describe('int32', () => {
    test('creates from array', () => {
      const s = Series.int32([1, 2, 3]);
      expect(s.length).toBe(3);
      expect(s.dtype.kind).toBe('int32');
    });

    test('truncates floats to integers', () => {
      const s = Series.int32([1.9, 2.1, 3.5]);
      expect([...s]).toEqual([1, 2, 3]);
    });
  });

  describe('string', () => {
    test('creates from array', () => {
      const s = Series.string(['a', 'b', 'c']);
      expect(s.length).toBe(3);
      expect(s.dtype.kind).toBe('string');
    });

    test('at() returns correct values', () => {
      const s = Series.string(['hello', 'world']);
      expect(s.at(0)).toBe('hello');
      expect(s.at(1)).toBe('world');
    });
  });

  describe('bool', () => {
    test('creates from array', () => {
      const s = Series.bool([true, false, true]);
      expect(s.length).toBe(3);
      expect(s.dtype.kind).toBe('bool');
    });

    test('at() returns boolean values', () => {
      const s = Series.bool([true, false, true]);
      expect(s.at(0)).toBe(true);
      expect(s.at(1)).toBe(false);
      expect(s.at(2)).toBe(true);
    });
  });

  describe('display', () => {
    test('toString() formats correctly', () => {
      const s = Series.float64([1.5, 2.5, 3.5]);
      const str = s.toString();
      expect(str).toContain('dtype: float64');
      expect(str).toContain('length: 3');
    });
  });

  describe('statistics', () => {
    test('sum() returns total', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      expect(s.sum()).toBe(15);
    });

    test('mean() returns average', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      expect(s.mean()).toBe(3);
    });

    test('min() returns minimum', () => {
      const s = Series.float64([5, 2, 8, 1, 9]);
      expect(s.min()).toBe(1);
    });

    test('max() returns maximum', () => {
      const s = Series.float64([5, 2, 8, 1, 9]);
      expect(s.max()).toBe(9);
    });

    test('std() returns standard deviation', () => {
      const s = Series.float64([2, 4, 4, 4, 5, 5, 7, 9]);
      expect(s.std()).toBeCloseTo(2, 1);
    });

    test('describe() returns summary', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      const desc = s.describe();
      expect(desc.count).toBe(5);
      expect(desc.mean).toBe(3);
      expect(desc.min).toBe(1);
      expect(desc.max).toBe(5);
    });
  });

  describe('transformations', () => {
    test('filter() returns matching values', () => {
      const s = Series.float64([1, 2, 3, 4, 5]);
      const filtered = s.filter((v) => v > 2);
      expect([...filtered]).toEqual([3, 4, 5]);
    });

    test('map() transforms values', () => {
      const s = Series.float64([1, 2, 3]);
      const doubled = s.map((v) => v * 2);
      expect([...doubled]).toEqual([2, 4, 6]);
    });

    test('sort() sorts ascending by default', () => {
      const s = Series.float64([3, 1, 4, 1, 5]);
      const sorted = s.sort();
      expect([...sorted]).toEqual([1, 1, 3, 4, 5]);
    });

    test('sort(false) sorts descending', () => {
      const s = Series.float64([3, 1, 4, 1, 5]);
      const sorted = s.sort(false);
      expect([...sorted]).toEqual([5, 4, 3, 1, 1]);
    });

    test('unique() removes duplicates', () => {
      const s = Series.float64([1, 2, 2, 3, 3, 3]);
      const unique = s.unique();
      expect([...unique]).toEqual([1, 2, 3]);
    });

    test('valueCounts() counts occurrences', () => {
      const s = Series.string(['a', 'b', 'a', 'c', 'a']);
      const counts = s.valueCounts();
      expect(counts.get('a')).toBe(3);
      expect(counts.get('b')).toBe(1);
      expect(counts.get('c')).toBe(1);
    });

    test('toArray() returns plain array', () => {
      const s = Series.float64([1, 2, 3]);
      const arr = s.toArray();
      expect(arr).toEqual([1, 2, 3]);
      expect(Array.isArray(arr)).toBe(true);
    });
  });

  describe('missing value operations', () => {
    test('isna() detects NaN values', () => {
      const s = Series.float64([1, Number.NaN, 3]);
      const na = s.isna();
      expect([...na]).toEqual([false, true, false]);
    });

    test('fillna() replaces NaN with value', () => {
      const s = Series.float64([1, Number.NaN, 3]);
      const filled = s.fillna(0);
      expect([...filled]).toEqual([1, 0, 3]);
    });

    test('copy() creates independent copy', () => {
      const s = Series.float64([1, 2, 3]);
      const c = s.copy();
      expect([...c]).toEqual([1, 2, 3]);
      expect(c.length).toBe(s.length);
    });
  });
});
