import { describe, expect, test } from 'bun:test';
import { DataFrame, m, Series } from '../../src';
import {
  ColumnNotFoundError,
  TypeMismatchError,
  IndexOutOfBoundsError,
  MornyeError,
  SchemaError,
} from '../../src/errors';

describe('Error Messages', () => {
  const schema = {
    name: m.string(),
    age: m.int32(),
  } as const;

  const df = DataFrame.from(schema, [
    { name: 'Alice', age: 25 },
    { name: 'Bob', age: 30 },
  ]);

  describe('ColumnNotFoundError', () => {
    test('includes column name and available columns', () => {
      try {
        df.col('nonexistent' as never);
        expect(true).toBe(false);
      } catch (e) {
        expect(e).toBeInstanceOf(ColumnNotFoundError);
        const err = e as ColumnNotFoundError;
        expect(err.column).toBe('nonexistent');
        expect(err.available).toContain('name');
        expect(err.available).toContain('age');
      }
    });

    test('format() returns Rust-style message with location', () => {
      const err = new ColumnNotFoundError('salary', ['name', 'age', 'score']);
      const formatted = err.format();
      expect(formatted).toContain('error: column not found at');
      expect(formatted).toContain("--> df.col('salary')");
      expect(formatted).toContain("column 'salary' does not exist");
      expect(formatted).toContain('help: available columns are:');
    });
  });

  describe('TypeMismatchError', () => {
    test('thrown for invalid Series operations', () => {
      const names = Series.string(['a', 'b']);
      try {
        names.sum();
        expect(true).toBe(false);
      } catch (e) {
        expect(e).toBeInstanceOf(TypeMismatchError);
        const err = e as TypeMismatchError;
        expect(err.operation).toBe('sum');
        expect(err.actualType).toBe('string');
      }
    });

    test('format() shows operation and type info', () => {
      const err = new TypeMismatchError('sum', 'string', ['float64', 'int32']);
      const formatted = err.format();
      expect(formatted).toContain('error: type mismatch');
      expect(formatted).toContain("cannot call 'sum()' on string Series");
      expect(formatted).toContain("help: 'sum' requires float64 or int32");
    });
  });

  describe('IndexOutOfBoundsError', () => {
    test('includes valid range in hint', () => {
      const err = new IndexOutOfBoundsError(100, 0, 50);
      const formatted = err.format();
      expect(formatted).toContain('error: index out of bounds');
      expect(formatted).toContain('index 100');
      expect(formatted).toContain('help: valid range is 0 to 50');
    });
  });

  describe('SchemaError', () => {
    test('shows detail and hint', () => {
      const err = new SchemaError("unknown dtype 'complex'", 'supported: float64, int32, string, bool');
      const formatted = err.format();
      expect(formatted).toContain('error: schema error');
      expect(formatted).toContain("unknown dtype 'complex'");
      expect(formatted).toContain('help: supported:');
    });
  });

  describe('MornyeError base class', () => {
    test('format() includes location from stack trace', () => {
      const err = new MornyeError('test error', 'test hint');
      const formatted = err.format();
      expect(formatted).toContain('error: test error');
      expect(formatted).toContain('-->'); // location pointer
      expect(formatted).toContain('help: test hint');
    });

    test('format() includes visual tree structure', () => {
      const err = new MornyeError('test');
      const formatted = err.format();
      expect(formatted).toContain('   |');
      expect(formatted).toContain('   └──');
    });
  });
});
