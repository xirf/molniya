import { describe, expect, test } from 'bun:test';
import { RowIndex } from '../../src/core/lazyframe';
import { parseLine, parseValue } from '../../src/core/lazyframe/parser';

const PATH = './tests/fixtures/lazy-private.csv';

describe('LazyFrame private parsing helpers', () => {
  test('parseLine and parseValue cover quote and bool branches', async () => {
    // No need to write file or creating LazyFrame for unit testing parser functions

    expect(parseLine('"a,b",c', ',')).toEqual(['a,b', 'c']);
    expect(parseValue('True', { kind: 'bool', nullable: true })).toBe(true);
    expect(parseValue('nope', { kind: 'bool', nullable: true })).toBe(false);
  });

  test('RowIndex memory usage reflects segments', async () => {
    const csv = 'a\n1\n';
    const path = `${PATH}.tmp`;
    await Bun.write(path, csv);
    const idx = await RowIndex.build(Bun.file(path), false);
    expect(idx.memoryUsage()).toBeGreaterThan(0);
  });
});
