import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { ChunkCache, RowIndex } from '../../src/core/lazyframe';
import { scanCsv } from '../../src/io/csv';

const TMP_DIR = './tests/fixtures';
const QUOTED_PATH = `${TMP_DIR}/lazy-quoted.csv`;

beforeAll(async () => {
  const quoted = 'id,name,flag\n1,"hello, world",True\n2,"He said ""Hi""",false\n';
  await Bun.write(QUOTED_PATH, quoted);
});

afterAll(async () => {
  const file = Bun.file(QUOTED_PATH);
  if (await file.exists()) {
    await file.delete().catch(() => {});
  }
});

describe('ChunkCache', () => {
  test('evicts least recently used chunk when memory exceeded', () => {
    const cache = new ChunkCache({ maxMemoryBytes: 60, chunkSize: 2 });

    // Chunk size approx: 40 bytes
    cache.set(0, {
      startRow: 0,
      columns: { id: [1, 2] },
      rowCount: 2,
      sizeBytes: 40,
    });
    // Another 40 bytes -> Total 80 > 60 -> Evict 0
    cache.set(1, {
      startRow: 2,
      columns: { id: [3, 4] },
      rowCount: 2,
      sizeBytes: 40,
    });

    expect(cache.size).toBe(1);
    expect(cache.get(0)).toBeUndefined();
    expect(cache.get(1)?.columns.id?.[0]).toBe(3);
  });

  test('estimateSize uses field count when no rows', () => {
    const estimate = ChunkCache.estimateSize({}, 0);
    expect(estimate).toBe(0);
  });

  test('provides chunk utilities and clear resets memory', () => {
    const cache = new ChunkCache({ maxMemoryBytes: 1_000, chunkSize: 5 });

    expect(cache.getChunkIndex(9)).toBe(1);
    expect(cache.getChunkRange(2)).toEqual([10, 15]);

    cache.set(0, {
      startRow: 0,
      columns: { id: [1] },
      rowCount: 1,
      sizeBytes: 100,
    });
    expect(cache.size).toBe(1);
    expect(cache.memoryUsed).toBe(100);

    cache.clear();
    expect(cache.size).toBe(0);
    expect(cache.memoryUsed).toBe(0);
  });
});

describe('RowIndex', () => {
  test('builds offsets and enforces bounds', async () => {
    const csv = 'a,b\n1,2\n3,4\n';
    const path = `${TMP_DIR}/row-index-test-1.csv`;
    await Bun.write(path, csv);

    const idx = await RowIndex.build(Bun.file(path), true);

    expect(idx.rowCount).toBe(2);
    expect(idx.getRowOffset(0)).toBe(csv.indexOf('1'));
    // Row 1 starts at '3'. The previous line '1,2\n' ends before '3'.
    // 'a,b\n' (4) + '1,2\n' (4) = 8
    expect(idx.getRowOffset(1)).toBe(csv.indexOf('3'));
    expect(() => idx.getRowOffset(5)).toThrow();
  });

  test('getRowsRange returns contiguous range to file end', async () => {
    const csv = 'a\n1\n2\n';
    const path = `${TMP_DIR}/row-index-test-2.csv`;
    await Bun.write(path, csv);
    const idx = await RowIndex.build(Bun.file(path), false);

    const [start, end] = idx.getRowsRange(0, 1);
    expect(start).toBe(0);
    expect(end).toBe(csv.indexOf('1'));
  });
});

import { parseLine, parseValue } from '../../src/core/lazyframe/parser';

describe('LazyFrame parse internals', () => {
  test('parses quoted fields and booleans via scanCsv', async () => {
    const lazy = await scanCsv(QUOTED_PATH);
    const head = await lazy.head(2);

    expect(head.col('name').at(0)).toBe('hello, world');
    expect(head.col('name').at(1)).toBe('He said "Hi"');
    expect(head.col('flag').at(0)).toBe(true);
  });

  test('column view retains behavior for head/tail/collect/info', async () => {
    const lazy = await scanCsv(QUOTED_PATH);
    const view = lazy.select('id', 'flag');

    const head = await view.head(1);
    expect(head.columns()).toEqual(['id', 'flag']);

    const tail = await view.tail(1);
    expect(tail.shape[0]).toBe(1);

    const result = await view.collect(1);
    expect(result.data!.shape[1]).toBe(2);

    const info = view.info();
    expect(info.columns).toBe(2);
  });

  test('internal parse line/value helpers handle quotes and bools', () => {
    expect(parseLine('"a,b",c', ',')).toEqual(['a,b', 'c']);

    const boolTrue = parseValue('True', { kind: 'bool', nullable: true });
    const boolFalse = parseValue('0', { kind: 'bool', nullable: true });
    expect(boolTrue).toBe(true);
    expect(boolFalse).toBe(false);
  });
});
