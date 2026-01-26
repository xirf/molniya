import type {
  ColumnarBatch,
  ColumnarBatchColumn,
  ColumnarBatchIterator,
} from '../../io/columnar-batch';
import type { BatchFilter } from './batch-utils';
import {
  buildFilteredColumn,
  buildSchemaFromColumns,
  computeBatchByteSize,
  getBatchValue,
  matchesFilter,
} from './batch-utils';

export function filterBatchIterator(
  iterator: ColumnarBatchIterator,
  filters: BatchFilter[],
): ColumnarBatchIterator {
  return {
    schema: iterator.schema,
    columnOrder: iterator.columnOrder,
    batchSizeBytes: iterator.batchSizeBytes,
    async *[Symbol.asyncIterator](): AsyncGenerator<ColumnarBatch> {
      for await (const batch of iterator) {
        if (filters.length === 0) {
          yield batch;
          continue;
        }

        const predicates = filters.map((filter) => {
          const column = batch.columns[filter.column];
          if (!column) {
            throw new Error(`Filter column '${filter.column}' not found in batch`);
          }
          return { filter, column };
        });

        const matching: number[] = [];
        for (let row = 0; row < batch.rowCount; row++) {
          let matches = true;
          for (const { filter, column } of predicates) {
            const value = getBatchValue(column, row);
            if (!matchesFilter(value, filter.operator, filter.value)) {
              matches = false;
              break;
            }
          }
          if (matches) matching.push(row);
        }

        if (matching.length === 0) {
          continue;
        }

        const filteredColumns: Record<string, ColumnarBatchColumn> = {};
        for (const [name, column] of Object.entries(batch.columns)) {
          filteredColumns[name] = buildFilteredColumn(column, matching);
        }

        yield {
          rowCount: matching.length,
          byteSize: computeBatchByteSize(filteredColumns),
          columns: filteredColumns,
        };
      }
    },
  };
}

export function selectBatchIterator(
  iterator: ColumnarBatchIterator,
  columns: string[],
): ColumnarBatchIterator {
  const outputOrder = columns.slice();
  const outputSchema = buildSchemaFromColumns(iterator.schema, outputOrder);

  return {
    schema: outputSchema,
    columnOrder: outputOrder,
    batchSizeBytes: iterator.batchSizeBytes,
    async *[Symbol.asyncIterator](): AsyncGenerator<ColumnarBatch> {
      for await (const batch of iterator) {
        const selected: Record<string, ColumnarBatchColumn> = {};
        for (const name of outputOrder) {
          const column = batch.columns[name];
          if (!column) {
            throw new Error(`Select column '${name}' not found in batch`);
          }
          selected[name] = column;
        }

        yield {
          rowCount: batch.rowCount,
          byteSize: computeBatchByteSize(selected),
          columns: selected,
        };
      }
    },
  };
}
