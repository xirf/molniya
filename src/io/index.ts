/**
 * I/O module exports
 */

// CSV parsing
export {
  parseCsvLine,
  parseCsvHeader,
  type CsvParseOptions,
} from './csv-parser';

// CSV eager reading
export { readCsv, readCsvFromString } from './csv-reader';
export type { CsvOptions } from './csv-reader';

// CSV streaming/chunked scanning
export { scanCsv, scanCsvFromString } from './csv-scanner';
export type { CsvScanOptions } from './csv-scanner';

// Streaming CSV batches
export { streamCsvBatches } from './csv-streamer';
export type { CsvStreamOptions, CsvStreamPredicate } from './csv-streamer';

// Columnar batch primitives
export {
  ColumnarBatchBuilder,
  DEFAULT_BATCH_BYTES,
} from './columnar-batch';
export type {
  ColumnarBatch,
  ColumnarBatchColumn,
  ColumnarBatchIterator,
  ColumnarData,
} from './columnar-batch';
