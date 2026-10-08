import { EngineError, integer } from "./errors.ts";
import { type Allocation, MemoryBudget, type MemorySnapshot } from "./memory.ts";
import { DiskSpillStore, type SpillStore } from "./spill.ts";

export interface ExecutionOptions {
  readonly memoryBytes?: number;
  readonly batchBytes?: number;
  readonly signal?: AbortSignal;
  readonly beforeAllocate?: (event: Allocation) => void;
  readonly spillStore?: (context: Context) => Promise<SpillStore>;
  readonly onFinish?: (metrics: ExecutionMetrics) => void;
}
export interface ExecutionMetrics {
  readonly memory: MemorySnapshot;
  readonly scannedRows: number;
  readonly outputRows: number;
  readonly spillRuns: number;
}
export class Context {
  readonly memory: MemoryBudget;
  readonly batchBytes: number;
  scannedRows = 0;
  outputRows = 0;
  spillRuns = 0;
  #store?: SpillStore;
  constructor(readonly options: ExecutionOptions = {}) {
    this.memory = new MemoryBudget(options.memoryBytes ?? 64 * 1024 * 1024, options.beforeAllocate);
    this.batchBytes = options.batchBytes ?? Math.min(64 * 1024, Math.floor(this.memory.limit / 16));
    integer(this.batchBytes, "batchBytes", 1);
    if (this.batchBytes > this.memory.limit / 4)
      throw new EngineError("RANGE", "batchBytes must not exceed one quarter of memoryBytes");
  }
  check(): void {
    if (this.options.signal?.aborted) throw new EngineError("CANCELLED", "Query cancelled");
  }
  async store(): Promise<SpillStore> {
    this.check();
    return (this.#store ??= await (this.options.spillStore?.(this) ??
      DiskSpillStore.create(undefined, this.memory)));
  }
  async close(): Promise<void> {
    try {
      await this.#store?.close();
    } catch (error) {
      try {
        await this.#store?.close();
      } catch (retry) {
        throw new AggregateError([error, retry], "Spill cleanup failed");
      }
      throw error;
    }
  }
  report(): void {
    this.options.onFinish?.(
      Object.freeze({
        memory: this.memory.snapshot,
        scannedRows: this.scannedRows,
        outputRows: this.outputRows,
        spillRuns: this.spillRuns,
      }),
    );
  }
}
