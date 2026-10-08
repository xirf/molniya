import { EngineError, integer, requireThat } from "./errors.ts";

export interface MemorySnapshot {
  readonly used: number;
  readonly peak: number;
  readonly limit: number;
  readonly allocations: number;
}
export interface Allocation {
  readonly bytes: number;
  readonly label: string;
  readonly ordinal: number;
}

/** One query owns one budget. The hook is also the deterministic allocation fault boundary. */
export class MemoryBudget {
  #used = 0;
  #peak = 0;
  #allocations = 0;
  constructor(
    readonly limit: number,
    private readonly beforeAllocate?: (event: Allocation) => void,
  ) {
    integer(limit, "memoryBytes", 1);
  }
  reserve(bytes: number, label: string): Lease {
    this.grow(bytes, label);
    return new Lease(this, bytes, label);
  }
  grow(bytes: number, label: string): void {
    integer(bytes, "allocation bytes");
    if (bytes === 0) return;
    if (bytes > this.limit - this.#used)
      throw new EngineError(
        "MEMORY",
        `${label}: need ${bytes} bytes, ${this.limit - this.#used} available`,
      );
    this.#allocations++;
    this.beforeAllocate?.({ bytes, label, ordinal: this.#allocations });
    this.#used += bytes;
    this.#peak = Math.max(this.#peak, this.#used);
  }
  shrink(bytes: number): void {
    integer(bytes, "released bytes");
    requireThat(bytes <= this.#used, "RANGE", "Release exceeds reservations");
    this.#used -= bytes;
  }
  get snapshot(): MemorySnapshot {
    return Object.freeze({
      used: this.#used,
      peak: this.#peak,
      limit: this.limit,
      allocations: this.#allocations,
    });
  }
}

export class Lease {
  #released = false;
  constructor(
    private readonly budget: MemoryBudget,
    private bytes: number,
    private readonly label: string,
  ) {}
  resize(bytes: number): void {
    requireThat(!this.#released, "DISPOSED", "Lease is released");
    integer(bytes, "allocation bytes");
    if (bytes > this.bytes) this.budget.grow(bytes - this.bytes, this.label);
    else this.budget.shrink(this.bytes - bytes);
    this.bytes = bytes;
  }
  release(): void {
    if (this.#released) return;
    this.budget.shrink(this.bytes);
    this.#released = true;
  }
}
