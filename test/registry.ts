import { afterAll, test } from "bun:test";
import { mkdir } from "node:fs/promises";
import { resolve, join } from "node:path";
import { fingerprint } from "../scripts/provenance.ts";
import requirements from "../docs/requirements.json";

export interface Case {
  id: string;
  requirements: readonly string[];
  category: string;
  objective: string;
  preconditions?: string;
  steps: string;
  expected: string;
  cleanup?: string;
}
const cases: (Case & {
  source: string;
  status: "pending" | "passed" | "failed";
  error?: string;
})[] = [];
const validRequirements = new Set(requirements.map((requirement) => requirement.id));

export function tc(spec: Case, run: () => unknown | Promise<unknown>): void {
  if (cases.some((test) => test.id === spec.id)) throw new Error(`Duplicate case ${spec.id}`);
  if (!spec.requirements.length || spec.requirements.some((id) => !validRequirements.has(id)))
    throw new Error(`Untraced case ${spec.id}`);
  const entry = {
    preconditions: "Fresh per-case fixtures and execution state; no network access",
    cleanup:
      "Dispose batches, close databases, and remove each case's temporary directory in finally blocks",
    ...spec,
    source:
      new Error().stack
        ?.split("\n")
        .find((line) => line.includes(".cases.ts"))
        ?.trim() ?? "test/suite.test.ts",
    status: "pending" as "pending" | "passed" | "failed",
  };
  cases.push(entry);
  test(`${spec.id} [${spec.requirements.join(",")}] ${spec.objective}`, async () => {
    try {
      await run();
      entry.status = "passed";
    } catch (error) {
      entry.status = "failed";
      Object.assign(entry, { error: String(error) });
      throw error;
    }
  });
}

afterAll(async () => {
  const root = process.env.MOLNIYA_PROJECT_ROOT ?? resolve(import.meta.dir, "..");
  const output = process.env.MOLNIYA_EVIDENCE_DIR ?? join(root, "evidence");
  await mkdir(output, { recursive: true });
  const sourceDigest = await fingerprint(root);
  const tools = (await Bun.file(join(root, "package.json")).json()).devDependencies;
  const report = {
    version: 1,
    tools,
    instrumented: Boolean((globalThis as { __coverage__?: unknown }).__coverage__),
    runtime: `Bun ${Bun.version}`,
    platform: `${process.platform}/${process.arch}`,
    sourceDigest,
    cases,
    traceability: requirements.map((requirement) => ({
      ...requirement,
      tests: cases
        .filter((test) => test.requirements.includes(requirement.id))
        .map((test) => ({ id: test.id, status: test.status })),
    })),
  };
  await Bun.write(join(output, "tests.json"), JSON.stringify(report, null, 2) + "\n");
  const coverage = (globalThis as typeof globalThis & { __coverage__?: unknown }).__coverage__;
  if (coverage) await Bun.write(join(output, "coverage-final.json"), JSON.stringify(coverage));
});
