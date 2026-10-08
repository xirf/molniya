import { cp, mkdir, rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import { fingerprint } from "./provenance.ts";

const root = resolve(import.meta.dir, "..");
const mutations = [
  {
    id: "MUT-FILTER-NULL",
    file: "execute.ts",
    from: "predicate!.evaluate((column) => batch.get(row, column)) === true",
    to: "predicate!.evaluate((column) => batch.get(row, column)) !== false",
    requirement: "REQ-NULL-001",
  },
  {
    id: "MUT-DIV-ZERO",
    file: "expr.ts",
    from: 'if (expr.op === "div" && (y === 0 || y === 0n)) return null;',
    to: 'if (expr.op === "div" && (y === 0 || y === 0n)) return 0;',
    requirement: "REQ-EXPR-001",
  },
  {
    id: "MUT-BUDGET-EQUALITY",
    file: "memory.ts",
    from: "bytes > this.limit - this.#used",
    to: "bytes >= this.limit - this.#used",
    requirement: "REQ-MEM-001",
  },
  {
    id: "MUT-LIMIT-BOUNDARY",
    file: "execute.ts",
    from: "emitted >= plan.count",
    to: "emitted > plan.count",
    requirement: "REQ-REL-001",
  },
  {
    id: "MUT-SUM-EMPTY",
    file: "execute.ts",
    from: "value: null, seen: false",
    to: "value: 0, seen: false",
    requirement: "REQ-AGG-001",
  },
  {
    id: "MUT-CSV-QUOTE",
    file: "csv.ts",
    from: "text.replaceAll('\"', '\"\"')",
    to: "text.replaceAll('\"', '\"')",
    requirement: "REQ-CSV-003",
  },
];
const results = [];
for (const mutation of mutations) {
  const work = join(root, "coverage/mutations", mutation.id);
  await rm(work, { recursive: true, force: true });
  await mkdir(work, { recursive: true });
  for (const directory of ["src", "test", "docs", "scripts"])
    await cp(join(root, directory), join(work, directory), { recursive: true });
  const path = join(work, "src", mutation.file);
  const source = await Bun.file(path).text();
  if (!source.includes(mutation.from)) throw new Error(`Mutation location missing: ${mutation.id}`);
  await Bun.write(path, source.replace(mutation.from, mutation.to));
  const child = Bun.spawn(
    [process.execPath, "test", "./test/suite.test.ts", "--timeout", "60000"],
    {
      cwd: work,
      env: {
        ...process.env,
        MOLNIYA_PROJECT_ROOT: root,
        MOLNIYA_EVIDENCE_DIR: join(work, "evidence"),
      },
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  const [stdout, stderr, exit] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  await Bun.write(join(work, "test.log"), stdout + stderr);
  const report = await Bun.file(join(work, "evidence/tests.json")).json();
  const killedBy = report.cases
    .filter((test: { status: string }) => test.status === "failed")
    .map((test: { id: string }) => test.id);
  const killed = exit !== 0 && killedBy.length > 0;
  results.push({ ...mutation, killed, killedBy });
  console.log(`${mutation.id}: ${killed ? "killed" : "SURVIVED"}`);
}
await Bun.write(
  join(root, "evidence/mutations.json"),
  JSON.stringify(
    {
      sourceDigest: await fingerprint(root),
      method: "Six targeted semantic mutations; not an exhaustive mutation score",
      results,
    },
    null,
    2,
  ) + "\n",
);
if (results.some((result) => !result.killed))
  throw new Error("A required semantic mutation survived");
