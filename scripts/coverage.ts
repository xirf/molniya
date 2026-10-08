import { cp, mkdir, readdir, rm } from "node:fs/promises";
import { resolve, join } from "node:path";
import { createInstrumenter } from "istanbul-lib-instrument";
import { createCoverageMap } from "istanbul-lib-coverage";
import { createContext } from "istanbul-lib-report";
import { create } from "istanbul-reports";
import gates from "../docs/coverage-gates.json";

const root = resolve(import.meta.dir, "..");
const work = join(root, "coverage/work");
const evidence = join(root, "evidence");
await rm(work, { recursive: true, force: true });
await mkdir(join(work, "src"), { recursive: true });
await mkdir(evidence, { recursive: true });
await cp(join(root, "test"), join(work, "test"), { recursive: true });
await cp(join(root, "scripts"), join(work, "scripts"), { recursive: true });
await cp(join(root, "docs"), join(work, "docs"), { recursive: true });
await Bun.write(join(work, "package.json"), '{"type":"module"}\n');
const nativeEvidence = join(root, "coverage/native");
const native = Bun.spawn(
  [globalThis.process.execPath, "test", "./test/suite.test.ts", "--timeout", "60000"],
  {
    cwd: root,
    env: {
      ...globalThis.process.env,
      MOLNIYA_PROJECT_ROOT: root,
      MOLNIYA_EVIDENCE_DIR: nativeEvidence,
    },
    stdout: "inherit",
    stderr: "inherit",
  },
);
if ((await native.exited) !== 0) throw new Error("Uninstrumented tests failed");
const nativeResults = await Bun.file(join(nativeEvidence, "tests.json")).json();
await Bun.write(join(evidence, "native-tests.json"), JSON.stringify(nativeResults, null, 2) + "\n");
const zero = createCoverageMap({});
for (const name of (await readdir(join(root, "src")))
  .filter((name) => name.endsWith(".ts"))
  .sort()) {
  const path = join(root, "src", name);
  const instrumenter = createInstrumenter({
    esModules: true,
    parserPlugins: ["typescript"],
    compact: false,
  });
  await Bun.write(
    join(work, "src", name),
    instrumenter.instrumentSync(await Bun.file(path).text(), path),
  );
  zero.addFileCoverage(instrumenter.lastFileCoverage());
}
const process = Bun.spawn(
  [globalThis.process.execPath, "test", "./test/suite.test.ts", "--timeout", "60000"],
  {
    cwd: work,
    env: { ...globalThis.process.env, MOLNIYA_PROJECT_ROOT: root, MOLNIYA_EVIDENCE_DIR: evidence },
    stdout: "inherit",
    stderr: "inherit",
  },
);
const code = await process.exited;
if (code !== 0) throw new Error(`Instrumented tests failed (${code})`);
const instrumentedResults = await Bun.file(join(evidence, "tests.json")).json();
const outcomes = (report: { cases: { id: string; status: string }[] }) =>
  JSON.stringify(report.cases.map(({ id, status }) => ({ id, status })));
if (outcomes(nativeResults) !== outcomes(instrumentedResults))
  throw new Error("Instrumentation changed test outcomes");
zero.merge(await Bun.file(join(evidence, "coverage-final.json")).json());
const context = createContext({ dir: join(root, "coverage"), coverageMap: zero });
create("html").execute(context);
create("lcovonly").execute(context);
create("text-summary").execute(context);
const summary = zero.getCoverageSummary().toJSON();
const files = zero.files().map((file) => ({
  file: file.replace(root + "/", ""),
  ...zero.fileCoverageFor(file).toSummary().toJSON(),
}));
await Bun.write(
  join(evidence, "coverage.json"),
  JSON.stringify(
    { tool: "istanbul-lib-instrument 6.0.3", runtime: `Bun ${Bun.version}`, gates, summary, files },
    null,
    2,
  ) + "\n",
);
for (const [metric, minimum] of Object.entries(gates)) {
  const value = summary[metric as keyof typeof gates].pct;
  if (typeof value !== "number" || value < minimum)
    throw new Error(`${metric}: ${value}% below ${minimum}% gate`);
}
