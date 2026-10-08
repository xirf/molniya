import { resolve } from "node:path";
import { fingerprint } from "./provenance.ts";
import requirements from "../docs/requirements.json";

const root = resolve(import.meta.dir, "..");
const report = await Bun.file(resolve(root, "evidence/tests.json")).json();
if (report.sourceDigest !== (await fingerprint(root)))
  throw new Error("Test evidence is stale for the current source tree");
const known = new Set(requirements.map((requirement) => requirement.id));
const seen = new Set<string>();
for (const test of report.cases) {
  if (seen.has(test.id)) throw new Error(`Duplicate test ${test.id}`);
  seen.add(test.id);
  for (const field of [
    "id",
    "objective",
    "preconditions",
    "steps",
    "expected",
    "cleanup",
    "category",
    "source",
  ])
    if (!test[field]) throw new Error(`${test.id} lacks ${field}`);
  if (test.status !== "passed") throw new Error(`${test.id} is ${test.status}`);
  if (!test.requirements.length || test.requirements.some((id: string) => !known.has(id)))
    throw new Error(`Untraced test ${test.id}`);
}
const lines = [
  "# Requirements and last test results",
  "",
  `Runtime: ${report.runtime}. Source SHA-256: \`${report.sourceDigest}\`.`,
  "",
  "| Requirement | Passing test cases |",
  "| --- | --- |",
];
for (const requirement of requirements) {
  const tests = report.cases.filter((test: { requirements: string[] }) =>
    test.requirements.includes(requirement.id),
  );
  if (!tests.length) throw new Error(`Requirement without a test: ${requirement.id}`);
  lines.push(`| ${requirement.id} | ${tests.map((test: { id: string }) => test.id).join(", ")} |`);
}
await Bun.write(resolve(root, "evidence/traceability.md"), lines.join("\n") + "\n");
console.log(`${requirements.length} requirements linked to ${report.cases.length} passing cases.`);
