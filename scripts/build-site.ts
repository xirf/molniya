import { mkdir, rm } from "node:fs/promises";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const output = join(root, ".vercel/output");
await rm(output, { recursive: true, force: true });
await mkdir(join(output, "static"), { recursive: true });
await Bun.write(join(output, "static/index.html"), Bun.file(join(root, "site/index.html")));
await Bun.write(join(output, "config.json"), '{"version":3}\n');
console.log(`Built static landing page at ${output}/static`);
