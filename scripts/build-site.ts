import { mkdir, rm } from "node:fs/promises";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const output = join(root, "dist");
await rm(output, { recursive: true, force: true });
await mkdir(output, { recursive: true });
await Bun.write(join(output, "index.html"), Bun.file(join(root, "site/index.html")));
console.log(`Built static landing page at ${output}`);
