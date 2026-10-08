import { readdir } from "node:fs/promises";
import { join } from "node:path";

export async function fingerprint(root: string): Promise<string> {
  const hasher = new Bun.CryptoHasher("sha256");
  async function include(path: string): Promise<void> {
    hasher.update(path + "\0");
    hasher.update(await Bun.file(join(root, path)).arrayBuffer());
  }
  for (const directory of ["src", "test", "scripts", "docs", ".github/workflows"]) {
    for (const name of (await readdir(join(root, directory))).sort()) {
      if (/\.(ts|json|yml)$/.test(name)) await include(`${directory}/${name}`);
    }
  }
  for (const name of [
    "package.json",
    "bun.lock",
    "tsconfig.json",
    "bunfig.toml",
    "biome.json",
    "vercel.json",
  ])
    await include(name);
  return hasher.digest("hex");
}
