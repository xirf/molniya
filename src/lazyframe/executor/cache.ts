import { createHash } from 'node:crypto';
import * as fs from 'node:fs';
import * as path from 'node:path';
import type { ScanPredicate } from '../predicate-pushdown';

export const DEFAULT_CACHE_DIR = path.join(process.cwd(), '.molniya_cache');
export const DEFAULT_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000;
export const DEFAULT_CACHE_MAX_SIZE_BYTES = 10 * 1024 * 1024 * 1024;

function ensureCacheDir(cacheDir: string): void {
  if (!fs.existsSync(cacheDir)) {
    fs.mkdirSync(cacheDir, { recursive: true });
  }
}

function sanitizeBaseName(csvPath: string): string {
  return path.basename(csvPath).replace(/[^a-zA-Z0-9._-]/g, '_');
}

function extractCacheMtime(fileName: string): number | null {
  const parts = fileName.split('.');
  if (parts.length < 3) return null;
  if (parts[parts.length - 1] !== 'mbin') return null;

  for (let i = parts.length - 2; i >= 0; i--) {
    const value = Number(parts[i]);
    if (Number.isFinite(value)) {
      return value;
    }
  }

  return null;
}

export function getCacheDir(): string {
  ensureCacheDir(DEFAULT_CACHE_DIR);
  return DEFAULT_CACHE_DIR;
}

export function getCachePath(csvPath: string, cacheKey?: string): string | null {
  try {
    const stat = fs.statSync(csvPath);
    const cacheDir = getCacheDir();
    const baseName = sanitizeBaseName(csvPath);
    const mtime = Math.floor(stat.mtimeMs);
    const suffix = cacheKey ? `.${cacheKey}` : '';
    return path.join(cacheDir, `${baseName}.${mtime}${suffix}.mbin`);
  } catch {
    return null;
  }
}

export function createCacheKey(payload: unknown): string {
  const json = JSON.stringify(payload, (_key, value) =>
    typeof value === 'bigint' ? `bigint:${value.toString()}` : value,
  );
  return createHash('sha1')
    .update(json ?? '')
    .digest('hex')
    .slice(0, 16);
}

export function buildSchemaSignature(
  schema: Record<string, string>,
  columns?: Set<string>,
): Array<[string, string]> {
  const names = columns ? Array.from(columns) : Object.keys(schema);
  names.sort();
  return names.map((name) => [name, schema[name] ?? '']);
}

export function normalizeNullValues(values?: string[]): string[] {
  return values ?? ['NA', 'null', '-', ''];
}

export function normalizePredicates(predicates: ScanPredicate[]): Array<{
  columnName: string;
  operator: string;
  value: string;
}> {
  return predicates
    .map((pred) => ({
      columnName: pred.columnName,
      operator: pred.operator,
      value:
        typeof pred.value === 'bigint'
          ? `bigint:${pred.value.toString()}`
          : JSON.stringify(pred.value),
    }))
    .sort((a, b) =>
      a.columnName === b.columnName
        ? a.operator === b.operator
          ? a.value.localeCompare(b.value)
          : a.operator.localeCompare(b.operator)
        : a.columnName.localeCompare(b.columnName),
    );
}

export function cleanupStaleCacheFiles(csvPath: string, cacheDir = getCacheDir()): void {
  try {
    ensureCacheDir(cacheDir);
    const baseName = sanitizeBaseName(csvPath);
    const csvExists = fs.existsSync(csvPath);
    const csvMtime = csvExists ? Math.floor(fs.statSync(csvPath).mtimeMs) : null;

    for (const file of fs.readdirSync(cacheDir)) {
      if (!file.startsWith(`${baseName}.`) || !file.endsWith('.mbin')) continue;
      if (!csvExists) {
        fs.unlinkSync(path.join(cacheDir, file));
        continue;
      }

      const cacheMtime = extractCacheMtime(file);
      if (!cacheMtime || cacheMtime !== csvMtime) {
        fs.unlinkSync(path.join(cacheDir, file));
      }
    }
  } catch {
    // ignore cleanup failures
  }
}

export function applyCacheRetention(options?: {
  cacheDir?: string;
  maxAgeMs?: number;
  maxSizeBytes?: number;
}): void {
  const cacheDir = options?.cacheDir ?? getCacheDir();
  const maxAgeMs = options?.maxAgeMs ?? DEFAULT_CACHE_MAX_AGE_MS;
  const maxSizeBytes = options?.maxSizeBytes ?? DEFAULT_CACHE_MAX_SIZE_BYTES;

  try {
    ensureCacheDir(cacheDir);
    const now = Date.now();
    const entries = fs
      .readdirSync(cacheDir)
      .filter((file) => file.endsWith('.mbin'))
      .map((file) => {
        const fullPath = path.join(cacheDir, file);
        const stat = fs.statSync(fullPath);
        return { file, fullPath, mtimeMs: stat.mtimeMs, size: stat.size };
      });

    for (const entry of entries) {
      if (now - entry.mtimeMs > maxAgeMs) {
        fs.unlinkSync(entry.fullPath);
      }
    }

    const remaining = entries
      .filter((entry) => fs.existsSync(entry.fullPath))
      .sort((a, b) => a.mtimeMs - b.mtimeMs);

    let totalSize = remaining.reduce((sum, entry) => sum + entry.size, 0);
    for (const entry of remaining) {
      if (totalSize <= maxSizeBytes) break;
      try {
        fs.unlinkSync(entry.fullPath);
        totalSize -= entry.size;
      } catch {
        // ignore
      }
    }
  } catch {
    // ignore cleanup failures
  }
}
