import * as fs from "node:fs/promises";
import * as path from "node:path";
import { BinaryWriter } from "./mbf/writer.ts";
import type { DataFrame } from "../dataframe/core.ts";

/**
 * Write a DataFrame to an MBF file
 */
export async function writeToMbf(
	df: DataFrame,
	filePath: string,
): Promise<void> {
	// Ensure parent directory exists
	const dir = path.dirname(filePath);
	await fs.mkdir(dir, { recursive: true });

	// Create writer
	const writer = new BinaryWriter(filePath, df.schema);
	await writer.open();

	try {
		// Stream all chunks to file
		for await (const chunk of df.stream()) {
			await writer.writeChunk(chunk);
		}
	} finally {
		await writer.close();
	}
}

/**
 * Check if cache file is valid (exists and newer than source)
 */
export async function isCacheValid(
	cachePath: string,
	sourcePath: string,
): Promise<boolean> {
	try {
		const [cacheStats, sourceStats] = await Promise.all([
			fs.stat(cachePath),
			fs.stat(sourcePath),
		]);

		// Cache is valid if it's newer than source
		return cacheStats.mtime >= sourceStats.mtime;
	} catch {
		// If either file doesn't exist or error accessing, cache is invalid
		return false;
	}
}

/**
 * Ensure a directory exists, creating it if necessary
 */
export async function ensureCacheDir(dirPath: string): Promise<void> {
	await fs.mkdir(dirPath, { recursive: true });
}
