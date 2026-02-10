import * as fs from "node:fs/promises";
import * as crypto from "node:crypto";
import * as path from "node:path";

/**
 * Manages MBF offload file lifecycle and cleanup.
 * Tracks temporary files and ensures cleanup on process exit.
 */
export class OffloadManager {
	private static tempFiles = new Set<string>();
	private static cleanupRegistered = false;

	/**
	 * Default cache directory for auto-managed offload files
	 */
	static readonly CACHE_DIR = "./cache";

	/**
	 * Register a temporary offload file for cleanup on exit
	 */
	static registerTempFile(filePath: string): void {
		this.tempFiles.add(filePath);
		
		// Register cleanup handler on first call
		if (!this.cleanupRegistered) {
			process.on("exit", () => {
				// Sync cleanup on exit
				for (const file of this.tempFiles) {
					try {
						require("node:fs").unlinkSync(file);
					} catch {}
				}
			});
			
			// Async cleanup on SIGINT/SIGTERM
			const asyncCleanup = async () => {
				await this.cleanup();
				process.exit(0);
			};
			
			process.on("SIGINT", asyncCleanup);
			process.on("SIGTERM", asyncCleanup);
			
			this.cleanupRegistered = true;
		}
	}

	/**
	 * Get set of registered temp files (for testing)
	 */
	static getTempFiles(): Set<string> {
		return new Set(this.tempFiles);
	}

	/**
	 * Clear temp file registry (for testing)
	 */
	static clearTempFiles(): void {
		this.tempFiles.clear();
	}

	/**
	 * Generate a cache key from source file path, mtime, and size
	 */
	static async generateCacheKey(sourcePath: string): Promise<string> {
		const stats = await fs.stat(sourcePath);
		
		// Create hash from path + mtime + size
		const hash = crypto.createHash("sha256");
		hash.update(path.resolve(sourcePath));
		hash.update(stats.mtime.toISOString());
		hash.update(stats.size.toString());
		
		// Return first 16 chars of hex hash
		return hash.digest("hex").slice(0, 16);
	}

	/**
	 * Get full cache path for a given cache key
	 */
	static getCachePath(cacheKey: string): string {
		return path.join(this.CACHE_DIR, `${cacheKey}.mbf`);
	}

	/**
	 * Delete all registered temporary files
	 */
	static async cleanup(): Promise<void> {
		const promises = Array.from(this.tempFiles).map(async (file) => {
			try {
				await fs.unlink(file);
			} catch {
				// Ignore errors (file might already be deleted)
			}
		});
		
		await Promise.all(promises);
		this.tempFiles.clear();
	}

	/**
	 * Ensure cache directory exists
	 */
	static async ensureCacheDir(): Promise<void> {
		await fs.mkdir(this.CACHE_DIR, { recursive: true });
	}
}
