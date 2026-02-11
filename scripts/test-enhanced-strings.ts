/**
 * Test runner and demonstration script for enhanced string handling.
 * 
 * Run this to verify the implementation works correctly and see
 * performance characteristics in action.
 */

// Import the test runner
import { run } from "vitest/node";

async function runTests() {
	console.log("🚀 Running Enhanced String Handling Tests\n");
	
	try {
		// Run the main functionality tests
		console.log("📋 Running functionality tests...");
		await run(["test/enhanced-strings.test.ts"], {
			reporter: "verbose",
			bail: 1 // Stop on first failure
		});
		
		console.log("\n⚡ Running performance benchmarks...");
		await run(["test/enhanced-strings-benchmark.test.ts"], {
			reporter: "verbose",
			bail: 1
		});
		
		console.log("\n✅ All tests passed!");
		
	} catch (error) {
		console.error("❌ Tests failed:", error);
		process.exit(1);
	}
}

// Also export a demonstration function
export async function demonstrateEnhancedStrings() {
	console.log("🔥 Enhanced String Handling Demonstration\n");
	
	// Dynamic import to avoid loading issues
	const { 
		createEnhancedColumnBuffer, 
		DTypeKind, 
		StringViewOps,
		getStringResourceManager 
	} = await import("../src/buffer/enhanced-strings.ts");
	
	console.log("1. 📊 Categorical Data (Dictionary Encoding)");
	const statusColumn = createEnhancedColumnBuffer(DTypeKind.String, 10000, false);
	
	const statuses = ["active", "inactive", "pending", "archived"];
	console.time("Adding 10K categorical values");
	for (let i = 0; i < 10000; i++) {
		statusColumn.append(statuses[i % 4]);
	}
	console.timeEnd("Adding 10K categorical values");
	
	console.log(`   Encoding chosen: ${statusColumn.getStringEncoding()}`);
	console.log(`   Should use dictionary: ${statusColumn.shouldUseDictionary()}`);
	console.log(`   Memory efficient: stores each status only once\n`);
	
	console.log("2. 🔤 High Cardinality Data (StringView Encoding)");
	const urlColumn = createEnhancedColumnBuffer(DTypeKind.String, 10000, false);
	
	console.time("Adding 10K unique URLs");
	for (let i = 0; i < 10000; i++) {
		urlColumn.append(`https://api.example.com/users/${i}?token=${Math.random().toString(36)}`);
	}
	console.timeEnd("Adding 10K unique URLs");
	
	console.log(`   Encoding chosen: ${urlColumn.getStringEncoding()}`);
	console.log(`   Should use dictionary: ${urlColumn.shouldUseDictionary()}`);
	console.log(`   Zero V8 heap pressure, O(1) inserts\n`);
	
	console.log("3. 🤝 Shared Dictionary for Joins");
	const originColumn = createEnhancedColumnBuffer(
		DTypeKind.StringDict, 
		5000, 
		false,
		{ sharedDictionaryId: "cities", resourceTags: ["location"] }
	);
	
	const destColumn = createEnhancedColumnBuffer(
		DTypeKind.StringDict,
		5000,
		false,
		{ sharedDictionaryId: "cities", resourceTags: ["location"] }
	);
	
	const cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"];
	console.time("Adding 5K origin/dest pairs");
	for (let i = 0; i < 5000; i++) {
		originColumn.append(cities[i % 5]);
		destColumn.append(cities[(i + 2) % 5]);
	}
	console.timeEnd("Adding 5K origin/dest pairs");
	
	console.log(`   Both columns share dictionary - joins = integer comparison!`);
	console.log(`   Resource manager stats:`, getStringResourceManager().getStats());
	console.log("");
	
	console.log("4. 🌍 UTF-8 Operations Without Materialization");
	const textColumn = createEnhancedColumnBuffer(DTypeKind.StringView, 1000, false);
	
	const texts = [
		"apple_product_123",
		"application_server_456", 
		"app_mobile_789",
		"banana_split_000",
		"apple_watch_111"
	];
	
	for (let i = 0; i < 1000; i++) {
		textColumn.append(texts[i % 5]);
	}
	
	const internals = textColumn.getInternals();
	if (internals.stringView) {
		console.time("Fast prefix filtering (no string materialization)");
		const appResults = StringViewOps.filterByPrefix(internals.stringView, "app");
		console.timeEnd("Fast prefix filtering (no string materialization)");
		
		console.log(`   Found ${appResults.length} strings starting with "app"`);
		
		console.time("Byte length filtering");
		const shortResults = StringViewOps.filterByByteLength(internals.stringView, 0, 15);
		console.timeEnd("Byte length filtering");
		
		console.log(`   Found ${shortResults.length} strings ≤15 bytes\n`);
	}
	
	console.log("5. 🔄 Adaptive Behavior");
	const adaptiveColumn = createEnhancedColumnBuffer(
		DTypeKind.String,
		2000,
		false,
		{ sampleSize: 200, cardinalityThreshold: 0.15 }
	);
	
	// Start with unique data
	console.log("   Adding unique data (high cardinality)...");
	for (let i = 0; i < 500; i++) {
		adaptiveColumn.append(`unique_${i}`);
	}
	console.log(`   Encoding: ${adaptiveColumn.getStringEncoding()}`);
	console.log(`   Recommendation: ${adaptiveColumn.shouldUseDictionary() ? "Dictionary" : "StringView"}`);
	
	// Add repetitive data
	console.log("   Adding repetitive data...");
	const categories = ["A", "B", "C"];
	for (let i = 0; i < 1500; i++) {
		adaptiveColumn.append(categories[i % 3]);
	}
	
	const finalStats = adaptiveColumn.getStringStats();
	console.log(`   Final cardinality: ${(finalStats?.cardinality * 100).toFixed(1)}%`);
	console.log(`   Final recommendation: ${adaptiveColumn.shouldUseDictionary() ? "Dictionary" : "StringView"}`);
	
	if (adaptiveColumn.shouldUseDictionary()) {
		console.log("   Converting to dictionary encoding...");
		console.time("Dictionary conversion");
		adaptiveColumn.convertToDictionary();
		console.timeEnd("Dictionary conversion");
		console.log(`   New encoding: ${adaptiveColumn.getStringEncoding()}`);
	}
	
	console.log("\n🎉 Demonstration complete!");
	console.log("\n📈 Key Benefits:");
	console.log("   ✅ Automatic encoding selection based on data characteristics");
	console.log("   ✅ User override with explicit DType hints (stringDict/stringView)"); 
	console.log("   ✅ Shared resources for efficient joins");
	console.log("   ✅ UTF-8 operations without string materialization");
	console.log("   ✅ Bounded memory usage with TypedArrays");
	console.log("   ✅ No automatic O(N) switching during operations");
	
	// Cleanup
	getStringResourceManager().clear();
}

// Run if called directly
if (import.meta.main) {
	demonstrateEnhancedStrings().catch(console.error);
}