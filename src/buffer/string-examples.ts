/**
 * Example demonstrating the enhanced string handling capabilities.
 * 
 * Shows how the new system handles both categorical (low cardinality) 
 * and textual (high cardinality) data efficiently.
 */

import { 
	createEnhancedColumnBuffer, 
	DTypeKind, 
	StringEncoding,
	getSharedDictionary,
	StringViewOps,
	UTF8Utils 
} from "./strings.ts";

// Example 1: Categorical data (low cardinality) - Perfect for dictionary
function demonstrateCategoricalData() {
	console.log("=== Categorical Data Example ===");
	
	const statusColumn = createEnhancedColumnBuffer(
		DTypeKind.String, // Auto-adaptive
		10000,
		false
	);
	
	// Simulate 10K rows with only 4 unique statuses
	const statuses = ["active", "inactive", "pending", "archived"];
	for (let i = 0; i < 10000; i++) {
		statusColumn.append(statuses[i % 4]);
	}
	
	console.log(`Encoding chosen: ${statusColumn.getStringEncoding()}`); // Should be "dictionary"
	console.log(`Cardinality stats:`, statusColumn.getStringStats());
	console.log(`Memory efficient: dictionary stores each status only once`);
}

// Example 2: High cardinality data - Perfect for StringView
function demonstrateHighCardinalityData() {
	console.log("\n=== High Cardinality Data Example ===");
	
	const urlColumn = createEnhancedColumnBuffer(
		DTypeKind.String, // Auto-adaptive
		10000,
		false
	);
	
	// Simulate 10K unique URLs
	for (let i = 0; i < 10000; i++) {
		urlColumn.append(`https://example.com/user/${i}/profile?session=${Math.random()}`);
	}
	
	console.log(`Encoding chosen: ${urlColumn.getStringEncoding()}`); // Should be "view"
	console.log(`Cardinality stats:`, urlColumn.getStringStats());
	console.log(`Memory efficient: no string heap pressure, O(1) inserts`);
}

// Example 3: Shared dictionary for joins
function demonstrateSharedDictionary() {
	console.log("\n=== Shared Dictionary Example ===");
	
	// Create city columns that share the same dictionary
	const originColumn = createEnhancedColumnBuffer(
		DTypeKind.StringDict, // Force dictionary
		1000,
		false,
		{ 
			sharedDictionaryId: "cities",
			resourceTags: ["location", "geographic"]
		}
	);
	
	const destColumn = createEnhancedColumnBuffer(
		DTypeKind.StringDict, // Force dictionary  
		1000,
		false,
		{
			sharedDictionaryId: "cities", // Same dictionary!
			resourceTags: ["location", "geographic"]
		}
	);
	
	// Add some cities
	const cities = ["New York", "Los Angeles", "Chicago", "Houston"];
	for (let i = 0; i < 1000; i++) {
		originColumn.append(cities[i % 4]);
		destColumn.append(cities[(i + 1) % 4]);
	}
	
	console.log(`Both columns share dictionary - joins become integer comparisons!`);
	console.log(`Origin encoding: ${originColumn.getStringEncoding()}`);
	console.log(`Dest encoding: ${destColumn.getStringEncoding()}`);
}

// Example 4: UTF-8 operations without string materialization
function demonstrateUTF8Operations() {
	console.log("\n=== UTF-8 Operations Example ===");
	
	const textColumn = createEnhancedColumnBuffer(
		DTypeKind.StringView, // Force view for UTF-8 ops
		1000,
		false
	);
	
	// Add text with emojis and international characters
	const texts = [
		"Hello World! 👋",
		"Bonjour le monde! 🇫🇷",
		"¡Hola mundo! 🇪🇸",
		"こんにちは世界! 🇯🇵",
		"Привет мир! 🇷🇺"
	];
	
	for (let i = 0; i < 1000; i++) {
		textColumn.append(texts[i % 5]);
	}
	
	const internals = textColumn.getInternals();
	if (internals.stringView) {
		// Fast prefix filtering without string materialization
		const helloIndices = StringViewOps.filterByPrefix(internals.stringView, "Hello");
		console.log(`Found ${helloIndices.length} strings starting with "Hello"`);
		
		// Fast byte length filtering  
		const shortTexts = StringViewOps.filterByByteLength(internals.stringView, 0, 20);
		console.log(`Found ${shortTexts.length} strings with ≤20 bytes`);
		
		// UTF-8 aware character length (slower but accurate)
		const shortChars = StringViewOps.filterByCharLength(internals.stringView, 0, 15);
		console.log(`Found ${shortChars.length} strings with ≤15 characters`);
	}
}

// Example 5: Adaptive behavior and recommendations
function demonstrateAdaptiveBehavior() {
	console.log("\n=== Adaptive Behavior Example ===");
	
	const column = createEnhancedColumnBuffer(
		DTypeKind.String, // Auto-adaptive
		2000,
		false,
		{
			sampleSize: 500,      // Sample first 500 strings
			cardinalityThreshold: 0.2 // 20% threshold
		}
	);
	
	// Start with high cardinality (will use StringView)
	console.log("Adding high cardinality data...");
	for (let i = 0; i < 1000; i++) {
		column.append(`unique_id_${i}`);
	}
	
	console.log(`Initial encoding: ${column.getStringEncoding()}`);
	console.log(`Should use dictionary: ${column.shouldUseDictionary()}`);
	
	// Now add repetitive data (changes the cardinality profile)
	console.log("Adding repetitive data...");
	const categories = ["A", "B", "C"];
	for (let i = 0; i < 1000; i++) {
		column.append(categories[i % 3]);
	}
	
	console.log(`Final encoding: ${column.getStringEncoding()}`);
	console.log(`Should use dictionary: ${column.shouldUseDictionary()}`);
	
	if (column.shouldUseDictionary()) {
		console.log("Converting to dictionary (expensive O(N) operation)...");
		column.convertToDictionary();
		console.log(`New encoding: ${column.getStringEncoding()}`);
	}
}

// Run all examples
function runExamples() {
	demonstrateCategoricalData();
	demonstrateHighCardinalityData();
	demonstrateSharedDictionary();
	demonstrateUTF8Operations();
	demonstrateAdaptiveBehavior();
	
	console.log("\n=== Summary ===");
	console.log("✅ Dictionary encoding: Perfect for categorical data");
	console.log("✅ StringView encoding: Handles high cardinality efficiently"); 
	console.log("✅ Shared resources: Enable fast joins and reduce memory");
	console.log("✅ UTF-8 operations: Work without string materialization");
	console.log("✅ Adaptive sampling: Chooses optimal encoding automatically");
	console.log("✅ User control: Override with explicit DType hints");
}

export { runExamples };