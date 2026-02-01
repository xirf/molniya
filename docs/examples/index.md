# Examples

Real-world examples demonstrating Molniya in practical scenarios.

## Overview

These examples show complete, runnable solutions to common data processing problems. Each example includes:

- Sample data setup
- Step-by-step transformation
- Explanation of key concepts
- Expected output

## Available Examples

### [Sales Analysis](./sales-analysis)

Analyze sales data to find top products, revenue trends, and customer behavior.

**Covers:**
- Reading CSV data
- Filtering and grouping
- Calculating aggregations
- Time-based analysis

### [Log Processing](./log-processing)

Parse and analyze web server logs to find popular endpoints, error rates, and response times.

**Covers:**
- String parsing
- Pattern matching
- High-volume filtering
- Statistical summaries

### [ETL Pipeline](./etl-pipeline)

Extract, transform, and load data between different formats and systems.

**Covers:**
- Multiple data sources
- Data cleaning
- Type conversions
- Output formatting

### [Data Cleaning](./data-cleaning)

Clean messy real-world data: handle nulls, fix types, remove duplicates.

**Covers:**
- Null handling
- Type casting
- Deduplication
- Validation

## Running the Examples

All examples assume you have Molniya installed:

```bash
bun add Molniya
```

And use this import pattern:

```typescript
import { 
  readCsv, fromRecords, col, lit, and, or,
  sum, avg, min, max, count, asc, desc,
  DType, unwrap, createSchema
} from "Molniya";
```

## Sample Data

Many examples use inline CSV strings for reproducibility:

```typescript
const csvData = `
id,name,category,price,quantity
1,Laptop,Electronics,999.99,5
2,Mouse,Electronics,29.99,50
3,Desk,Furniture,299.99,10
`.trim();

const df = fromCsvString(csvData, {
  id: DType.int32,
  name: DType.string,
  category: DType.string,
  price: DType.float64,
  quantity: DType.int32
});
```

## Performance Notes

Examples are written for clarity. For production use with large datasets:

1. **Filter early** - Apply filters before expensive operations
2. **Select only needed columns** - Reduces memory usage
3. **Use streaming** - `readCsv()` streams by default
4. **Limit during development** - Use `.limit(1000)` while testing

## Next Steps

- Start with [Sales Analysis](./sales-analysis) for a complete walkthrough
- Check the [Cookbook](../cookbook/) for specific patterns
- Browse the [API Reference](../api/) for detailed documentation
