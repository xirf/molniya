# Log Processing Example

A complete example of parsing and analyzing web server logs using Molniya.

## Scenario

You have web server logs in Common Log Format and want to:
1. Parse log entries into structured data
2. Find the most visited pages
3. Identify error rates by endpoint
4. Analyze response times
5. Detect suspicious traffic patterns

## Sample Data

```typescript
import { fromCsvString, DType, col, count, avg, sum, desc } from "molniya";

// Common Log Format: host ident authuser date request status bytes
const logData = `
host,timestamp,method,path,status,response_time
192.168.1.1,2024-01-15T10:30:00Z,GET,/api/users,200,45
192.168.1.2,2024-01-15T10:30:01Z,GET,/api/products,200,120
192.168.1.3,2024-01-15T10:30:02Z,POST,/api/orders,201,230
192.168.1.1,2024-01-15T10:30:05Z,GET,/api/users/123,200,35
192.168.1.4,2024-01-15T10:30:10Z,GET,/api/products/456,404,15
192.168.1.2,2024-01-15T10:30:12Z,GET,/api/users,200,42
192.168.1.5,2024-01-15T10:30:15Z,POST,/api/login,200,180
192.168.1.1,2024-01-15T10:30:20Z,GET,/api/orders,200,95
192.168.1.6,2024-01-15T10:30:25Z,GET,/admin,403,10
192.168.1.7,2024-01-15T10:30:30Z,GET,/api/products,500,5000
192.168.1.3,2024-01-15T10:30:35Z,PUT,/api/orders/789,200,150
192.168.1.8,2024-01-15T10:30:40Z,GET,/api/users,200,38
192.168.1.2,2024-01-15T10:30:45Z,DELETE,/api/orders/789,204,80
192.168.1.9,2024-01-15T10:30:50Z,GET,/api/products/999,404,12
192.168.1.10,2024-01-15T10:30:55Z,POST,/api/users,201,200
`.trim();

const schema = {
  host: DType.string,
  timestamp: DType.string,
  method: DType.string,
  path: DType.string,
  status: DType.int32,
  response_time: DType.int32
};

const df = fromCsvString(logData, schema);
```

## Step 1: Parse and Enrich

Extract additional information from the logs:

```typescript
// Extract endpoint category and detect API vs static requests
const enriched = df
  .withColumn("endpoint_category",
    when(col("path").startsWith("/api/users"), lit("users"))
      .when(col("path").startsWith("/api/products"), lit("products"))
      .when(col("path").startsWith("/api/orders"), lit("orders"))
      .when(col("path").startsWith("/api/"), lit("other_api"))
      .otherwise(lit("static"))
  )
  .withColumn("is_error", col("status").gte(400))
  .withColumn("is_slow", col("response_time").gt(1000));
```

## Step 2: Top Visited Pages

Find the most frequently accessed endpoints:

```typescript
const topPages = await enriched
  .groupBy("path", [
    { name: "request_count", expr: count() },
    { name: "avg_response_time", expr: avg("response_time") },
    { name: "total_response_time", expr: sum("response_time") }
  ])
  .sort(desc("request_count"))
  .limit(10)
  .toArray();

console.log("Top Pages:");
console.log(topPages);
```

## Step 3: Error Rate Analysis

Analyze errors by endpoint category:

```typescript
const errorAnalysis = await enriched
  .groupBy("endpoint_category", [
    { name: "total_requests", expr: count() },
    { name: "error_count", expr: sum(when(col("is_error"), lit(1)).otherwise(lit(0))) },
    { name: "error_rate", expr: avg(when(col("is_error"), lit(1)).otherwise(lit(0))) }
  ])
  .sort(desc("error_rate"))
  .toArray();

console.log("Error Analysis:");
console.log(errorAnalysis);
```

## Step 4: Response Time Statistics

Get response time percentiles and identify slow endpoints:

```typescript
const responseTimeStats = await enriched
  .groupBy("endpoint_category", [
    { name: "count", expr: count() },
    { name: "avg_time", expr: avg("response_time") },
    { name: "min_time", expr: min("response_time") },
    { name: "max_time", expr: max("response_time") }
  ])
  .toArray();

console.log("Response Time Stats:");
console.log(responseTimeStats);

// Find slow requests
const slowRequests = await enriched
  .filter(col("response_time").gt(1000))
  .select("timestamp", "path", "status", "response_time")
  .sort(desc("response_time"))
  .toArray();

console.log("Slow Requests (>1000ms):");
console.log(slowRequests);
```

## Step 5: Traffic Pattern Analysis

Analyze traffic by time periods:

```typescript
// Extract hour from timestamp (simplified - in real use, parse timestamp properly)
const hourlyTraffic = await enriched
  .withColumn("hour", 
    col("timestamp").substring(11, 2)  // Extract "10" from "2024-01-15T10:30:00Z"
  )
  .groupBy("hour", [
    { name: "request_count", expr: count() },
    { name: "error_count", expr: sum(when(col("is_error"), lit(1)).otherwise(lit(0))) },
    { name: "avg_response_time", expr: avg("response_time") }
  ])
  .sort(asc("hour"))
  .toArray();

console.log("Hourly Traffic:");
console.log(hourlyTraffic);
```

## Step 6: Detect Suspicious Traffic

Identify potential security issues:

```typescript
// Find IPs with high error rates (potential attackers)
const suspiciousIPs = await enriched
  .groupBy("host", [
    { name: "total_requests", expr: count() },
    { name: "error_count", expr: sum(when(col("is_error"), lit(1)).otherwise(lit(0))) },
    { name: "error_rate", expr: avg(when(col("is_error"), lit(1)).otherwise(lit(0))) }
  ])
  .filter(col("total_requests").gte(3))  // At least 3 requests
  .filter(col("error_rate").gte(0.5))     // 50%+ error rate
  .sort(desc("error_rate"))
  .toArray();

console.log("Suspicious IPs (high error rate):");
console.log(suspiciousIPs);

// Find access to admin areas
const adminAccess = await enriched
  .filter(col("path").contains("/admin"))
  .select("timestamp", "host", "path", "status")
  .toArray();

console.log("Admin Access Attempts:");
console.log(adminAccess);
```

## Step 7: Method Analysis

Analyze HTTP methods usage:

```typescript
const methodAnalysis = await enriched
  .groupBy("method", [
    { name: "request_count", expr: count() },
    { name: "avg_response_time", expr: avg("response_time") },
    { name: "error_count", expr: sum(when(col("is_error"), lit(1)).otherwise(lit(0))) }
  ])
  .sort(desc("request_count"))
  .toArray();

console.log("HTTP Method Analysis:");
console.log(methodAnalysis);
```

## Complete Pipeline

```typescript
async function analyzeLogs(logFile: string) {
  const df = await readCsv(logFile, schema);
  
  // Enrich data
  const enriched = df
    .withColumn("endpoint_category", /* ... */)
    .withColumn("is_error", col("status").gte(400))
    .withColumn("is_slow", col("response_time").gt(1000));
  
  // Run all analyses
  const [
    topPages,
    errorAnalysis,
    responseTimeStats,
    suspiciousIPs
  ] = await Promise.all([
    enriched.groupBy("path", [/* ... */]).sort(desc("request_count")).limit(10).toArray(),
    enriched.groupBy("endpoint_category", [/* ... */]).toArray(),
    enriched.groupBy("endpoint_category", [/* ... */]).toArray(),
    enriched.groupBy("host", [/* ... */]).filter(col("error_rate").gte(0.5)).toArray()
  ]);
  
  return {
    topPages,
    errorAnalysis,
    responseTimeStats,
    suspiciousIPs
  };
}
```

## Key Takeaways

1. **Parsing**: Use `fromCsvString()` or `readCsv()` to load log data
2. **Enrichment**: Use `withColumn()` to extract and compute new fields
3. **Aggregation**: Use `groupBy()` with aggregation functions for statistics
4. **Filtering**: Use `filter()` to isolate specific patterns
5. **Sorting**: Use `sort()` with `desc()` or `asc()` for rankings

## Performance Tips

- Filter early to reduce data volume
- Use `select()` to keep only needed columns
- For large log files, process in chunks using `toChunks()`
- Consider aggregating by time windows for long time ranges
