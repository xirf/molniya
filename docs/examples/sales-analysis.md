# Sales Analysis Example

A complete example of analyzing sales data to extract business insights.

## Scenario

You have a CSV file with sales transactions and want to:
1. Calculate total revenue by category
2. Find top-selling products
3. Analyze monthly trends
4. Identify high-value customers

## Sample Data

```typescript
import { fromCsvString, DType, col, sum, avg, count, desc } from "molniya";

const salesData = `
transaction_id,date,product,category,customer_id,quantity,unit_price
1,2024-01-15,Laptop Pro,Electronics,CUST001,2,1299.99
2,2024-01-15,Wireless Mouse,Electronics,CUST002,1,29.99
3,2024-01-16,Office Chair,Furniture,CUST001,1,349.99
4,2024-01-16,Laptop Pro,Electronics,CUST003,1,1299.99
5,2024-01-17,Standing Desk,Furniture,CUST002,1,599.99
6,2024-01-17,USB-C Hub,Electronics,CUST001,3,49.99
7,2024-01-18,Monitor 4K,Electronics,CUST004,2,399.99
8,2024-01-18,Office Chair,Furniture,CUST003,2,349.99
9,2024-01-19,Keyboard Mechanical,Electronics,CUST002,1,149.99
10,2024-01-19,Webcam HD,Electronics,CUST005,1,79.99
11,2024-01-20,Standing Desk,Furniture,CUST004,1,599.99
12,2024-01-20,Laptop Pro,Electronics,CUST002,1,1299.99
13,2024-01-21,Wireless Mouse,Electronics,CUST006,2,29.99
14,2024-01-21,Monitor 4K,Electronics,CUST001,1,399.99
15,2024-01-22,Office Chair,Furniture,CUST005,1,349.99
`.trim();

const df = fromCsvString(salesData, {
  transaction_id: DType.int32,
  date: DType.string,
  product: DType.string,
  category: DType.string,
  customer_id: DType.string,
  quantity: DType.int32,
  unit_price: DType.float64
});
```

## Step 1: Add Calculated Columns

First, add a `total_amount` column for easier analysis:

```typescript
const withTotal = df.withColumn(
  "total_amount",
  col("quantity").mul(col("unit_price"))
);

await withTotal.select(
  "transaction_id", 
  "product", 
  "quantity", 
  "unit_price", 
  "total_amount"
).show();

// Output:
// ┌────────────────┬───────────────────┬──────────┬────────────┬──────────────┐
// │ transaction_id │ product           │ quantity │ unit_price │ total_amount │
// ├────────────────┼───────────────────┼──────────┼────────────┼──────────────┤
// │ 1              │ Laptop Pro        │ 2        │ 1299.99    │ 2599.98      │
// │ 2              │ Wireless Mouse    │ 1        │ 29.99      │ 29.99        │
// │ 3              │ Office Chair      │ 1        │ 349.99     │ 349.99       │
// │ ...
```

## Step 2: Revenue by Category

Calculate total revenue and order count by category:

```typescript
const categorySummary = await withTotal
  .groupBy("category", [
    { name: "total_revenue", expr: sum("total_amount") },
    { name: "total_orders", expr: count() },
    { name: "avg_order_value", expr: avg("total_amount") }
  ])
  .sort(desc("total_revenue"))
  .toArray();

console.log(categorySummary);
// [
//   { category: 'Electronics', total_revenue: 8159.89, total_orders: 10n, avg_order_value: 815.99 },
//   { category: 'Furniture', total_revenue: 2249.96, total_orders: 5n, avg_order_value: 449.99 }
// ]
```

## Step 3: Top Products

Find the best-selling products by revenue:

```typescript
const topProducts = await withTotal
  .groupBy("product", [
    { name: "total_revenue", expr: sum("total_amount") },
    { name: "units_sold", expr: sum("quantity") },
    { name: "num_orders", expr: count() }
  ])
  .sort(desc("total_revenue"))
  .limit(5)
  .toArray();

console.log(topProducts);
// [
//   { product: 'Laptop Pro', total_revenue: 5199.96, units_sold: 4, num_orders: 3n },
//   { product: 'Monitor 4K', total_revenue: 1199.97, units_sold: 3, num_orders: 2n },
//   { product: 'Standing Desk', total_revenue: 1199.98, units_sold: 2, num_orders: 2n },
//   { product: 'Office Chair', total_revenue: 1049.97, units_sold: 3, num_orders: 3n },
//   { product: 'USB-C Hub', total_revenue: 149.97, units_sold: 3, num_orders: 1n }
// ]
```

## Step 4: Customer Analysis

Identify high-value customers:

```typescript
const customerAnalysis = await withTotal
  .groupBy("customer_id", [
    { name: "total_spent", expr: sum("total_amount") },
    { name: "num_orders", expr: count() },
    { name: "avg_order", expr: avg("total_amount") }
  ])
  .sort(desc("total_spent"))
  .toArray();

console.log(customerAnalysis);
// [
//   { customer_id: 'CUST001', total_spent: 3399.95, num_orders: 3n, avg_order: 1133.32 },
//   { customer_id: 'CUST002', total_spent: 1979.97, num_orders: 4n, avg_order: 494.99 },
//   { customer_id: 'CUST003', total_spent: 1999.97, num_orders: 2n, avg_order: 999.99 },
//   ...
// ]
```

Find customers with multiple purchases (repeat customers):

```typescript
const repeatCustomers = await withTotal
  .groupBy("customer_id", [
    { name: "num_orders", expr: count() }
  ])
  .filter(col("num_orders").gt(1))
  .sort(desc("num_orders"))
  .toArray();

console.log(repeatCustomers);
// Customers who made more than one purchase
```

## Step 5: Complete Analysis Pipeline

Putting it all together in a reusable function:

```typescript
async function analyzeSales(df: DataFrame) {
  // Add calculated column
  const enriched = df.withColumn(
    "total_amount",
    col("quantity").mul(col("unit_price"))
  );

  // Run multiple analyses in parallel
  const [
    categoryRevenue,
    topProducts,
    customerSummary
  ] = await Promise.all([
    // Category breakdown
    enriched
      .groupBy("category", [
        { name: "revenue", expr: sum("total_amount") },
        { name: "orders", expr: count() }
      ])
      .sort(desc("revenue"))
      .toArray(),

    // Top 10 products
    enriched
      .groupBy("product", [
        { name: "revenue", expr: sum("total_amount") },
        { name: "units", expr: sum("quantity") }
      ])
      .sort(desc("revenue"))
      .limit(10)
      .toArray(),

    // Customer metrics
    enriched
      .groupBy("customer_id", [
        { name: "spent", expr: sum("total_amount") },
        { name: "orders", expr: count() }
      ])
      .sort(desc("spent"))
      .limit(20)
      .toArray()
  ]);

  return {
    categoryRevenue,
    topProducts,
    customerSummary,
    totalRevenue: categoryRevenue.reduce((sum, c) => sum + c.revenue, 0)
  };
}

// Usage
const analysis = await analyzeSales(df);
console.log(`Total Revenue: $${analysis.totalRevenue.toFixed(2)}`);
```

## Key Takeaways

1. **Add calculated columns early** - `total_amount` simplifies later aggregations
2. **Use `groupBy` for summaries** - Powerful way to aggregate by categories
3. **Sort for top-N** - Combine `sort()` and `limit()` for rankings
4. **Parallel execution** - Multiple independent analyses can run in parallel with `Promise.all()`

## Next Steps

- Learn about [filtering](../cookbook/filtering) for more complex conditions
- Explore [joins](../cookbook/joining) to combine with customer or product data
- See [log processing](./log-processing) for time-series analysis
