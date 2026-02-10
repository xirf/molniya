# ETL Pipeline Example

A complete example of building an Extract, Transform, Load pipeline with Molniya.

## Scenario

You need to:
1. Extract data from multiple CSV sources
2. Transform and clean the data
3. Join related datasets
4. Aggregate and summarize
5. Output results

## Source Data

### Customers

```typescript
const customersData = `
customer_id,name,email,country,signup_date
1,Alice Johnson,alice@example.com,US,2023-01-15
2,Bob Smith,bob@example.com,UK,2023-02-20
3,Charlie Brown,charlie@example.com,US,2023-03-10
4,Diana Prince,diana@example.com,CA,2023-04-05
5,Eve Anderson,eve@example.com,AU,2023-05-12
`.trim();

const customerSchema = {
  customer_id: DType.int32,
  name: DType.string,
  email: DType.string,
  country: DType.string,
  signup_date: DType.string
};
```

### Orders

```typescript
const ordersData = `
order_id,customer_id,product_id,quantity,unit_price,order_date,status
101,1,1001,2,29.99,2024-01-10,completed
102,1,1002,1,149.99,2024-01-15,completed
103,2,1001,3,29.99,2024-01-20,completed
104,3,1003,1,999.99,2024-02-01,pending
105,2,1002,2,149.99,2024-02-05,completed
106,4,1001,1,29.99,2024-02-10,cancelled
107,5,1003,2,999.99,2024-02-15,completed
108,1,1001,5,29.99,2024-02-20,completed
`.trim();

const orderSchema = {
  order_id: DType.int32,
  customer_id: DType.int32,
  product_id: DType.int32,
  quantity: DType.int32,
  unit_price: DType.float64,
  order_date: DType.string,
  status: DType.string
};
```

### Products

```typescript
const productsData = `
product_id,name,category,cost_price
1001,Wireless Mouse,Electronics,15.00
1002,Mechanical Keyboard,Electronics,75.00
1003,Gaming Laptop,Electronics,750.00
`.trim();

const productSchema = {
  product_id: DType.int32,
  name: DType.string,
  category: DType.string,
  cost_price: DType.float64
};
```

## Step 1: Extract

Load data from multiple sources:

```typescript
import { fromCsvString, DType } from "molniya";

// Load all sources
const customers = fromCsvString(customersData, customerSchema);
const orders = fromCsvString(ordersData, orderSchema);
const products = fromCsvString(productsData, productSchema);

// Verify data loaded
console.log("Customers:", await customers.count());
console.log("Orders:", await orders.count());
console.log("Products:", await products.count());
```

## Step 2: Transform

### Clean and Enrich Orders

```typescript
const cleanedOrders = orders
  // Calculate total amount
  .withColumn("total_amount", col("quantity").mul(col("unit_price")))
  
  // Extract year-month for reporting
  .withColumn("year_month", col("order_date").substring(0, 7))
  
  // Standardize status
  .withColumn("status_normalized", upper(col("status")))
  
  // Flag high-value orders
  .withColumn("is_high_value", col("total_amount").gt(500))
  
  // Filter out cancelled orders for revenue calculations
  .filter(col("status").neq("cancelled"));
```

### Clean Customers

```typescript
const cleanedCustomers = customers
  // Standardize country codes
  .withColumn("country_code", upper(col("country")))
  
  // Extract domain from email
  .withColumn("email_domain", 
    col("email").substring(
      col("email").indexOf("@").add(1),
      100
    )
  )
  
  // Flag new customers (signed up in last 6 months)
  .withColumn("is_new_customer", 
    col("signup_date").gte("2023-10-01")
  );
```

## Step 3: Join

Combine datasets:

```typescript
// Join orders with customers
const ordersWithCustomers = await cleanedOrders
  .innerJoin(cleanedCustomers, "customer_id")
  .select(
    "order_id",
    "customer_id",
    "name",
    "country_code",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "order_date",
    "year_month",
    "status_normalized",
    "is_high_value",
    "is_new_customer"
  );

// Join with products
const fullData = await ordersWithCustomers
  .innerJoin(products, "product_id")
  .select(
    "order_id",
    "customer_id",
    "name",
    "country_code",
    "product_id",
    "product_name",  // from products
    "category",      // from products
    "quantity",
    "unit_price",
    "cost_price",
    "total_amount",
    "order_date",
    "year_month",
    "status_normalized",
    "is_high_value",
    "is_new_customer"
  )
  // Calculate profit
  .withColumn("profit", 
    col("total_amount").sub(col("quantity").mul(col("cost_price")))
  );
```

## Step 4: Aggregate

### Sales by Country

```typescript
const salesByCountry = await fullData
  .groupBy("country_code", [
    { name: "total_orders", expr: count() },
    { name: "total_revenue", expr: sum("total_amount") },
    { name: "total_profit", expr: sum("profit") },
    { name: "avg_order_value", expr: avg("total_amount") }
  ])
  .sort(desc("total_revenue"))
  .toArray();

console.log("Sales by Country:");
console.log(salesByCountry);
```

### Sales by Category

```typescript
const salesByCategory = await fullData
  .groupBy("category", [
    { name: "total_orders", expr: count() },
    { name: "total_revenue", expr: sum("total_amount") },
    { name: "total_units_sold", expr: sum("quantity") }
  ])
  .sort(desc("total_revenue"))
  .toArray();

console.log("Sales by Category:");
console.log(salesByCategory);
```

### Monthly Trends

```typescript
const monthlyTrends = await fullData
  .groupBy("year_month", [
    { name: "order_count", expr: count() },
    { name: "revenue", expr: sum("total_amount") },
    { name: "profit", expr: sum("profit") },
    { name: "new_customer_orders", expr: sum(when(col("is_new_customer"), lit(1)).otherwise(lit(0))) }
  ])
  .sort(asc("year_month"))
  .toArray();

console.log("Monthly Trends:");
console.log(monthlyTrends);
```

### Top Customers

```typescript
const topCustomers = await fullData
  .groupBy(["customer_id", "name"], [
    { name: "order_count", expr: count() },
    { name: "total_spent", expr: sum("total_amount") },
    { name: "avg_order_value", expr: avg("total_amount") }
  ])
  .sort(desc("total_spent"))
  .limit(10)
  .toArray();

console.log("Top Customers:");
console.log(topCustomers);
```

## Step 5: Load (Output)

### Export to CSV

```typescript
// Export summary reports
await salesByCountry.toCsv("output/sales_by_country.csv");
await salesByCategory.toCsv("output/sales_by_category.csv");
await monthlyTrends.toCsv("output/monthly_trends.csv");
```

### Export to JSON

```typescript
// Export for API consumption
const jsonData = await topCustomers.toArray();
await Bun.write("output/top_customers.json", JSON.stringify(jsonData, null, 2));
```

### Display Results

```typescript
// Show final summary
await fullData.show();

// Print statistics
console.log("\n=== ETL Pipeline Summary ===");
console.log(`Total Orders Processed: ${await fullData.count()}`);
console.log(`Total Revenue: ${(await fullData.agg(sum("total_amount")))}`);
console.log(`Total Profit: ${(await fullData.agg(sum("profit")))}`);
```

## Complete Pipeline

```typescript
async function runETLPipeline() {
  // Extract
  console.log("Extracting data...");
  const customers = fromCsvString(customersData, customerSchema);
  const orders = fromCsvString(ordersData, orderSchema);
  const products = fromCsvString(productsData, productSchema);
  
  // Transform
  console.log("Transforming data...");
  const cleanedOrders = orders
    .withColumn("total_amount", col("quantity").mul(col("unit_price")))
    .withColumn("year_month", col("order_date").substring(0, 7))
    .filter(col("status").neq("cancelled"));
  
  const cleanedCustomers = customers
    .withColumn("country_code", upper(col("country")));
  
  // Join
  console.log("Joining datasets...");
  const joined = await cleanedOrders
    .innerJoin(cleanedCustomers, "customer_id")
    .innerJoin(products, "product_id")
    .withColumn("profit", col("total_amount").sub(col("quantity").mul(col("cost_price"))));
  
  // Aggregate
  console.log("Aggregating...");
  const reports = await Promise.all([
    joined.groupBy("country_code", [/* ... */]).toArray(),
    joined.groupBy("category", [/* ... */]).toArray(),
    joined.groupBy("year_month", [/* ... */]).toArray()
  ]);
  
  // Load
  console.log("Saving outputs...");
  // ... save reports
  
  return reports;
}

// Run the pipeline
runETLPipeline().catch(console.error);
```

## Error Handling

```typescript
async function safeETL() {
  try {
    // Validate inputs
    if (!(await customers.count())) {
      throw new Error("No customers data found");
    }
    
    // Run pipeline
    const result = await runETLPipeline();
    
    // Validate outputs
    for (const report of result) {
      if (report.length === 0) {
        console.warn("Empty report generated");
      }
    }
    
    return result;
  } catch (error) {
    console.error("ETL Pipeline failed:", error);
    throw error;
  }
}
```

## Key Takeaways

1. **Extract**: Use `fromCsvString()` or `readCsv()` to load data
2. **Transform**: Use `withColumn()` for calculations, `filter()` for cleaning
3. **Join**: Use `innerJoin()` to combine datasets
4. **Aggregate**: Use `groupBy()` with aggregation functions
5. **Load**: Use `toArray()` and Bun's file APIs for output

## Performance Tips

- Filter before joining to reduce data volume
- Use `select()` after joins to keep only needed columns
- Process large datasets in batches
- Use `Promise.all()` for independent aggregations
