# Joining DataFrames

Combine data from multiple sources using join operations.

## Overview

Joins combine rows from two DataFrames based on matching values in specified columns. Molniya supports several join types:

| Join Type | Description | Use Case |
|-----------|-------------|----------|
| `innerJoin` | Only matching rows from both | Find orders with valid customers |
| `leftJoin` | All left rows, matching right | All orders with customer info if available |
| `semiJoin` | Left rows with matches in right | Find customers who have orders |
| `antiJoin` | Left rows without matches | Find customers with no orders |
| `crossJoin` | All combinations | Generate combinations |

::: warning Join Performance
Joins currently load both DataFrames into memory. For large datasets, filter before joining when possible.
:::

## Inner Join

Returns only rows where the join key exists in both DataFrames.

```typescript
import { readCsv, DType, col } from "Molniya";

const orders = await readCsv("orders.csv", {
  orderId: DType.int32,
  customerId: DType.int32,
  amount: DType.float64
});

const customers = await readCsv("customers.csv", {
  id: DType.int32,
  name: DType.string,
  email: DType.string
});

// Join orders with customer info
const withCustomers = await orders.innerJoin(
  customers,
  "customerId",  // Column in orders
  "id"           // Column in customers
);

await withCustomers.select("orderId", "name", "amount").show();
```

### With Different Column Names

```typescript
const result = await orders.innerJoin(
  customers,
  "customerId",  // Left column
  "customer_id"  // Right column (different name)
);
```

### With Suffix for Overlapping Columns

```typescript
const result = await orders.innerJoin(
  customers,
  "customerId",
  "id",
  "_right"  // Suffix for overlapping column names
);
```

## Left Join

Returns all rows from the left DataFrame, with matching rows from the right (null if no match).

```typescript
// All orders, with customer info where available
const withCustomers = await orders.leftJoin(
  customers,
  "customerId",
  "id"
);

// Orders without customers will have null customer columns
```

## Semi Join

Returns rows from the left DataFrame where the join key exists in the right.

```typescript
// Find customers who have placed orders
const customersWithOrders = await customers.semiJoin(
  orders,
  "id"  // customers.id matches orders.customerId
);

// Result only has columns from customers
```

::: tip Semi Join vs Inner Join
Use `semiJoin` when you only need columns from the left DataFrame. It's more efficient than `innerJoin` followed by `select()`.
:::

## Anti Join

Returns rows from the left DataFrame where the join key does NOT exist in the right.

```typescript
// Find customers who haven't placed orders
const inactiveCustomers = await customers.antiJoin(
  orders,
  "id"
);

// Find orders without valid customers
const orphanedOrders = await orders.antiJoin(
  customers,
  "customerId"
);
```

## Cross Join

Returns the Cartesian product of both DataFrames (every combination).

```typescript
const products = await readCsv("products.csv", {
  productId: DType.int32,
  name: DType.string
});

const regions = await readCsv("regions.csv", {
  regionId: DType.int32,
  regionName: DType.string
});

// All product-region combinations
const combinations = await products.crossJoin(regions);
```

::: warning Cross Join Size
A cross join of two DataFrames with 1000 rows each produces 1,000,000 rows. Use with caution!
:::

## Multi-Column Joins

Currently, joins only support single-column keys. For multi-column joins, combine keys first:

```typescript
// Workaround: Create composite key
const ordersWithKey = orders.withColumn(
  "join_key",
  col("customerId").mul(1000000).add(col("regionId"))
);

const customersWithKey = customers.withColumn(
  "join_key",
  col("customerId").mul(1000000).add(col("regionId"))
);

const result = await ordersWithKey.innerJoin(
  customersWithKey,
  "join_key"
);
```

## Practical Examples

### Enriching Data

```typescript
// Add category names to products
const productsWithCategory = await products
  .innerJoin(categories, "categoryId", "id")
  .select("productId", "productName", "categoryName", "price");
```

### Finding Missing Data

```typescript
// Products without categories
const uncategorized = await products.antiJoin(
  categories,
  "categoryId"
);

// Categories without products
const emptyCategories = await categories.antiJoin(
  products,
  "id",
  "categoryId"
);
```

### Many-to-Many Relationships

```typescript
// Tags and products (through junction table)
const productTags = await products
  .innerJoin(productTagJunction, "productId", "productId")
  .innerJoin(tags, "tagId", "id");
```

### Self Join

```typescript
// Find employees and their managers
const employeesWithManagers = await employees
  .innerJoin(
    employees,           // Join with itself
    "managerId",         // Employee's manager
    "employeeId",        // Manager's ID
    "_manager"           // Suffix for manager columns
  )
  .select(
    "name",              // Employee name
    "name_manager"       // Manager name
  );
```

## Performance Tips

1. **Filter before joining** - Reduce data volume on both sides
2. **Use semi/anti joins** - When you only need one side's columns
3. **Avoid cross joins** - Unless you truly need all combinations
4. **Consider alternatives** - Sometimes `withColumn()` with a lookup is faster

```typescript
// Good: Filter first
const recentOrders = orders.filter(col("date").gte("2024-01-01"));
const result = await recentOrders.innerJoin(customers, "customerId");

// Less efficient: Join then filter
const result = await orders
  .innerJoin(customers, "customerId")
  .filter(col("date").gte("2024-01-01"));
```

## Common Patterns

### Lookup Table Pattern

```typescript
// Instead of joining, use withColumn for simple lookups
const statusNames = {
  1: "Pending",
  2: "Processing",
  3: "Shipped",
  4: "Delivered"
};

// This requires collecting and processing in JS
const ordersWithStatus = await orders.toArray().then(rows =>
  rows.map(row => ({
    ...row,
    statusName: statusNames[row.statusId] || "Unknown"
  }))
);
```

### Master-Detail Pattern

```typescript
// Get order headers with their line items
const orderDetails = await orders
  .innerJoin(orderLines, "orderId", "orderId")
  .groupBy("orderId", [
    { name: "total", expr: sum(col("price").mul(col("quantity"))) },
    { name: "itemCount", expr: count() }
  ]);
```
