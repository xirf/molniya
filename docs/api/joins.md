# Joins API

API reference for combining DataFrames using join operations.

## Overview

Joins combine rows from two DataFrames based on matching values in specified columns.

## Join Types

| Method | Description | Result |
|--------|-------------|--------|
| `innerJoin()` | Only matching rows from both | Intersection |
| `leftJoin()` | All left rows, matching right | Left + matches |
| `rightJoin()` | All right rows, matching left | Right + matches |
| `fullJoin()` | All rows from both | Union |
| `semiJoin()` | Left rows with matches in right | Filter left |
| `antiJoin()` | Left rows without matches in right | Filter left (inverse) |
| `crossJoin()` | All combinations | Cartesian product |

## innerJoin()

Returns only rows where the join key exists in both DataFrames.

```typescript
innerJoin(other: DataFrame, leftOn: string, rightOn: string): DataFrame<T & U>
innerJoin(other: DataFrame, on: string): DataFrame<T & U>
```

**Parameters:**
- `other` - DataFrame to join with
- `leftOn` / `on` - Column name in left DataFrame
- `rightOn` - Column name in right DataFrame (if different from left)

**Returns:** DataFrame with matching rows from both

**Example:**

```typescript
// Same column name
orders.innerJoin(customers, "customer_id");

// Different column names
orders.innerJoin(customers, "customer_id", "id");
```

## leftJoin()

Returns all rows from the left DataFrame, with matching rows from the right. Non-matching right rows are null.

```typescript
leftJoin(other: DataFrame, leftOn: string, rightOn: string): DataFrame<T & Partial<U>>
leftJoin(other: DataFrame, on: string): DataFrame<T & Partial<U>>
```

**Example:**

```typescript
// All orders with customer info (if available)
orders.leftJoin(customers, "customer_id");

// Same column name
employees.leftJoin(departments, "dept_id");
```

## rightJoin()

Returns all rows from the right DataFrame, with matching rows from the left. Non-matching left rows are null.

```typescript
rightJoin(other: DataFrame, leftOn: string, rightOn: string): DataFrame<Partial<T> & U>
rightJoin(other: DataFrame, on: string): DataFrame<Partial<T> & U>
```

**Example:**

```typescript
// All customers with their orders (if any)
orders.rightJoin(customers, "customer_id", "id");
```

## fullJoin()

Returns all rows from both DataFrames. Non-matching rows have nulls for the other DataFrame's columns.

```typescript
fullJoin(other: DataFrame, leftOn: string, rightOn: string): DataFrame<Partial<T> & Partial<U>>
fullJoin(other: DataFrame, on: string): DataFrame<Partial<T> & Partial<U>>
```

**Example:**

```typescript
// All employees and all departments (with matches where they exist)
employees.fullJoin(departments, "dept_id");
```

## semiJoin()

Returns rows from the left DataFrame that have a match in the right DataFrame. Only columns from the left are included.

```typescript
semiJoin(other: DataFrame, leftOn: string, rightOn: string): DataFrame<T>
semiJoin(other: DataFrame, on: string): DataFrame<T>
```

**Example:**

```typescript
// Customers who have placed orders (no order details)
customers.semiJoin(orders, "id", "customer_id");

// Active users (users with login records)
users.semiJoin(logins, "id", "user_id");
```

## antiJoin()

Returns rows from the left DataFrame that do NOT have a match in the right DataFrame.

```typescript
antiJoin(other: DataFrame, leftOn: string, rightOn: string): DataFrame<T>
antiJoin(other: DataFrame, on: string): DataFrame<T>
```

**Example:**

```typescript
// Customers who haven't placed orders
customers.antiJoin(orders, "id", "customer_id");

// Products that have never been sold
products.antiJoin(orderItems, "id", "product_id");
```

## crossJoin()

Returns the Cartesian product of both DataFrames (all combinations).

```typescript
crossJoin(other: DataFrame): DataFrame<T & U>
```

::: warning Memory Usage
Cross joins can produce very large results. Use with caution on large DataFrames.
:::

**Example:**

```typescript
// All combinations of products and stores
products.crossJoin(stores);

// Generate date series for each user
users.crossJoin(dates);
```

## Multi-Column Joins

Join on multiple columns by chaining or using arrays:

```typescript
// Chained single-column joins (less efficient)
df1.innerJoin(df2, "col1")
   .innerJoin(df2, "col2");

// Future: Array syntax for multi-column joins
// df1.innerJoin(df2, ["col1", "col2"], ["col1", "col2"]);
```

## Join with Select

Select specific columns after joining:

```typescript
const result = orders
  .innerJoin(customers, "customer_id")
  .select(
    "order_id",
    "customer_name",
    "order_date",
    "total"
  );
```

## Join with Filter

Filter before or after joining:

```typescript
// Filter before (more efficient)
const recentOrders = orders.filter(col("date").gte("2024-01-01"));
const result = recentOrders.innerJoin(customers, "customer_id");

// Filter after
const result = orders
  .innerJoin(customers, "customer_id")
  .filter(col("customer_status").eq("active"));
```

## Handling Column Name Conflicts

When both DataFrames have columns with the same name (other than join keys):

```typescript
// Rename columns before joining
const renamedCustomers = customers
  .withColumnRenamed("name", "customer_name")
  .withColumnRenamed("created_at", "customer_created");

const result = orders.innerJoin(renamedCustomers, "customer_id");
```

## Null Handling in Joins

Null values in join keys never match:

```typescript
// Rows with null customer_id won't match
orders.innerJoin(customers, "customer_id");

// Filter out nulls before joining if needed
orders
  .filter(col("customer_id").isNotNull())
  .innerJoin(customers, "customer_id");
```

## Performance Considerations

- Joins currently load both DataFrames into memory
- Filter DataFrames before joining when possible
- Use smaller DataFrame as the right side when possible
- Consider the join type - inner joins can be more efficient than outer

## Complete Example

```typescript
import { readCsv, DType, col, sum, desc } from "molniya";

const orders = await readCsv("orders.csv", {
  order_id: DType.int32,
  customer_id: DType.int32,
  product_id: DType.int32,
  amount: DType.float64
});

const customers = await readCsv("customers.csv", {
  customer_id: DType.int32,
  name: DType.string,
  region: DType.string
});

const products = await readCsv("products.csv", {
  product_id: DType.int32,
  name: DType.string,
  category: DType.string
});

// Chain multiple joins
const result = await orders
  .innerJoin(customers, "customer_id")
  .innerJoin(products, "product_id")
  .groupBy("region", [
    { name: "total_sales", expr: sum("amount") }
  ])
  .sort(desc("total_sales"))
  .show();
```

## Common Patterns

### Lookup/Mapping

```typescript
// Map codes to descriptions
const withDescriptions = data
  .leftJoin(codeMapping, "code")
  .select("id", "code", "description");
```

### Existence Check

```typescript
// Check if records exist in another table
const hasOrders = customers
  .semiJoin(orders, "id", "customer_id")
  .withColumn("has_orders", lit(true));
```

### Missing Records

```typescript
// Find orphaned records
const orphaned = orders
  .antiJoin(customers, "customer_id", "id");
```

### Enrichment

```typescript
// Add metadata from reference tables
const enriched = events
  .leftJoin(users, "user_id")
  .leftJoin(categories, "category_id");
```
