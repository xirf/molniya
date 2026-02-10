# Data Cleaning Example

A complete example of cleaning messy real-world data using Molniya.

## Scenario

You have a messy dataset with common issues:
- Missing/null values
- Inconsistent formatting
- Duplicate records
- Invalid data types
- Outliers
- Mixed formats

## Sample Messy Data

```typescript
import { fromCsvString, DType, col, when, lit, count, sum } from "molniya";

const messyData = `
id,name,email,age,phone,salary,join_date,department,status
1,John Doe,john@example.com,32,555-1234,75000,2023-01-15,Engineering,active
2,Jane Smith,jane@email.com,28,555.5678,82000,2023-02-20,Sales,active
3,Bob Johnson,,45,(555) 9012,68000,2023-03-10,Engineering,inactive
4,Alice Brown,alice@test.com,,555-3456,,2023-04-05,Marketing,active
5,Charlie Wilson,charlie@company.com,38,5557890,95000,,Sales,pending
6,Diana Prince,diana@example.com,29,555-4321,71000,2023-06-15,Engineering,active
7,Eve Anderson,eve@email.com,33,555.8765,88000,2023-07-20,Marketing,active
8,Frank Miller,,52,(555) 2468,105000,2023-08-01,Engineering,inactive
9,Grace Lee,grace@test.com,27,555-1357,62000,2023-09-10,Sales,active
10,Henry Davis,henry@company.com,,555.9753,,2023-10-15,Marketing,pending
11,John Doe,john@example.com,32,555-1234,75000,2023-01-15,Engineering,active
`.trim();

const schema = {
  id: DType.int32,
  name: DType.string,
  email: DType.nullable.string,
  age: DType.nullable.int32,
  phone: DType.string,
  salary: DType.nullable.float64,
  join_date: DType.nullable.string,
  department: DType.string,
  status: DType.string
};

const df = fromCsvString(messyData, schema);
```

## Step 1: Profile the Data

First, understand what issues exist:

```typescript
// Count total rows
const totalRows = await df.count();
console.log(`Total rows: ${totalRows}`);

// Check for nulls in each column
const nullCounts = await df.agg([
  sum(when(col("email").isNull(), lit(1)).otherwise(lit(0))).as("email_nulls"),
  sum(when(col("age").isNull(), lit(1)).otherwise(lit(0))).as("age_nulls"),
  sum(when(col("salary").isNull(), lit(1)).otherwise(lit(0))).as("salary_nulls"),
  sum(when(col("join_date").isNull(), lit(1)).otherwise(lit(0))).as("join_date_nulls")
]);
console.log("Null counts:", nullCounts);

// Check for duplicates
const uniqueIds = await df.select("id").distinct().count();
console.log(`Unique IDs: ${uniqueIds} / ${totalRows}`);

// View sample of problematic rows
const missingEmail = await df.filter(col("email").isNull()).toArray();
console.log("Rows with missing email:", missingEmail);
```

## Step 2: Remove Duplicates

```typescript
// Remove exact duplicate rows
const deduped = df.distinct();

// Or remove duplicates based on specific columns (e.g., same person)
const dedupedByEmail = await df
  .sort(desc("id"))  // Keep most recent record
  .distinct("email", "name")
  .filter(col("email").isNotNull());  // Keep only those with email

console.log(`After deduplication: ${await dedupedByEmail.count()} rows`);
```

## Step 3: Standardize Formats

### Phone Numbers

```typescript
// Normalize phone number format
const phoneNormalized = dedupedByEmail
  .withColumn("phone_clean",
    col("phone")
      .replace("-", "")
      .replace(".", "")
      .replace("(", "")
      .replace(")", "")
      .replace(" ", "")
  )
  .withColumn("phone_formatted",
    lit("(").add(col("phone_clean").substring(0, 3)).add(") ")
      .add(col("phone_clean").substring(3, 3)).add("-")
      .add(col("phone_clean").substring(6, 4))
  );
```

### Names

```typescript
// Standardize name capitalization
const nameStandardized = phoneNormalized
  .withColumn("name_clean",
    col("name")
      .split(" ")
      .map(part => part.substring(0, 1).upper().add(part.substring(1).lower()))
      .join(" ")
  );
```

### Emails

```typescript
// Normalize emails to lowercase
const emailNormalized = nameStandardized
  .withColumn("email_clean", lower(col("email")));
```

## Step 4: Handle Missing Values

### Fill with Defaults

```typescript
// Fill missing ages with median
const medianAge = await df
  .filter(col("age").isNotNull())
  .agg(avg("age"));

const ageFilled = emailNormalized
  .withColumn("age_filled", col("age").fillNull(Math.round(medianAge)));
```

### Fill with Derived Values

```typescript
// For missing join dates, use earliest date in dataset
const earliestDate = await df
  .filter(col("join_date").isNotNull())
  .agg(min("join_date"));

const datesFilled = ageFilled
  .withColumn("join_date_filled", col("join_date").fillNull(earliestDate));
```

### Fill Salary Based on Department

```typescript
// Calculate median salary by department
const deptSalaries = await df
  .filter(col("salary").isNotNull())
  .groupBy("department", [
    { name: "median_salary", expr: avg("salary") }  // Using avg as proxy for median
  ])
  .toArray();

// Create a mapping (in practice, you might use a join)
const deptSalaryMap = Object.fromEntries(
  deptSalaries.map(d => [d.department, d.median_salary])
);

const salaryFilled = datesFilled
  .withColumn("salary_filled",
    when(col("salary").isNotNull(), col("salary"))
      .when(col("department").eq("Engineering"), lit(deptSalaryMap["Engineering"]))
      .when(col("department").eq("Sales"), lit(deptSalaryMap["Sales"]))
      .when(col("department").eq("Marketing"), lit(deptSalaryMap["Marketing"]))
      .otherwise(lit(70000))
  );
```

## Step 5: Validate and Filter

### Remove Invalid Records

```typescript
// Remove rows with critical missing data
const validRecords = salaryFilled
  .filter(col("email_clean").isNotNull())  // Must have email
  .filter(col("age_filled").gte(18).and(col("age_filled").lte(100)))  // Valid age range
  .filter(col("salary_filled").gt(0));  // Must have positive salary
```

### Flag Suspicious Data

```typescript
// Flag potential outliers
const withFlags = validRecords
  .withColumn("is_high_earner", col("salary_filled").gt(150000))
  .withColumn("is_young", col("age_filled").lt(25))
  .withColumn("is_senior", col("age_filled").gt(60));
```

## Step 6: Standardize Categories

### Status Values

```typescript
// Normalize status values
const statusStandardized = withFlags
  .withColumn("status_clean",
    lower(col("status"))
  )
  .withColumn("is_active", col("status_clean").eq("active"));
```

### Department Names

```typescript
// Standardize department names
const deptStandardized = statusStandardized
  .withColumn("department_clean",
    when(col("department").eq("Eng"), lit("Engineering"))
      .when(col("department").eq("Sales"), lit("Sales"))
      .when(col("department").eq("Marketing"), lit("Marketing"))
      .otherwise(col("department"))
  );
```

## Step 7: Final Clean Dataset

```typescript
// Select and rename final columns
const cleaned = deptStandardized
  .select(
    "id",
    "name_clean",
    "email_clean",
    "age_filled",
    "phone_formatted",
    "salary_filled",
    "join_date_filled",
    "department_clean",
    "status_clean",
    "is_active",
    "is_high_earner"
  )
  .rename({
    name_clean: "name",
    email_clean: "email",
    age_filled: "age",
    phone_formatted: "phone",
    salary_filled: "salary",
    join_date_filled: "join_date",
    department_clean: "department",
    status_clean: "status"
  });

// View cleaned data
await cleaned.show();
```

## Step 8: Quality Report

```typescript
// Generate cleaning report
const report = {
  originalRows: totalRows,
  finalRows: await cleaned.count(),
  duplicatesRemoved: totalRows - (await deduped.count()),
  nullsFilled: {
    email: await df.filter(col("email").isNull()).count(),
    age: await df.filter(col("age").isNull()).count(),
    salary: await df.filter(col("salary").isNull()).count()
  },
  validationStats: {
    activeEmployees: await cleaned.filter(col("is_active").eq(true)).count(),
    highEarners: await cleaned.filter(col("is_high_earner").eq(true)).count(),
    avgSalary: await cleaned.agg(avg("salary")),
    avgAge: await cleaned.agg(avg("age"))
  }
};

console.log("\n=== Data Cleaning Report ===");
console.log(JSON.stringify(report, null, 2));
```

## Complete Cleaning Pipeline

```typescript
async function cleanEmployeeData(inputData: string) {
  // Load
  const raw = fromCsvString(inputData, schema);
  
  // Profile
  const totalRows = await raw.count();
  console.log(`Starting with ${totalRows} rows`);
  
  // Clean
  const cleaned = raw
    // Remove duplicates
    .distinct()
    
    // Standardize formats
    .withColumn("phone_clean", /* ... */)
    .withColumn("email_clean", lower(col("email")))
    
    // Fill missing values
    .withColumn("age_filled", col("age").fillNull(35))
    .withColumn("salary_filled", /* ... */)
    
    // Validate
    .filter(col("email_clean").isNotNull())
    .filter(col("age_filled").gte(18))
    
    // Select final columns
    .select("id", "name", "email_clean", "age_filled", "salary_filled");
  
  return cleaned;
}

// Usage
const cleanData = await cleanEmployeeData(messyData);
await cleanData.show();
```

## Key Takeaways

1. **Profile First**: Always understand your data issues before cleaning
2. **Deduplicate Early**: Remove exact duplicates before other operations
3. **Standardize Formats**: Use string operations for consistent formatting
4. **Handle Nulls Intelligently**: Fill with defaults, derived values, or remove
5. **Validate**: Filter out records that don't meet quality standards
6. **Document**: Track what was changed for auditability

## Common Cleaning Patterns

| Issue | Solution |
|-------|----------|
| Duplicates | `df.distinct()` or `df.distinct("key")` |
| Nulls | `col().fillNull()` or `coalesce()` |
| Case inconsistency | `upper()` or `lower()` |
| Format inconsistency | `replace()`, `substring()`, regex |
| Invalid values | `filter()` with validation logic |
| Outliers | Flag with `when()` or filter with ranges |
