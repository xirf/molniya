# String Operations

Recipes for manipulating and analyzing string data in Molniya.

## Basic String Operations

### Concatenation

```typescript
import { col, lit } from "Molniya";

// Combine first and last name
df.withColumn("full_name", 
  col("first_name").add(" ").add(col("last_name"))
)

// Add prefix/suffix
df.withColumn("email_domain", lit("@").add(col("domain")))
  .withColumn("greeting", lit("Hello, ").add(col("name")))

// Concatenate multiple columns
df.withColumn("address",
  col("street").add(", ").add(col("city")).add(", ").add(col("zip"))
)
```

### String Length

```typescript
import { col, length } from "Molniya";

// Get string length
df.withColumn("name_length", length(col("name")))

// Filter by length
df.filter(length(col("password")).gte(8))

// Find long names
df.filter(length(col("description")).gt(100))
```

### Case Conversion

```typescript
import { col, upper, lower } from "Molniya";

// Convert to uppercase
df.withColumn("code_upper", upper(col("code")))

// Convert to lowercase
df.withColumn("email_lower", lower(col("email")))

// Normalize for comparison
df.filter(lower(col("status")).eq("active"))
```

## Substring Operations

### Extract Substring

```typescript
import { col, substring } from "Molniya";

// Extract first 3 characters (area code)
df.withColumn("area_code", substring(col("phone"), 0, 3))

// Extract year from date string
df.withColumn("year", substring(col("date_str"), 0, 4))

// Extract domain from email
df.withColumn("domain", substring(col("email"), col("email").indexOf("@").add(1), 100))
```

### Left and Right

```typescript
import { col, left, right } from "Molniya";

// First N characters
df.withColumn("prefix", left(col("code"), 3))

// Last N characters
df.withColumn("suffix", right(col("code"), 4))

// File extension
df.withColumn("extension", right(col("filename"), 4))
```

## Searching Strings

### Contains

```typescript
import { col, contains } from "Molniya";

// Check if string contains substring
df.filter(contains(col("email"), "@company.com"))
  .filter(contains(col("description"), "urgent"))
  .filter(contains(col("title"), "sale"))
```

### Starts With / Ends With

```typescript
import { col, startsWith, endsWith } from "Molniya";

// Check prefix
df.filter(startsWith(col("phone"), "+1"))
  .filter(startsWith(col("code"), "US-"))

// Check suffix
df.filter(endsWith(col("email"), ".com"))
  .filter(endsWith(col("file"), ".csv"))
```

### Position

```typescript
import { col, indexOf } from "Molniya";

// Find position of substring
df.withColumn("at_position", indexOf(col("email"), "@"))
  .filter(indexOf(col("email"), "@").gt(0))  // Valid email check
```

## Pattern Matching

### Simple Patterns

```typescript
import { col, like } from "Molniya";

// SQL-style LIKE patterns
df.filter(like(col("name"), "John%"))      // Starts with John
df.filter(like(col("name"), "%Smith"))     // Ends with Smith
df.filter(like(col("code"), "US-___"))     // US- followed by 3 chars
df.filter(like(col("email"), "%@%.com"))   // Contains @ and ends with .com
```

### Regular Expressions

```typescript
import { col, regexpMatch, regexpExtract, regexpReplace } from "Molniya";

// Match pattern
df.filter(regexpMatch(col("email"), "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))

// Extract group
df.withColumn("domain", regexpExtract(col("email"), "@([a-zA-Z0-9.-]+)", 1))

// Replace pattern
df.withColumn("clean_phone", regexpReplace(col("phone"), "[^0-9]", ""))
```

## String Splitting

### Split by Delimiter

```typescript
import { col, split } from "Molniya";

// Split by comma
df.withColumn("tags_array", split(col("tags"), ","))

// Split email to get username
df.withColumn("username", split(col("email"), "@").get(0))

// Split path
df.withColumn("parts", split(col("path"), "/"))
```

### Explode Arrays

```typescript
import { col, split, explode } from "Molniya";

// Split and explode to separate rows
df.withColumn("tag", explode(split(col("tags"), ",")))
```

## String Trimming

### Trim Whitespace

```typescript
import { col, trim, ltrim, rtrim } from "Molniya";

// Remove whitespace from both ends
df.withColumn("clean_name", trim(col("name")))

// Remove from left only
df.withColumn("clean_left", ltrim(col("code")))

// Remove from right only
df.withColumn("clean_right", rtrim(col("description")))
```

### Remove Specific Characters

```typescript
import { col, trim } from "Molniya";

// Remove specific characters
df.withColumn("clean_code", trim(col("code"), "-"))
```

## Padding Strings

### Pad Left/Right

```typescript
import { col, lpad, rpad } from "Molniya";

// Pad left with zeros
df.withColumn("padded_id", lpad(col("id"), 8, "0"))
// "123" becomes "00000123"

// Pad right with spaces
df.withColumn("padded_name", rpad(col("name"), 20, " "))
```

## String Replacement

### Replace Substring

```typescript
import { col, replace } from "Molniya";

// Replace all occurrences
df.withColumn("clean_text", replace(col("text"), "old", "new"))

// Remove substring
df.withColumn("no_spaces", replace(col("text"), " ", ""))
```

### Replace First Only

```typescript
import { col, replaceFirst } from "Molniya";

// Replace first occurrence only
df.withColumn("fixed", replaceFirst(col("path"), "/old/", "/new/"))
```

## URL/Path Operations

### Parse URLs

```typescript
import { col, regexpExtract } from "Molniya";

// Extract URL components
df.withColumn("protocol", regexpExtract(col("url"), "^(https?)://", 1))
  .withColumn("domain", regexpExtract(col("url"), "https?://([^/]+)", 1))
  .withColumn("path", regexpExtract(col("url"), "https?://[^/]+(/.*)$", 1))
```

### File Path Operations

```typescript
import { col, regexpExtract, regexpReplace } from "Molniya";

// Extract filename from path
df.withColumn("filename", regexpExtract(col("path"), "([^/]+)$", 1))

// Extract directory
df.withColumn("directory", regexpReplace(col("path"), "/[^/]+$", ""))

// Change extension
df.withColumn("new_path", regexpReplace(col("path"), "\\.[^.]+$", ".txt"))
```

## Common Recipes

### Email Validation

```typescript
import { col, regexpMatch, lower } from "Molniya";

const emailPattern = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$";

const validEmails = df
  .withColumn("email_clean", lower(trim(col("email"))))
  .filter(regexpMatch(col("email_clean"), emailPattern));
```

### Phone Number Normalization

```typescript
import { col, regexpReplace, lpad } from "Molniya";

const normalized = df
  // Remove all non-digit characters
  .withColumn("digits_only", regexpReplace(col("phone"), "[^0-9]", ""))
  // Ensure 10 digits
  .withColumn("normalized", lpad(col("digits_only"), 10, "0"));
```

### Name Formatting

```typescript
import { col, concat, upper, lower, substring, length } from "Molniya";

const formatted = df
  // Capitalize first letter, lowercase rest
  .withColumn("first_formatted",
    concat(
      upper(substring(col("first_name"), 0, 1)),
      lower(substring(col("first_name"), 1, length(col("first_name"))))
    )
  )
  .withColumn("last_formatted",
    concat(
      upper(substring(col("last_name"), 0, 1)),
      lower(substring(col("last_name"), 1, length(col("last_name"))))
    )
  )
  .withColumn("full_name", concat(col("first_formatted"), lit(" "), col("last_formatted")));
```

### Tag Processing

```typescript
import { col, lower, trim, split, explode } from "Molniya";

// Normalize and split tags
const tagAnalysis = df
  .withColumn("tags_clean", lower(trim(col("tags"))))
  .withColumn("tag_array", split(col("tags_clean"), ","))
  .withColumn("tag", explode(col("tag_array")))
  .withColumn("tag_trimmed", trim(col("tag")))
  .filter(length(col("tag_trimmed")).gt(0))
  .groupBy("tag_trimmed", [{ name: "count", expr: count() }]);
```

### Search Highlighting

```typescript
import { col, contains, when, lit } from "Molniya";

// Mark rows containing search term
const searchTerm = "urgent";
const results = df
  .withColumn("matches",
    when(contains(lower(col("title")), searchTerm), lit(true))
      .when(contains(lower(col("description")), searchTerm), lit(true))
      .otherwise(lit(false))
  )
  .filter(col("matches").eq(true));
```

## Best Practices

1. **Normalize before comparing**: Use `lower()` or `upper()` for case-insensitive comparisons
2. **Trim whitespace**: Always `trim()` user input before processing
3. **Use regex carefully**: Complex patterns can be slow; use simple operations when possible
4. **Handle nulls**: String operations on null return null; use `fillNull()` if needed
5. **Consider encoding**: Molniya uses dictionary encoding for strings; repeated values are efficient

## Performance Tips

- Prefer `startsWith()`/`endsWith()` over regex for simple patterns
- Use `contains()` instead of regex for substring search
- Dictionary encoding makes equality comparisons very fast
- Avoid complex regex on very long strings
