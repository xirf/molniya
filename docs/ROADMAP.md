# Molniya Roadmap

This document tracks features that are documented but not yet fully implemented in the library.

## Core API Gaps

### DataFrame Creation
- [ ] `fromColumns()` - Create DataFrame from column-oriented data with TypedArrays
- [ ] `fromArrays()` - Create DataFrame from arrays of values
- [ ] `range()` - Create DataFrame with sequence of numbers
- [ ] `DataFrame.empty()` - Static method needs verification

### String Operations (ColumnRef methods)
- [ ] `col().length()` - String length
- [ ] `col().substring(start, len)` - Extract substring
- [ ] `col().upper()` / `col().lower()` - Case conversion
- [ ] `col().trim()` - Remove whitespace
- [ ] `col().replace()` - Replace substring
- [ ] `col().contains()` - Check if contains substring (exists as expr, needs ColumnRef method)
- [ ] `col().startsWith()` / `col().endsWith()` - Prefix/suffix checks

### Date/Time Operations (ColumnRef methods)
- [ ] `col().year()` - Extract year
- [ ] `col().month()` - Extract month
- [ ] `col().day()` - Extract day
- [ ] `col().dayOfWeek()` - Extract day of week
- [ ] `col().quarter()` - Extract quarter
- [ ] `col().hour()` / `col().minute()` / `col().second()` - Extract time components
- [ ] `col().addDays()` / `col().subDays()` - Date arithmetic
- [ ] `col().diffDays()` - Difference between dates
- [ ] `col().truncateDate()` - Truncate to period

### Math Functions
- [ ] `col().round(decimals)` - Round to decimal places
- [ ] `col().floor()` - Floor function
- [ ] `col().ceil()` - Ceiling function
- [ ] `col().abs()` - Absolute value
- [ ] `col().sqrt()` - Square root
- [ ] `col().pow(exp)` - Power/exponentiation

### Aggregation Functions
- [ ] `std()` - Standard deviation
- [ ] `var()` - Variance
- [ ] `median()` - Median value
- [ ] `countDistinct()` - Count unique values

### Sorting Enhancements
- [ ] `asc().nullsLast()` - Sort nulls last option
- [ ] `desc().nullsLast()` - Sort nulls last option

### Join Operations
- [ ] `rightJoin()` - Right outer join
- [ ] `fullJoin()` - Full outer join
- [ ] Multi-column join support (array syntax)

### DataFrame Methods
- [ ] `df.union()` - Union with deduplication
- [ ] `df.unionAll()` - Union without deduplication
- [ ] `df.offset()` / `df.slice()` - Pagination support
- [ ] `df.tail(n)` - Last n rows
- [ ] `df.shuffle()` - Random shuffle
- [ ] `df.sample(fraction)` - Random sample
- [ ] `df.explode()` - Explode array column into rows

### Utility Functions
- [ ] `when().otherwise()` - Complete conditional expression
- [ ] `coalesce()` - First non-null value
- [ ] `between()` - Range check
- [ ] `isIn()` - Check if in array

## I/O Features

### CSV
- [ ] `readCsv()` with `projection` option
- [ ] `readCsv()` with `filter` predicate pushdown

### Parquet
- [ ] `readParquet()` with `projection` option
- [ ] `readParquet()` with `filter` predicate pushdown
- [ ] Complete Parquet type mapping (INT96 timestamps, complex types)

## Type System

### Schema
- [ ] `createSchema()` export verification
- [ ] Schema validation utilities
- [ ] Schema comparison functions

### Type Casting
- [ ] `toDate()` / `toTimestamp()` - String to date parsing
- [ ] `formatDate()` - Date to string formatting
- [ ] `parseJson()` - Parse JSON strings

## Documentation Notes

The following documentation files contain API that may not be fully implemented:

- `guide/strings-dates.md` - Many date/string methods documented
- `guide/expressions.md` - Some expression builders documented
- `guide/aggregations.md` - Statistical aggregations documented
- `api/column-ops.md` - Many column operations documented
- `api/sort-limit.md` - Some sorting options documented
- `api/groupby.md` - Pivot functionality documented
- `api/joins.md` - Right and full joins documented

## Implementation Priority

### High Priority (Core functionality)
1. String operations on ColumnRef (length, substring, upper, lower, trim)
2. Date extraction methods (year, month, day)
3. Math functions (round, floor, ceil, abs)
4. `when().otherwise()` completion

### Medium Priority (Convenience)
1. `fromColumns()` / `fromArrays()` creation methods
2. `df.slice()` / `df.offset()` for pagination
3. `coalesce()` function
4. `between()` and `isIn()` operators

### Low Priority (Advanced)
1. Statistical aggregations (std, var, median)
2. `rightJoin()` / `fullJoin()`
3. `df.shuffle()` / `df.sample()`
4. `df.explode()`
