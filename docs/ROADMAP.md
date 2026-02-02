# Molniya Roadmap

This document tracks features that are documented but not yet fully implemented in the library.

## Core API Gaps

### DataFrame Creation
- [x] `fromColumns()` - Create DataFrame from column-oriented data with TypedArrays ✅
- [x] `fromArrays()` - Create DataFrame from arrays of values ✅
- [x] `range()` - Create DataFrame with sequence of numbers ✅
- [ ] `DataFrame.empty()` - Static method needs verification

### String Operations (ColumnRef methods)
- [x] `col().length()` - String length ✅
- [x] `col().substring(start, len)` - Extract substring ✅
- [x] `col().upper()` / `col().lower()` - Case conversion ✅
- [x] `col().trim()` - Remove whitespace ✅
- [x] `col().replace()` - Replace substring ✅
- [x] `col().contains()` - Check if contains substring ✅
- [x] `col().startsWith()` / `col().endsWith()` - Prefix/suffix checks ✅

### Date/Time Operations (ColumnRef methods)
- [x] `col().year()` - Extract year ✅
- [x] `col().month()` - Extract month ✅
- [x] `col().day()` - Extract day ✅
- [x] `col().dayOfWeek()` - Extract day of week ✅
- [x] `col().quarter()` - Extract quarter ✅
- [x] `col().hour()` / `col().minute()` / `col().second()` - Extract time components ✅
- [ ] `col().addDays()` / `col().subDays()` - Date arithmetic
- [ ] `col().diffDays()` - Difference between dates
- [ ] `col().truncateDate()` - Truncate to period

### Math Functions
- [x] `col().round(decimals)` - Round to decimal places ✅
- [x] `col().floor()` - Floor function ✅
- [x] `col().ceil()` - Ceiling function ✅
- [x] `col().abs()` - Absolute value ✅
- [x] `col().sqrt()` - Square root ✅
- [x] `col().pow(exp)` - Power/exponentiation ✅

### Aggregation Functions
- [x] `std()` - Standard deviation ✅
- [x] `var()` - Variance ✅
- [x] `median()` - Median value ✅
- [x] `countDistinct()` - Count unique values ✅

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
- [x] `df.offset()` / `df.slice()` - Pagination support ✅
- [x] `df.tail(n)` - Last n rows ✅
- [ ] `df.shuffle()` - Random shuffle
- [ ] `df.sample(fraction)` - Random sample
- [ ] `df.explode()` - Explode array column into rows

### Utility Functions
- [x] `when().otherwise()` - Complete conditional expression ✅
- [x] `coalesce()` - First non-null value ✅
- [x] `between()` - Range check ✅
- [x] `isIn()` - Check if in array ✅

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
