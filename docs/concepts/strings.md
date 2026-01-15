# String Manipulation

Mornye provides a powerful `.str` accessor on string Series, offering efficient vectorized string operations similar to Pandas.

## Basic Operations

Access string methods via the `.str` property of a Series.

```typescript
const s = Series.string(['  Hello  ', 'World', 'Bun']);

// Trimming
s.str.trim();      // ['Hello', 'World', 'Bun']
s.str.lower();     // ['  hello  ', 'world', 'bun']
s.str.upper();     // ['  HELLO  ', 'WORLD', 'BUN']
s.str.len();       // [9, 5, 3]
```

## Pattern Matching

```typescript
const s = Series.string(['Apple', 'Banana', 'Cherry']);

s.str.contains('pp');    // [true, false, false]
s.str.startsWith('B');   // [false, true, false]
s.str.endsWith('y');     // [false, false, true]
```

## Modification

```typescript
const s = Series.string(['2023-01', '2023-02']);

// Replace substrings
s.str.replace('-', '/'); // ['2023/01', '2023/02']

// Padding
s.str.padEnd(10, '.');   // ['2023-01...', '2023-02...']

// Repeating
s.str.repeat(2);         // ['2023-012023-01', ...]
```

## Slicing & Splitting

```typescript
const s = Series.string(['John Doe', 'Jane Smith']);

// Slice characters
s.str.slice(0, 4);       // ['John', 'Jane']

// Split (returns array of strings - handled as general object currently)
// s.str.split(' '); 
```
