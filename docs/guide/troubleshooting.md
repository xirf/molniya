# Troubleshooting

Stuck? Here are solutions to common problems.

## TypeScript Errors

### `Property 'x' does not exist on type 'Row'`

**Cause**: TypeScript doesn't know the schema of your DataFrame yet.

**Solution**:
1.  **Check Inference**: Did you load from a CSV? Check if the column exists in `df.columns()`.
2.  **Cast**: If you know it's there:
    ```typescript
    df.filter((row: any) => row.x > 10);
    ```
3.  **Better Solution**: Use generics (coming soon) or define specific schema.

### `Argument of type string[] is not assignable...`

**Cause**: You passed an array instead of arguments.

**Wrong**: `df.select(['a', 'b'])`
**Right**: `df.select('a', 'b')`

## CSV Parsing

### "All my columns are one big string!"

**Cause**: Wrong delimiter.
**Solution**:
```typescript
readCsv('file.csv', { delimiter: ';' }); // Try ; or \t
```

### "My numbers are strings!" (e.g., "1,000.00")

**Cause**: CSV contains formatting chars like commas in numbers.
**Solution**:
Clean it after loading:
```typescript
df.assign('price', row => 
  parseFloat(row.price.replace(',', ''))
);
```

## Runtime Errors

### `FATAL ERROR: Ineffective mark-compacts near heap limit`

**Cause**: You ran out of RAM trying to load a massive file.
**Solution**:
1.  Increase Node/Bun memory: `NODE_OPTIONS="--max-old-space-size=4096" bun run script.ts`
2.  Use `scanCsv` (LazyFrame) instead of `readCsv`.

## DateTime Issues

### "My dates are all invalid"

**Cause**: The format string doesn't match the input.

**Debug**:
1.  Load as string first (default behavior).
2.  Inspect the format: `2023/01/01` is not ISO-8601.
3.  Use explicit format:
    ```typescript
    readCsv('file.csv', {
        datetime: { columns: { 'date': { format: 'sql' } } }
    })
    ```

