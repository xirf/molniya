# Installation

Molniya is distributed as an npm package and requires the Bun runtime.

## Prerequisites

- [Bun](https://bun.sh) 1.0 or later
- TypeScript 5.0 or later (for type checking)

## Install with Bun

```bash
bun add Molniya
```

## TypeScript Configuration

Molniya is written in TypeScript and provides full type definitions. Ensure your `tsconfig.json` includes:

```json
{
  "compilerOptions": {
    "module": "ESNext",
    "moduleResolution": "bundler",
    "target": "ES2022",
    "strict": true,
    "esModuleInterop": true
  }
}
```

## Verify Installation

Create a test file to verify everything works:

```typescript
// test.ts
import { DataFrame, DType, createSchema, unwrap } from "Molniya";

const schema = unwrap(createSchema({
  id: DType.int32,
  name: DType.string
}));

const df = DataFrame.empty(schema, null);
console.log("Column names:", df.columnNames);
```

Run it:

```bash
bun test.ts
```

## Development Setup

If you're contributing to Molniya or want to run the test suite:

```bash
# Clone the repository
git clone https://github.com/your-org/Molniya.git
cd Molniya

# Install dependencies
bun install

# Run tests
bun test

# Run linter
bun run lint

# Format code
bun run format
```

## Troubleshooting

### "Cannot find module 'Molniya'"

Make sure you're using Bun to run your code, not Node.js:

```bash
# Correct
bun run myfile.ts

# Incorrect - won't work
node myfile.ts
npx ts-node myfile.ts
```

### Type errors with schema definitions

Ensure your TypeScript configuration uses `"strict": true` for the best type inference experience.

### Memory issues with large files

Molniya streams CSV files by default, but some operations (like sorting) require loading data into memory. If you encounter memory issues:

1. Use `.limit()` before operations that materialize data
2. Process files in chunks manually if needed
3. Consider using `.filter()` early in your pipeline to reduce data volume
