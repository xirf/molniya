# Contributing to Molniya

Thank you for your interest in contributing to Molniya! We welcome contributions from the community to help make this the fastest JavaScript dataframe library.

## Getting Started

1. **Fork the repository** on GitHub.
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/your-username/molniya.git
   cd molniya
   ```
3. **Install dependencies** using [Bun](https://bun.sh):
   ```bash
   bun install
   ```

## Development Workflow

### Running Tests

We use Bun's built-in test runner.

```bash
bun test
```

### Running Benchmarks

Performance is critical for Molniya. If you make performance-sensitive changes, please run the benchmarks:

```bash
# Setup datasets (requires Kaggle API for real data)
bun run benchmarks/setup.ts

# Run benchmarks
bun run benchmarks/csv-load.bench.ts
bun run benchmarks/operations.bench.ts
```

### Code Style

We use Biome for linting and formatting.

```bash
# Check for issues
bun run lint

# Fix issues
bun run lint:fix

# Format code
bun run format
```

## Pull Request Process

1. Create a new branch for your feature or fix: `git checkout -b feature/amazing-feature`.
2. Commit your changes with clear, descriptive messages.
3. Push to your fork and submit a Pull Request to the `main` branch.
4. Ensure all tests and benchmarks pass.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
