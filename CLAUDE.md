# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_graphql is a PostgreSQL extension written in Rust using the pgrx framework. It reflects a GraphQL schema from existing SQL tables and enables GraphQL queries directly within PostgreSQL, without additional servers or processes.

## Build Commands

```bash
# Build and install extension (required after any Rust changes)
cargo pgrx install

# Interactive psql with extension installed
cargo pgrx run pg16  # or pg14, pg15, pg17, pg18
```

## Testing

**IMPORTANT**: pg_regress is the primary test suite for checking regressions. Always run pg_regress tests to verify changes, not just unit tests.

Tests use PostgreSQL's pg_regress framework with SQL files:
- Test SQL files: `test/sql/*.sql`
- Expected output: `test/expected/*.out`
- Actual output (after running): `results/*.out`

```bash
# Preferred: Run all pg_regress tests (installs extension and runs tests)
./run_tests.sh

# Run specific test(s)
./run_tests.sh test_name another_test

# Alternative: Manual steps
cargo pgrx install --pg-config /opt/homebrew/opt/postgresql@17/bin/pg_config --features pg17 --no-default-features
./bin/installcheck_local

# Run unit tests only (not sufficient for regression testing)
cargo pgrx test pg17
```

When writing or editing tests:
1. Create/modify SQL in `test/sql/test_name.sql`
2. Run the test to generate output in `results/test_name.out`
3. Manually verify the output
4. Copy to `test/expected/test_name.out` to make it pass

**Never modify expected output files** (`test/expected/*.out`) unless you have verified that the new output is correct. If a test fails, investigate and fix the code, don't change the expected output.

## Architecture

The extension processes GraphQL queries through this pipeline:

1. **Entry Point** (`src/lib.rs`): The `resolve` function is exposed as `graphql._internal_resolve`. It parses the GraphQL query and orchestrates resolution.

2. **SQL Schema Loading** (`src/sql_types.rs`): Loads PostgreSQL schema metadata (tables, columns, functions, foreign keys, permissions) into Rust structs via SQL queries in `sql/load_sql_context.sql`.

3. **GraphQL Schema Building** (`src/graphql.rs`): Transforms SQL metadata into a GraphQL schema (`__Schema`). Tables become connection types, foreign keys become relationships, and functions can extend types.

4. **Query Resolution** (`src/resolve.rs`): Validates the GraphQL query against the schema, handles fragments, variables, and operation selection.

5. **SQL Transpilation** (`src/transpile.rs`): Converts validated GraphQL operations into SQL queries. Implements `QueryEntrypoint` and `MutationEntrypoint` traits that generate and execute SQL.

Key SQL files loaded as extension SQL:
- `sql/resolve.sql`: Wrapper function exposing `graphql.resolve()`
- `sql/directives.sql`: Schema comment directive parsing (`@graphql({...})`)

## Debugging

Use the pgrx elog macro to print debug output that appears in test `.out` files:

```rust
pgrx_pg_sys::submodules::elog::info!("debug: {:?}", value);
```

## Documentation

```bash
pip install -r docs/requirements_docs.txt
mkdocs serve
# Visit http://127.0.0.1:8000/pg_graphql/
```
