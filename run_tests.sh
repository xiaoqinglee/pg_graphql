#!/bin/bash
# Run pg_regress tests for pg_graphql
# Usage: ./run_tests.sh [test_name ...]

set -e

export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"

# Install the extension
cargo pgrx install --pg-config /opt/homebrew/opt/postgresql@17/bin/pg_config --features pg17 --no-default-features

# Run tests
./bin/installcheck_local "$@"
