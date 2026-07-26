# Dashboard Final Primary Key Fix Report

Date: 2026-07-26
Branch: `feature/dashboard`

## Commit

- `056c93b fix: validate exact dashboard primary keys`

## RED/GREEN Evidence

- RED: `rejects_schema_v1_response_ip_primary_key_with_extra_nullable_column` showed `Store::open` accepted `PRIMARY KEY(query_id, ip, discriminator)` because validation inspected only required columns. A direct query-log sequence regression also initially failed to compile because exact whole-key validation did not exist.
- GREEN: schema validation now collects every column whose PK ordinal is greater than zero, sorts by ordinal, and compares the entire case-insensitive `(ordinal, column)` sequence against the required key for each table. Extra, missing, reversed, or misnumbered PK columns are rejected. Normal schema reopen and all prior malformed-schema tests pass.

## Verification

- `cargo test dashboard::store::tests`: pass, 40 tests.
- `cargo fmt --check`: pass.
- `cargo clippy --all-targets --all-features -- -D warnings`: pass.
- `cargo test --all-targets --all-features`: pass, 147 library + 16 dashboard integration + 5 forwarding tests, 0 failures.

## Residual Concerns

- None identified within the requested PK-validation scope.
