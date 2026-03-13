# TetoDB Limitations And Compatibility Notes

This page lists intentional scope limits and current gaps so users can plan safely.

## SQL Surface Limits

- No `ALTER TABLE`
- No `IF EXISTS` / `IF NOT EXISTS` grammar for DDL
- `INSERT` is positional only: `INSERT INTO table VALUES (...)`
- Column-targeted insert syntax is not supported
- No built-in auto-increment / sequence / serial
- `SELECT` requires `FROM` in parser grammar

## Planner/Optimizer Scope

- Join type keywords (`LEFT`, `RIGHT`, `FULL`) are not part of supported join syntax
- Index-scan rewrite is pattern-limited (single-column equality shape)
- No cost-based optimizer
- Advanced rewrite coverage is intentionally narrow

## SQLAlchemy/Dialect Limits

- Not PostgreSQL wire protocol compatible
- Reflection/introspection is limited due to no `information_schema`
- Dialect currently returns empty results for:
  - `get_table_names`
  - `get_view_names`
  - `get_indexes`
  - `get_foreign_keys`
- `get_pk_constraint` metadata is minimal
- `supports_alter = False`

## Protocol/Client Limits

- Wire protocol is custom TetoWire
- Native `teto_client` currently targets `127.0.0.1:5432`
- No TLS/auth/authz layer in native protocol path

## Operational Caveats

- Unique index key-size cap exists (256-byte key limit in index creation path)
- Failed statements inside explicit transactions can poison txn state until end of txn
- Views are available in-memory but view persistence across restart is limited; recreate views after restart if needed
