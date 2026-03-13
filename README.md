<h1 align="center">TetoDB</h1>

<p align="center">
  <i align="center">A lightweight relational database engine in C++ with WAL + ARIES recovery, B+Tree indexes, tuple-level locking, and a full SQL pipeline.</i>
</p>

<h4 align="center">
  <img src="https://img.shields.io/badge/c%2B%2B-17-blue?style=flat-square" alt="c++17" style="height: 20px;">
  <img src="https://img.shields.io/badge/build-cmake-success?style=flat-square" alt="cmake" style="height: 20px;">
  <img src="https://img.shields.io/badge/tests-gtest-informational?style=flat-square" alt="gtest" style="height: 20px;">
  <img src="https://img.shields.io/badge/benchmarks-google%20benchmark-orange?style=flat-square" alt="benchmark" style="height: 20px;">
</h4>

## Introduction

TetoDB is a from-scratch relational database with an end-to-end query path:

- Lexer -> Parser -> AST
- Planner -> Logical Plan
- Rule-based Optimizer -> Physical Plan
- Volcano-style Executor tree
- Storage engine with heap pages + B+Tree indexes
- WAL, checkpointing, and ARIES-style recovery
- Tuple-level locking with wait-die deadlock prevention

This repository also includes:

- Native server and REPL client (`teto_main`, `teto_client`)
- Python DBAPI driver (`python/tetodb/dbapi.py`)
- SQLAlchemy dialect (`python/tetodb/dialect.py`)
- Demo app (`todo-tetodb/`) with FastAPI + SQLAlchemy + React

## Documentation

All operational docs are split under `docs/`:

- `docs/quickstart.md` - build, run, REPL workflow, storage files
- `docs/sql-reference.md` - SQL syntax, operators, functions, examples
- `docs/python-sqlalchemy.md` - safe integration patterns and ORM caveats
- `docs/todo-demo.md` - run the full Todo web app against TetoDB
- `docs/testing-and-benchmarking.md` - test targets, benchmark usage, authoring notes
- `docs/architecture.md` - internal architecture map by subsystem
- `docs/limitations.md` - explicit feature gaps and compatibility limits


## Feature Summary

- SQL: `CREATE TABLE/INDEX/VIEW`, `DROP`, `SELECT`, `INSERT`, `UPDATE`, `DELETE`, CTEs, subqueries, set ops (`UNION/INTERSECT/EXCEPT`), `JOIN ... ON ...`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT/OFFSET`, `DISTINCT`, `EXPLAIN`
- Data types: `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `DECIMAL`, `CHAR`, `VARCHAR`, `TEXT`, `DATE`/`TIMESTAMP`
- Expressions and functions: arithmetic, comparisons, `LIKE`/`ILIKE`, `IN`, `BETWEEN`, `COUNT/SUM/MIN/MAX/AVG/MEDIAN`, `UPPER/LOWER/LENGTH/CONCAT/SUBSTRING`
- Constraints: `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, foreign keys with `CASCADE` / `SET NULL` / `RESTRICT`
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO`, `RELEASE SAVEPOINT`
- Durability and recovery: WAL, log flush thread, checkpoint thread, restart `REDO` + `UNDO`

## Quick Start

### 1) Build

Windows PowerShell:

```powershell
cmake -S . -B build
cmake --build build --config Release
```

Linux/macOS:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j4
```

### 2) Start Server

```powershell
# default: db=mydb, port=5432
./build/Release/teto_main.exe

# explicit db and port
./build/Release/teto_main.exe mydb 5432
./build/Release/teto_main.exe todoapp 9090
```

### 3) Connect REPL Client

```powershell
./build/Release/teto_client.exe
```

### 4) Run SQL

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL);
INSERT INTO users VALUES (1, 'Alice');
SELECT * FROM users;
```

For complete examples and operational notes, continue with `docs/quickstart.md` and `docs/sql-reference.md`.

## Compatibility Notes

- TetoDB uses a custom binary protocol (TetoWire), not PostgreSQL wire protocol
- `psql`, `pgAdmin`, and `psycopg2` are not compatible with the native server protocol
- Use `teto_client` or the provided Python `tetodb` driver
- SQLAlchemy support is practical but intentionally limited (see `docs/python-sqlalchemy.md` and `docs/limitations.md`)

## Repository Layout

- `src/` - engine implementation, headers, server/client entry points
- `test/` - focused regression tests
- `python/` - DBAPI + SQLAlchemy dialect + integration tests
- `todo-tetodb/` - sample full-stack application
- `docs/` - consolidated product and developer documentation

## License

This project is licensed under the Apache License 2.0. See `LICENSE`.
