# TetoDB Quickstart

This guide gets you from source checkout to running SQL quickly.

## Prerequisites

- CMake >= 3.10
- C++17 compiler
- Git

Optional:

- Python 3.10+ (driver, SQLAlchemy, demo backend)
- Node.js + npm (demo frontend)

## Build

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

Artifacts:

- `teto_main` / `teto_main.exe` - database server
- `teto_client` / `teto_client.exe` - interactive client

## Start Server

```powershell
# default database and port
./build/Release/teto_main.exe

# explicit database
./build/Release/teto_main.exe mydb

# explicit database and port
./build/Release/teto_main.exe mydb 9000
```

Behavior:

- No args -> database `mydb`, port `5432`
- Existing data directory -> opens and runs recovery
- New data directory -> creates fresh database files

To stop server, type `shutdown` in the server terminal.

## Data Directory Layout

Starting `teto_main mydb` creates `data_mydb/` with:

- `mydb.db` - page data
- `mydb.log` - write-ahead log
- `mydb.freelist` - free page list metadata
- `mydb.catalog` - table/index metadata

## Connect REPL Client

```powershell
./build/Release/teto_client.exe
```

The REPL supports multi-line SQL; statements execute when `;` is entered.

## First SQL Session

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL);
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

SELECT * FROM users;
SELECT COUNT(*) FROM users;

UPDATE users SET name = 'Alice Z' WHERE id = 1;
DELETE FROM users WHERE id = 2;
DROP TABLE users;
```

## Operational Notes

- If port `5432` is busy (for example PostgreSQL), start TetoDB on another port
- Do not manually delete `data_<dbname>` files while server is running
- To fully reset a database, stop server first, then remove that directory
