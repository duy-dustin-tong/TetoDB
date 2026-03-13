# TetoDB User Guide

TetoDB is a lightweight relational database engine built from scratch in C++. It features B+Tree indexing, Write-Ahead Logging (WAL) with ARIES recovery, 2PL concurrency control, and a full SQL query pipeline (Lexer → Parser → Planner → Optimizer → Executor).

---

## Building TetoDB

TetoDB can be compiled on both Windows and Linux/macOS.

**Windows (PowerShell):**
```powershell
cd TetoDB
cmake -S . -B build
cmake --build build --config Release
```

**Linux / macOS (Bash):**
```bash
cd TetoDB
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j4
```

This produces two executables in `build/Release/` (or `build/` on Linux):
- **`teto_main.exe`** — the database server
- **`teto_client.exe`** — the interactive REPL client

---

## Using the REPL Client

### 1. Start the Server

```powershell
cd build/Release
./teto_main.exe mydb
```

The first argument is the **database name**. TetoDB creates a folder called `data_mydb/` and stores all files inside it:

```
data_mydb/
├── mydb.db         # Page data
├── mydb.log        # Write-Ahead Log
├── mydb.freelist   # Free page list
└── mydb.catalog    # Table/index metadata
```

You can also specify a custom port:

```powershell
./teto_main.exe mydb 9000      # Listen on port 9000
./teto_main.exe production     # Uses default port 5432
```

- If no arguments are given, the database name defaults to `mydb` and port defaults to `5432`.
- If the database folder already exists, TetoDB **reopens** the existing database with full ARIES recovery.
- If the folder doesn't exist, TetoDB creates a **fresh** database.

To shut down the server, type `shutdown` into its terminal window.

### 2. Connect with the REPL

In a separate terminal:

```powershell
cd build/Release
./teto_client.exe
```

You'll see:

```
Starting TetoDB Client (TetoWire)...
Connected to TetoDB.

tetodb>
```

### 3. Run Queries

```sql
tetodb> CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL);
CREATE TABLE

tetodb> INSERT INTO users VALUES (1, 'Alice');
INSERT 0 1

tetodb> INSERT INTO users VALUES (2, 'Bob');
INSERT 0 1

tetodb> SELECT * FROM users;
id | name
---+------
1  | Alice
2  | Bob

SELECT 2

tetodb> SELECT COUNT(*) FROM users;
count
-----
2

SELECT 1

tetodb> UPDATE users SET name = 'Alice Z' WHERE id = 1;
UPDATE 1

tetodb> DELETE FROM users WHERE id = 2;
DELETE 1

tetodb> DROP TABLE users;
DROP TABLE

tetodb> exit
```

Multi-line statements are supported — the client accumulates input until it sees a `;`.

---

## Using with SQLAlchemy (Python)

TetoDB ships with a native Python driver and SQLAlchemy dialect. This lets you use it just like SQLite or PostgreSQL from any Python application.

### 1. Install the Driver

```powershell
cd TetoDB/python
pip install -e .
```

This registers the `tetodb` dialect with SQLAlchemy.

### 2. Make Sure the Server is Running

```powershell
cd build/Release
./teto_main.exe mydb
```

### 3. Connect from Python

```python
from sqlalchemy import create_engine, text

engine = create_engine("tetodb://127.0.0.1:5432/mydb")

with engine.connect() as conn:
    conn.execute(text("CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(50), price DECIMAL);"))
    conn.execute(text("INSERT INTO products VALUES (1, 'Widget', 9.99);"))
    conn.execute(text("INSERT INTO products VALUES (2, 'Gadget', 24.50);"))
    conn.commit()

    result = conn.execute(text("SELECT * FROM products;"))
    for row in result:
        print(row)

    conn.execute(text("DROP TABLE products;"))
    conn.commit()
```

### 4. Using the ORM

```python
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker, declarative_base

engine = create_engine("tetodb://127.0.0.1:5432/mydb")
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)

Session = sessionmaker(bind=engine)

# Create table manually (TetoDB doesn't support Base.metadata.create_all yet)
with engine.connect() as conn:
    conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50));"))
    conn.commit()

# Use the ORM
session = Session()
session.execute(text("INSERT INTO users VALUES (1, 'Dustin');"))
session.commit()

result = session.execute(text("SELECT * FROM users WHERE id = 1;"))
row = result.fetchone()
print(f"User: id={row[0]}, name={row[1]}")

session.close()
```

### Connection URL Format

```
tetodb://host:port/database
```

| Part       | Default       | Description                              |
|------------|---------------|------------------------------------------|
| `host`     | `127.0.0.1`   | Server IP address                        |
| `port`     | `5432`        | Server listening port                    |
| `database` | `mydb`        | Database name (matches server CLI arg)   |

---

## Important Notes & Gotchas

### Database Files & Storage
- TetoDB creates a **dedicated folder** named `data_<dbname>/` containing all files for that database.
- **Do not delete these files while the server is running.** You will corrupt your data.
- To start fresh, stop the server and delete the entire `data_<dbname>/` folder.
- To reopen an existing database, just start the server with the same name — ARIES recovery replays the WAL automatically.

### Table Creation
- `CREATE TABLE` must specify column types explicitly. Supported types: `INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT`, `BOOLEAN`, `DECIMAL`/`FLOAT`/`DOUBLE`, `VARCHAR`/`TEXT`, `CHAR`, `TIMESTAMP`/`DATE`.
- `Base.metadata.create_all(engine)` (SQLAlchemy auto-create) is **not supported**. You must create tables with explicit `CREATE TABLE` SQL statements.

### INSERT Syntax
- TetoDB requires **positional VALUES only** — you cannot specify column names in INSERT.
  ```sql
  -- CORRECT
  INSERT INTO users VALUES (1, 'Alice');

  -- NOT SUPPORTED
  INSERT INTO users (id, name) VALUES (1, 'Alice');
  ```

### Query Features (Current)
- Supported core statements include `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, and `DROP INDEX`.
- Aggregates include `COUNT(*)`, `COUNT(expr)`, `SUM`, `MIN`, `MAX`, `AVG`, and `MEDIAN`. (Empty table aggregates properly return 0 or NULL).
- Built-in string matching via `LIKE`/`ILIKE` uses safe iterative backtracking (no stack overflow risk).
- `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`, `JOIN`, `IN`, CTEs (`WITH`), and set operations (`UNION`, `INTERSECT`, `EXCEPT`) are available.
- `EXPLAIN` is available for inspecting plans.

### Transactions
- TetoDB supports `BEGIN`, `COMMIT`, and `ROLLBACK`.
- Without an explicit `BEGIN`, each statement runs in **autocommit mode**.
- If a statement fails inside a transaction, the transaction becomes **poisoned** — all subsequent statements are rejected until you `COMMIT` (which auto-rollbacks) or `ROLLBACK`.
- **Savepoints are fully supported**: `SAVEPOINT <name>`, `RELEASE SAVEPOINT <name>`, and `ROLLBACK TO <name>` work within an active transaction. `ROLLBACK TO` undoes all operations back to the savepoint and clears the poisoned state, allowing the transaction to continue. Locks are NOT released on `ROLLBACK TO` (standard SQL behavior).

### Concurrency
- TetoDB uses **Strict Two-Phase Locking (2PL)** for transaction isolation.
- Multiple clients can connect simultaneously, but heavy write contention on the same rows may cause lock waits or aborts.
- Background checkpointing runs every 5 seconds.

### SQLAlchemy Limitations (READ THIS)
Because TetoDB is a core engine without full PostgreSQL feature bloat, you must configure SQLAlchemy appropriately:

- **Nested Transactions (Savepoints)**: `session.begin_nested()` is supported! TetoDB implements `SAVEPOINT`, `RELEASE SAVEPOINT`, and `ROLLBACK TO SAVEPOINT`. Partial rollbacks undo data mutations but do not release locks.
- **Positional INSERTS Only**: TetoDB requires `INSERT INTO table VALUES (...)` rather than column targeting.
- **Reflection is limited**: `get_table_names()` returns an empty list since TetoDB doesn't have `information_schema`. The `has_table()` method works by probing with `SELECT ... LIMIT 0`.
- **No ALTER TABLE**: Schema modifications after creation are not supported.
- **No auto-increment**: Primary keys must be provided explicitly. TetoDB has no sequences.
- **Parameter style**: The driver uses client-side `?` parameter substitution. Values are safely escaped (including single quotes).
- **Data Types**: `TIMESTAMP` accepts strings formatted as `'YYYY-MM-DD'` or `'YYYY-MM-DD HH:MM:SS'` and uses 64-bit UTC epoch storage internally.

### Web App Compatibility Notes (FastAPI + SQLAlchemy)
- For startup migrations/seeding in demo apps, prefer small idempotent SQL statements and explicit error handling; if a statement fails in a transaction, issue `ROLLBACK` before continuing.
- Avoid assuming `DROP TABLE IF EXISTS` support; emulate it by attempting `DROP TABLE` and ignoring "table not found" errors.
- For portability across current builds, use explicit `CREATE UNIQUE INDEX ...` statements instead of relying only on inline `UNIQUE` column constraints.
- If your app uses optional foreign keys, validate parent existence before insert/update and ensure nullable FK handling matches your server build behavior.
- Browser CORS errors often mask backend `500` errors. Check backend logs first; once the API returns `2xx`, CORS headers are emitted normally by FastAPI middleware.

### Performance Tips
- Create **indexes** on columns you frequently filter by: `CREATE INDEX idx_name ON table (column);`
- Use `UNIQUE` indexes to enforce uniqueness: `CREATE UNIQUE INDEX idx ON table (col);`
- The query optimizer will automatically use B+Tree indexes for equality and range predicates.
- `EXPLAIN` any query to see the execution plan: `EXPLAIN SELECT * FROM users WHERE id > 5;`

### Network Protocol
- TetoDB uses a custom binary protocol called **TetoWire** (not PostgreSQL wire protocol).
- You **cannot** connect with `psql`, `pgAdmin`, or `psycopg2`. Use `teto_client.exe` or the Python `tetodb` driver.
- Default port is **5432**. Pass a different port as the second CLI argument if it conflicts with a local PostgreSQL installation.
