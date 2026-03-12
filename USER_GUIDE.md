# TetoDB User Guide

TetoDB is a lightweight relational database engine built from scratch in C++. It features B+Tree indexing, Write-Ahead Logging (WAL) with ARIES recovery, 2PL concurrency control, and a full SQL query pipeline (Lexer → Parser → Planner → Optimizer → Executor).

---

## Building TetoDB

```powershell
cd TetoDB
cmake -S . -B build
cmake --build build --config Release
```

This produces two executables in `build/Release/`:
- **`teto_main.exe`** — the database server
- **`teto_client.exe`** — the interactive REPL client

---

## Using the REPL Client

### 1. Start the Server

```powershell
cd build/Release
./teto_main.exe
```

The server boots on **port 5432** by default and creates database files (`e2e.db`, `e2e.log`, etc.) in the current working directory.

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
./teto_main.exe
```

### 3. Connect from Python

```python
from sqlalchemy import create_engine, text

engine = create_engine("tetodb://127.0.0.1:5432/e2e")

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
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base

engine = create_engine("tetodb://127.0.0.1:5432/e2e")
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

| Part       | Default       | Description                   |
|------------|---------------|-------------------------------|
| `host`     | `127.0.0.1`   | Server IP address             |
| `port`     | `5432`         | Server listening port         |
| `database` | `e2e`          | Database file name (no `.db`) |

---

## Important Notes & Gotchas

### Database Files
- TetoDB creates several files in the **server's working directory**: `.db` (pages), `.log` (WAL), `.freelist` (free pages), `.catalog` (table metadata).
- **Do not delete these files while the server is running.** You will corrupt your data.
- To start fresh, stop the server and delete all generated files.

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

### Transactions
- TetoDB supports `BEGIN`, `COMMIT`, and `ROLLBACK`.
- Without an explicit `BEGIN`, each statement runs in **autocommit mode**.
- If a statement fails inside a transaction, the transaction becomes **poisoned** — all subsequent statements are rejected until you `COMMIT` (which auto-rollbacks) or `ROLLBACK`.
- `SAVEPOINT`, `RELEASE SAVEPOINT`, and `ROLLBACK TO SAVEPOINT` are parsed and acknowledged but are **no-ops** (no nested transaction support).

### Concurrency
- TetoDB uses **Strict Two-Phase Locking (2PL)** for transaction isolation.
- Multiple clients can connect simultaneously, but heavy write contention on the same rows may cause lock waits or aborts.
- Background checkpointing runs every 5 seconds.

### SQLAlchemy Limitations
- **Reflection is limited**: `get_table_names()` returns an empty list since TetoDB doesn't have `information_schema`. The `has_table()` method works by probing with `SELECT ... LIMIT 0`.
- **No ALTER TABLE**: Schema modifications after creation are not supported.
- **No auto-increment**: Primary keys must be provided explicitly.
- **Parameter style**: The driver uses client-side `?` parameter substitution. Values are escaped, but avoid user-controlled input in raw SQL where possible.

### Performance Tips
- Create **indexes** on columns you frequently filter by: `CREATE INDEX idx_name ON table (column);`
- Use `UNIQUE` indexes to enforce uniqueness: `CREATE UNIQUE INDEX idx ON table (col);`
- The query optimizer will automatically use B+Tree indexes for equality and range predicates.
- `EXPLAIN` any query to see the execution plan: `EXPLAIN SELECT * FROM users WHERE id > 5;`

### Network Protocol
- TetoDB uses a custom binary protocol called **TetoWire** (not PostgreSQL wire protocol).
- You **cannot** connect with `psql`, `pgAdmin`, or `psycopg2`. Use `teto_client.exe` or the Python `tetodb` driver.
- Default port is **5432**. Change it in `server_main.cpp` if it conflicts with a local PostgreSQL installation.
