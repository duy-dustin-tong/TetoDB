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

## Building Flawless Apps with SQLAlchemy (Python)

TetoDB ships with a native Python DBAPI driver and SQLAlchemy dialect. However, because TetoDB is a streamlined engine built from scratch—without full PostgreSQL feature bloat or `information_schema`—you must follow specific integration patterns to reliably build applications.

### 1. Install the Driver
Register the `tetodb` dialect with SQLAlchemy by installing the driver package:

```powershell
cd TetoDB/python
pip install -e .
```

### 2. Setup the Engine and Session
Always use SQLAlchemy's `SessionLocal` dependency pattern. **Avoid using `engine.raw_connection()`**, as executing raw `BEGIN;` commands on the underlying DBAPI connection will conflict with SQLAlchemy's connection pooling and cause fatal `Failed to acquire Shared Lock` database errors!

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# The dialect must be tetodb://
DATABASE_URL = "tetodb://127.0.0.1:9090/mydb"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Generator for FastAPI or context managers
def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
```

### 3. Schema Initialization Pattern
TetoDB currently lacks `ALTER TABLE` and `IF NOT EXISTS` syntax, and SQLAlchemy's `Base.metadata.create_all(engine)` is **not supported**. 

To initialize your database perfectly on app startup without crashing if tables already exist, use a `safe_exec` wrapper:

```python
from sqlalchemy import text
from sqlalchemy.orm import Session

def safe_exec(session: Session, sql: str) -> bool:
    """Executes a query and swallows duplicate table/index errors."""
    try:
        session.execute(text(sql))
        session.commit()
        return True
    except Exception:
        # Table or Index already exists
        session.rollback()
        return False

# Call this on app startup
def init_schema(session: Session):
    safe_exec(session, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL);")
    safe_exec(session, "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title VARCHAR(100), FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE);")
    safe_exec(session, "CREATE INDEX idx_posts_user ON posts (user_id);")
```

### 4. The ID Generation Pattern (No Auto-Increment)
TetoDB has no `AUTOINCREMENT` or `SERIAL` columns. You must calculate primary keys explicitly. Crucially, querying `SELECT MAX(id)` on an **empty table** triggers a `ClosedError` from the DBAPI driver because no columns are returned.

**You must always check `COUNT(*)` first:**

```python
def next_id(session: Session, table_name: str) -> int:
    """Safely calculates the next auto-increment ID in TetoDB."""
    try:
        # 1. Check if the table is empty to prevent ClosedError
        count = int(session.execute(text(f"SELECT COUNT(*) FROM {table_name};")).scalar() or 0)
        if count == 0:
            return 1
    except Exception:
        pass
    
    try:
        # 2. Safely grab MAX(id) if rows exist
        max_id = session.execute(text(f"SELECT MAX(id) FROM {table_name};")).scalar()
        return int(max_id or 0) + 1
    except Exception:
        return 1
```

### 5. Transactions and Savepoints
TetoDB supports nested transactions natively, allowing partial rollbacks without aborting the entire process. SQLAlchemy fully understands this via the `session.begin_nested()` context manager.

```python
def seed_categories_and_posts(session: Session):
    # This automatically emits SAVEPOINT / RELEASE SAVEPOINT
    with session.begin_nested():
        session.execute(text("INSERT INTO categories VALUES (1, 'Tech');"))
        session.execute(text("INSERT INTO categories VALUES (2, 'Life');"))
        
    try:
        with session.begin_nested():
            # TetoDB requires POSITIONAL values. Do not target columns.
            session.execute(text("INSERT INTO posts VALUES (1, 1, 'Hello World');"))
    except Exception as e:
        # The inner block cleanly rolls back to its savepoint.
        # The categories from the first block are STILL safely staged!
        pass 
        
    session.commit()
```

### 6. Critical SQL Gotchas

* **Positional INSERT Only**: You cannot specify columns in INSERT. `INSERT INTO users (id, name)` will fail. You must use `INSERT INTO users VALUES (?, ?);`.
* **String Datatypes**: Boolean values are accepted as `TRUE` or `FALSE`. Timestamps are accepted as strings exactly matching `'YYYY-MM-DD HH:MM:SS'`.
* **Database Cleanup**: Do not delete TetoDB files manually while the server runs. To reset the DB entirely, stop the `teto_main.exe` process and delete the `data_<dbname>` folder.
* **Corrupted API States**: If your web API suddenly returns `500` continuously on all endpoints, a buggy request likely threw an unhandled DB exception and failed to call `session.rollback()`, leaving your connection pool holding a locked, uncommitted transaction. Restart your API server to reset the connections!

### Performance Tips
- Create **indexes** on columns you frequently filter by: `CREATE INDEX idx_name ON table (column);`
- Use `UNIQUE` indexes to enforce uniqueness: `CREATE UNIQUE INDEX idx ON table (col);`
- The query optimizer will automatically use B+Tree indexes for equality and range predicates.
- `EXPLAIN` any query to see the execution plan: `EXPLAIN SELECT * FROM users WHERE id > 5;`

### Network Protocol
- TetoDB uses a custom binary protocol called **TetoWire** (not PostgreSQL wire protocol).
- You **cannot** connect with `psql`, `pgAdmin`, or `psycopg2`. Use `teto_client.exe` or the Python `tetodb` driver.
- Default port is **5432**. Pass a different port as the second CLI argument if it conflicts with a local PostgreSQL installation.
