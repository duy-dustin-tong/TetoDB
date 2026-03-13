# TetoDB With Python And SQLAlchemy

This guide covers production-safe usage patterns for TetoDB's Python DBAPI driver and SQLAlchemy dialect.

## Install

```powershell
cd python
pip install -e .
```

Use engine URL format:

```python
tetodb://127.0.0.1:5432/mydb
```

## Recommended Session Pattern

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine("tetodb://127.0.0.1:9090/mydb")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
```

Use session-managed transactions and avoid raw unmanaged connection patterns.

## Schema Initialization Pattern

TetoDB does not support `ALTER TABLE`, and SQLAlchemy metadata reflection/introspection is limited.
Prefer explicit startup SQL with idempotent error handling.

```python
from sqlalchemy import text
from sqlalchemy.orm import Session

def safe_exec(session: Session, sql: str) -> bool:
    try:
        session.execute(text(sql))
        session.commit()
        return True
    except Exception:
        session.rollback()
        return False

def init_schema(session: Session):
    safe_exec(session, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL);")
    safe_exec(session, "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title VARCHAR(100), FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE);")
    safe_exec(session, "CREATE INDEX idx_posts_user ON posts (user_id);")
```

## ID Generation Pattern (No Auto Increment)

TetoDB has no serial/auto-increment columns. Generate ids explicitly.

```python
from sqlalchemy import text
from sqlalchemy.orm import Session

def next_id(session: Session, table_name: str) -> int:
    count = int(session.execute(text(f"SELECT COUNT(*) FROM {table_name};")).scalar() or 0)
    if count == 0:
        return 1
    max_id = session.execute(text(f"SELECT MAX(id) FROM {table_name};")).scalar()
    return int(max_id or 0) + 1
```

## Savepoints

TetoDB supports savepoints; SQLAlchemy maps this via `begin_nested()`:

```python
with session.begin_nested():
    session.execute(text("INSERT INTO categories VALUES (1, 'Tech');"))
```

## SQLAlchemy Dialect Caveats

- Reflection metadata is intentionally limited
- `supports_alter = False`
- `get_table_names`, `get_view_names`, `get_indexes`, and `get_foreign_keys` currently return empty results
- `get_pk_constraint` is not fully populated
- Prefer explicit schema and model definitions over reflection/autogen workflows

## DBAPI Notes

- Driver module: `tetodb.dbapi`
- `paramstyle` is `qmark` (`?`)
- Dialect rewrites SQLAlchemy named bind params to positional binds

## Common Failure Modes

- If an operation fails in a transaction, always call `session.rollback()`
- If your API starts returning repeated `500` errors after a DB exception, ensure sessions are rolled back/closed and restart API if needed
- Use positional inserts only:

```sql
INSERT INTO users VALUES (?, ?);
```

Not supported:

```sql
INSERT INTO users (id, name) VALUES (?, ?);
```
