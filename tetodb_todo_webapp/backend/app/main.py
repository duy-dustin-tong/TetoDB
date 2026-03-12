import time
import importlib
from urllib.parse import urlparse
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from .db import DATABASE_URL, get_session
from .models import Category, Priority, Project, Todo, TodoComment
from .schemas import (
    CategoryCreate,
    CategoryOut,
    CategoryStatsOut,
    DeleteResult,
    FeatureShowcaseOut,
    ProjectOut,
    StatsOut,
    TodoCreate,
    TodoListResponse,
    TodoOut,
    TodoUpdate,
)


app = FastAPI(title="TetoDB Todo API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _safe_execute(session: Session, sql: str) -> bool:
    try:
        session.execute(text(sql))
        return True
    except Exception:
        session.rollback()
        return False


def _table_exists(session: Session, table_name: str) -> bool:
    return _safe_execute(session, f"SELECT * FROM {table_name} LIMIT 0;")


def _schema_is_current(session: Session) -> bool:
    checks = [
        "SELECT id, slug, name FROM projects LIMIT 0;",
        "SELECT id, code, weight FROM priorities LIMIT 0;",
        "SELECT id, name, project_id FROM categories LIMIT 0;",
        "SELECT id, title, is_done, created_at, due_at, assignee, project_id, category_id, priority_id FROM todos LIMIT 0;",
        "SELECT id, todo_id, body, author, created_at FROM todo_comments LIMIT 0;",
    ]
    return all(_safe_execute(session, q) for q in checks)


def _drop_table_if_exists(session: Session, table_name: str) -> bool:
    if not _table_exists(session, table_name):
        return False

    # TetoDB doesn't currently support DROP TABLE IF EXISTS.
    # Also, each drop is committed independently so a later rollback
    # does not undo prior successful drops.
    try:
        session.execute(text(f"DROP TABLE {table_name};"))
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        msg = str(e).lower()
        if "not found" in msg or "table or view" in msg:
            return False
        if "referenced by a foreign key" in msg or "cannot drop table" in msg:
            return False
        raise


def _create_schema(session: Session) -> None:
    session.execute(
        text(
            "CREATE TABLE projects ("
            "id INTEGER PRIMARY KEY, "
            "slug VARCHAR(40) NOT NULL UNIQUE, "
            "name VARCHAR(80) NOT NULL"
            ");"
        )
    )

    session.execute(
        text(
            "CREATE TABLE priorities ("
            "id INTEGER PRIMARY KEY, "
            "code VARCHAR(20) NOT NULL UNIQUE, "
            "weight INTEGER NOT NULL"
            ");"
        )
    )

    session.execute(
        text(
            "CREATE TABLE categories ("
            "id INTEGER PRIMARY KEY, "
            "name VARCHAR(50) NOT NULL UNIQUE, "
            "project_id INTEGER NOT NULL, "
            "FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE"
            ");"
        )
    )

    session.execute(
        text(
            "CREATE TABLE todos ("
            "id INTEGER PRIMARY KEY, "
            "title VARCHAR(255) NOT NULL, "
            "is_done BOOLEAN NOT NULL, "
            "created_at BIGINT NOT NULL, "
            "due_at BIGINT, "
            "assignee VARCHAR(64), "
            "project_id INTEGER NOT NULL, "
            "category_id INTEGER, "
            "priority_id INTEGER, "
            "FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE, "
            "FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE, "
            "FOREIGN KEY (priority_id) REFERENCES priorities(id) ON DELETE CASCADE"
            ");"
        )
    )

    session.execute(
        text(
            "CREATE TABLE todo_comments ("
            "id INTEGER PRIMARY KEY, "
            "todo_id INTEGER NOT NULL, "
            "body VARCHAR(255) NOT NULL, "
            "author VARCHAR(64) NOT NULL, "
            "created_at BIGINT NOT NULL, "
            "FOREIGN KEY (todo_id) REFERENCES todos(id) ON DELETE CASCADE"
            ");"
        )
    )

    session.execute(text("CREATE INDEX idx_todos_is_done ON todos (is_done);"))
    session.execute(text("CREATE INDEX idx_todos_created_at ON todos (created_at);"))
    session.execute(text("CREATE INDEX idx_todos_project_id ON todos (project_id);"))
    session.execute(text("CREATE INDEX idx_todos_category_id ON todos (category_id);"))
    session.execute(text("CREATE INDEX idx_todos_priority_id ON todos (priority_id);"))
    session.execute(text("CREATE INDEX idx_comments_todo_id ON todo_comments (todo_id);"))
    session.commit()


def _seed_basics(session: Session) -> None:
    project_count = int(session.execute(text("SELECT COUNT(*) FROM projects;")).scalar() or 0)
    if project_count == 0:
        session.execute(text("INSERT INTO projects VALUES (1, 'inbox', 'Inbox');"))
        session.execute(text("INSERT INTO projects VALUES (2, 'work', 'Work');"))

    priority_count = int(session.execute(text("SELECT COUNT(*) FROM priorities;")).scalar() or 0)
    if priority_count == 0:
        session.execute(text("INSERT INTO priorities VALUES (1, 'LOW', 1);"))
        session.execute(text("INSERT INTO priorities VALUES (2, 'MEDIUM', 2);"))
        session.execute(text("INSERT INTO priorities VALUES (3, 'HIGH', 3);"))

    category_count = int(session.execute(text("SELECT COUNT(*) FROM categories;")).scalar() or 0)
    if category_count == 0:
        session.execute(text("INSERT INTO categories VALUES (1, 'General', 1);"))
        session.execute(text("INSERT INTO categories VALUES (2, 'Bugfix', 2);"))

    session.commit()


def _init_schema(session: Session) -> None:
    if _schema_is_current(session):
        _seed_basics(session)
        return

    # TetoDB currently does not support ALTER TABLE, so if schema drifts
    # we recreate the demo tables from scratch. We do multiple passes to
    # handle FK dependency order robustly across mixed legacy states.
    drop_order = ["todo_comments", "todos", "categories", "priorities", "projects"]
    for _ in range(4):
        dropped_any = False
        for table_name in drop_order:
            dropped_any = _drop_table_if_exists(session, table_name) or dropped_any
        if not dropped_any:
            break

    remaining = [name for name in drop_order if _table_exists(session, name)]
    if remaining:
        raise RuntimeError(
            "Could not reset old schema. Remaining tables: " + ", ".join(remaining)
        )

    _create_schema(session)
    _seed_basics(session)


def _to_todo(row: Any) -> TodoOut:
    if isinstance(row, Todo):
        return TodoOut(
            id=int(row.id),
            title=str(row.title),
            is_done=bool(row.is_done),
            created_at=int(row.created_at),
            due_at=row.due_at,
            assignee=row.assignee,
            project_id=int(row.project_id),
            category_id=row.category_id,
            priority_id=row.priority_id,
        )

    d = row._asdict() if hasattr(row, "_asdict") else dict(row)
    return TodoOut(
        id=int(d["id"]),
        title=str(d["title"]),
        is_done=bool(d["is_done"]),
        created_at=int(d["created_at"]),
        due_at=d.get("due_at"),
        assignee=d.get("assignee"),
        project_id=int(d["project_id"]),
        project_name=d.get("project_name"),
        category_id=d.get("category_id"),
        category_name=d.get("category_name"),
        priority_id=d.get("priority_id"),
        priority_code=d.get("priority_code"),
    )


def _next_id(session: Session, table_name: str) -> int:
    max_id = session.execute(text(f"SELECT MAX(id) FROM {table_name};")).scalar()
    return int(max_id or 0) + 1


def _parse_int_list(raw: str | None) -> list[int]:
    if not raw:
        return []
    values: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        values.append(int(part))
    return values


def _init_schema_bootstrap() -> None:
    # Startup DDL/DML via direct DBAPI connections to isolate each statement
    # from transaction poisoning when legacy objects are missing.
    parsed = urlparse(DATABASE_URL)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 5432
    database = parsed.path.lstrip("/") or "e2e"
    tetodb_dbapi = importlib.import_module("tetodb.dbapi")

    def _exec(sql: str, fetch_one: bool = False) -> tuple[bool, Any, str]:
        conn = tetodb_dbapi.connect(host=host, port=port, database=database)
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone() if fetch_one else None
            return True, row, ""
        except Exception as e:
            return False, None, str(e)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # Reset known app schema in FK-safe order. Run multiple passes because
    # legacy states may have partial dependencies.
    drop_order = ["todo_comments", "todos", "categories", "priorities", "projects"]
    for _ in range(6):
        dropped_any = False
        for table_name in drop_order:
            ok, _, err = _exec(f"DROP TABLE {table_name};")
            if ok:
                dropped_any = True
            else:
                err_l = err.lower()
                if "not found" in err_l or "table or view" in err_l:
                    continue
        if not dropped_any:
            break

    create_sql = [
            (
                "CREATE TABLE projects ("
                "id INTEGER PRIMARY KEY, "
                "slug VARCHAR(40) NOT NULL, "
                "name VARCHAR(80) NOT NULL"
                ");"
            ),
            (
                "CREATE TABLE priorities ("
                "id INTEGER PRIMARY KEY, "
                "code VARCHAR(20) NOT NULL, "
                "weight INTEGER NOT NULL"
                ");"
            ),
            (
                "CREATE TABLE categories ("
                "id INTEGER PRIMARY KEY, "
                "name VARCHAR(50) NOT NULL, "
                "project_id INTEGER NOT NULL, "
                "FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE"
                ");"
            ),
            (
                "CREATE TABLE todos ("
                "id INTEGER PRIMARY KEY, "
                "title VARCHAR(255) NOT NULL, "
                "is_done BOOLEAN NOT NULL, "
                "created_at BIGINT NOT NULL, "
                "due_at BIGINT, "
                "assignee VARCHAR(64), "
                "project_id INTEGER NOT NULL, "
                "category_id INTEGER, "
                "priority_id INTEGER, "
                "FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE, "
                "FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE, "
                "FOREIGN KEY (priority_id) REFERENCES priorities(id) ON DELETE CASCADE"
                ");"
            ),
            (
                "CREATE TABLE todo_comments ("
                "id INTEGER PRIMARY KEY, "
                "todo_id INTEGER NOT NULL, "
                "body VARCHAR(255) NOT NULL, "
                "author VARCHAR(64) NOT NULL, "
                "created_at BIGINT NOT NULL, "
                "FOREIGN KEY (todo_id) REFERENCES todos(id) ON DELETE CASCADE"
                ");"
            ),
            "CREATE UNIQUE INDEX uq_projects_slug ON projects (slug);",
            "CREATE UNIQUE INDEX uq_priorities_code ON priorities (code);",
            "CREATE UNIQUE INDEX uq_categories_name ON categories (name);",
            "CREATE INDEX idx_todos_is_done ON todos (is_done);",
            "CREATE INDEX idx_todos_created_at ON todos (created_at);",
            "CREATE INDEX idx_todos_project_id ON todos (project_id);",
            "CREATE INDEX idx_todos_category_id ON todos (category_id);",
            "CREATE INDEX idx_todos_priority_id ON todos (priority_id);",
            "CREATE INDEX idx_comments_todo_id ON todo_comments (todo_id);",
        ]

    for sql in create_sql:
        ok, _, err = _exec(sql)
        if not ok:
            raise RuntimeError(
                f"Schema bootstrap failed on SQL: {sql}. Error: {err}"
            )

    ok, row, _ = _exec("SELECT COUNT(*) FROM projects;", fetch_one=True)
    if ok:
        project_count = int((row[0] if row else 0) or 0)
        if project_count == 0:
            _exec("INSERT INTO projects VALUES (1, 'inbox', 'Inbox');")
            _exec("INSERT INTO projects VALUES (2, 'work', 'Work');")

    ok, row, _ = _exec("SELECT COUNT(*) FROM priorities;", fetch_one=True)
    if ok:
        priority_count = int((row[0] if row else 0) or 0)
        if priority_count == 0:
            _exec("INSERT INTO priorities VALUES (1, 'LOW', 1);")
            _exec("INSERT INTO priorities VALUES (2, 'MEDIUM', 2);")
            _exec("INSERT INTO priorities VALUES (3, 'HIGH', 3);")

    ok, row, _ = _exec("SELECT COUNT(*) FROM categories;", fetch_one=True)
    if ok:
        category_count = int((row[0] if row else 0) or 0)
        if category_count == 0:
            _exec("INSERT INTO categories VALUES (1, 'General', 1);")
            _exec("INSERT INTO categories VALUES (2, 'Bugfix', 2);")


@app.on_event("startup")
def startup() -> None:
    _init_schema_bootstrap()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/projects", response_model=list[ProjectOut])
def list_projects(session: Session = Depends(get_session)) -> list[ProjectOut]:
    try:
        rows = session.execute(
            text("SELECT id, slug, name FROM projects ORDER BY id ASC;")
        ).all()
        return [
            ProjectOut(id=int(row[0]), slug=str(row[1]), name=str(row[2]))
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list projects: {e}")


@app.get("/api/categories", response_model=list[CategoryOut])
def list_categories(session: Session = Depends(get_session)) -> list[CategoryOut]:
    try:
        cats = session.scalars(select(Category).order_by(Category.name.asc())).all()
        out: list[CategoryOut] = []
        for c in cats:
            project_name = None
            if c.project_id is not None:
                project_name = session.scalar(
                    select(Project.name).where(Project.id == c.project_id)
                )
            out.append(
                CategoryOut(
                    id=int(c.id),
                    name=str(c.name),
                    project_id=int(c.project_id),
                    project_name=str(project_name) if project_name is not None else None,
                )
            )
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list categories: {e}")


@app.post("/api/categories", response_model=CategoryOut)
def create_category(payload: CategoryCreate, session: Session = Depends(get_session)) -> CategoryOut:
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name cannot be empty")

    project_id = int(payload.project_id or 1)
    project = session.scalar(select(Project).where(Project.id == project_id))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    next_id = _next_id(session, "categories")
    try:
        session.execute(
            text("INSERT INTO categories VALUES (:id, :name, :project_id);"),
            {"id": next_id, "name": name, "project_id": project_id},
        )
        session.commit()
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=f"Failed to create category: {e}")

    return CategoryOut(
        id=next_id,
        name=name,
        project_id=project_id,
        project_name=project.name,
    )


@app.delete("/api/categories/{cat_id}", response_model=DeleteResult)
def delete_category(cat_id: int, session: Session = Depends(get_session)) -> DeleteResult:
    result = session.execute(text("DELETE FROM categories WHERE id = :id;"), {"id": cat_id})
    session.commit()
    rowcount = getattr(result, "rowcount", None)
    deleted = 0 if rowcount is None or rowcount < 0 else int(rowcount)
    return DeleteResult(deleted=deleted)


@app.get("/api/todos", response_model=TodoListResponse)
def list_todos(
    session: Session = Depends(get_session),
    done: bool | None = Query(default=None),
    q: str | None = Query(default=None, max_length=255),
    assignee: str | None = Query(default=None, max_length=64),
    project_id: int | None = Query(default=None),
    category_ids: str | None = Query(default=None, description="CSV list of category ids"),
    created_from: int | None = Query(default=None),
    created_to: int | None = Query(default=None),
    sort: str = Query(default="created_desc"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> TodoListResponse:
    try:
        stmt = select(Todo)

        if done is not None:
            stmt = stmt.where(Todo.is_done == done)
        if project_id is not None:
            stmt = stmt.where(Todo.project_id == project_id)
        if q:
            stmt = stmt.where(Todo.title.like(f"%{q}%"))
        if assignee:
            stmt = stmt.where(Todo.assignee.like(f"%{assignee}%"))

        cat_ids = _parse_int_list(category_ids)
        if cat_ids:
            stmt = stmt.where(Todo.category_id.in_(cat_ids))

        if created_from is not None and created_to is not None:
            stmt = stmt.where(Todo.created_at.between(created_from, created_to))
        elif created_from is not None:
            stmt = stmt.where(Todo.created_at >= created_from)
        elif created_to is not None:
            stmt = stmt.where(Todo.created_at <= created_to)

        sort_map = {
            "created_desc": Todo.created_at.desc(),
            "created_asc": Todo.created_at.asc(),
            "title_asc": Todo.title.asc(),
            "title_desc": Todo.title.desc(),
            "assignee_asc": Todo.assignee.asc(),
            "priority_desc": Todo.priority_id.desc(),
        }
        order_expr = sort_map.get(sort, Todo.created_at.desc())

        total = int(session.execute(text("SELECT COUNT(*) FROM todos;")).scalar() or 0)
        todos = session.scalars(stmt.order_by(order_expr).limit(limit).offset(offset)).all()

        items: list[TodoOut] = []
        for t in todos:
            project_name = session.scalar(select(Project.name).where(Project.id == t.project_id))
            category_name = (
                session.scalar(select(Category.name).where(Category.id == t.category_id))
                if t.category_id is not None
                else None
            )
            priority_code = (
                session.scalar(select(Priority.code).where(Priority.id == t.priority_id))
                if t.priority_id is not None
                else None
            )

            items.append(
                TodoOut(
                    id=int(t.id),
                    title=str(t.title),
                    is_done=bool(t.is_done),
                    created_at=int(t.created_at),
                    due_at=t.due_at,
                    assignee=t.assignee,
                    project_id=int(t.project_id),
                    project_name=str(project_name) if project_name is not None else None,
                    category_id=t.category_id,
                    category_name=str(category_name) if category_name is not None else None,
                    priority_id=t.priority_id,
                    priority_code=str(priority_code) if priority_code is not None else None,
                )
            )

        return TodoListResponse(items=items, total=total)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list todos: {e}")


@app.post("/api/todos", response_model=TodoOut)
def create_todo(payload: TodoCreate, session: Session = Depends(get_session)) -> TodoOut:
    title = payload.title.strip()
    if not title:
        raise HTTPException(status_code=400, detail="Title cannot be empty")
    try:
        project_id = int(payload.project_id or 1)

        project_exists = int(
            session.execute(
                text("SELECT COUNT(*) FROM projects WHERE id = :id;"),
                {"id": project_id},
            ).scalar()
            or 0
        )
        if project_exists == 0:
            raise HTTPException(status_code=404, detail="Project not found")

        if payload.category_id is not None:
            cat_exists = int(
                session.execute(
                    text("SELECT COUNT(*) FROM categories WHERE id = :id;"),
                    {"id": int(payload.category_id)},
                ).scalar()
                or 0
            )
            if cat_exists == 0:
                raise HTTPException(status_code=404, detail="Category not found")

        if payload.priority_id is not None:
            pri_exists = int(
                session.execute(
                    text("SELECT COUNT(*) FROM priorities WHERE id = :id;"),
                    {"id": int(payload.priority_id)},
                ).scalar()
                or 0
            )
            if pri_exists == 0:
                raise HTTPException(status_code=404, detail="Priority not found")

        # TetoDB FK checks currently reject NULL on this FK path in some builds,
        # so default to MEDIUM priority when not provided.
        priority_id = int(payload.priority_id) if payload.priority_id is not None else 2

        next_id = _next_id(session, "todos")
        created_at = int(time.time())
        session.execute(
            text(
                "INSERT INTO todos VALUES "
                "(:id, :title, :is_done, :created_at, :due_at, :assignee, :project_id, :category_id, :priority_id);"
            ),
            {
                "id": next_id,
                "title": title,
                "is_done": False,
                "created_at": created_at,
                "due_at": payload.due_at,
                "assignee": payload.assignee,
                "project_id": project_id,
                "category_id": payload.category_id,
                "priority_id": priority_id,
            },
        )
        session.commit()

        project_name = session.execute(
            text("SELECT name FROM projects WHERE id = :id;"),
            {"id": project_id},
        ).scalar()

        category_name = None
        if payload.category_id is not None:
            category_name = session.execute(
                text("SELECT name FROM categories WHERE id = :id;"),
                {"id": int(payload.category_id)},
            ).scalar()

        priority_code = None
        if priority_id is not None:
            priority_code = session.execute(
                text("SELECT code FROM priorities WHERE id = :id;"),
                {"id": int(priority_id)},
            ).scalar()

        return TodoOut(
            id=next_id,
            title=title,
            is_done=False,
            created_at=created_at,
            due_at=payload.due_at,
            assignee=payload.assignee,
            project_id=project_id,
            project_name=str(project_name) if project_name is not None else None,
            category_id=payload.category_id,
            category_name=str(category_name) if category_name is not None else None,
            priority_id=priority_id,
            priority_code=str(priority_code) if priority_code is not None else None,
        )
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create todo: {e}")


@app.patch("/api/todos/{todo_id}", response_model=TodoOut)
def update_todo(todo_id: int, payload: TodoUpdate, session: Session = Depends(get_session)) -> TodoOut:
    row = session.scalar(select(Todo).where(Todo.id == todo_id))
    if not row:
        raise HTTPException(status_code=404, detail="Todo not found")

    title = payload.title.strip() if payload.title is not None else row.title
    if not title:
        raise HTTPException(status_code=400, detail="Title cannot be empty")

    is_done = payload.is_done if payload.is_done is not None else bool(row.is_done)
    cat_id = payload.category_id if payload.category_id is not None else row.category_id
    pri_id = payload.priority_id if payload.priority_id is not None else row.priority_id
    if pri_id is None:
        pri_id = 2
    due_at = payload.due_at if payload.due_at is not None else row.due_at
    assignee = payload.assignee if payload.assignee is not None else row.assignee

    session.execute(
        text(
            "UPDATE todos "
            "SET title = :title, is_done = :is_done, category_id = :cat_id, "
            "priority_id = :pri_id, due_at = :due_at, assignee = :assignee "
            "WHERE id = :id;"
        ),
        {
            "id": todo_id,
            "title": title,
            "is_done": is_done,
            "cat_id": cat_id,
            "pri_id": pri_id,
            "due_at": due_at,
            "assignee": assignee,
        },
    )
    session.commit()

    updated = session.scalar(select(Todo).where(Todo.id == todo_id))
    if not updated:
        raise HTTPException(status_code=500, detail="Failed to update todo")
    project_name = session.scalar(select(Project.name).where(Project.id == updated.project_id))
    category_name = (
        session.scalar(select(Category.name).where(Category.id == updated.category_id))
        if updated.category_id is not None
        else None
    )
    priority_code = (
        session.scalar(select(Priority.code).where(Priority.id == updated.priority_id))
        if updated.priority_id is not None
        else None
    )
    return TodoOut(
        id=int(updated.id),
        title=str(updated.title),
        is_done=bool(updated.is_done),
        created_at=int(updated.created_at),
        due_at=updated.due_at,
        assignee=updated.assignee,
        project_id=int(updated.project_id),
        project_name=str(project_name) if project_name is not None else None,
        category_id=updated.category_id,
        category_name=str(category_name) if category_name is not None else None,
        priority_id=updated.priority_id,
        priority_code=str(priority_code) if priority_code is not None else None,
    )


@app.delete("/api/todos/{todo_id}", response_model=DeleteResult)
def delete_todo(todo_id: int, session: Session = Depends(get_session)) -> DeleteResult:
    result = session.execute(text("DELETE FROM todos WHERE id = :id;"), {"id": todo_id})
    session.commit()
    rowcount = getattr(result, "rowcount", None)
    deleted = 0 if rowcount is None or rowcount < 0 else int(rowcount)
    return DeleteResult(deleted=deleted)


@app.post("/api/todos/{todo_id}/comments", response_model=DeleteResult)
def add_comment(
    todo_id: int,
    body: str = Query(min_length=1, max_length=255),
    author: str = Query(default="web", min_length=1, max_length=64),
    session: Session = Depends(get_session),
) -> DeleteResult:
    if not session.scalar(select(Todo).where(Todo.id == todo_id)):
        raise HTTPException(status_code=404, detail="Todo not found")

    next_id = _next_id(session, "todo_comments")
    session.execute(
        text("INSERT INTO todo_comments VALUES (:id, :todo_id, :body, :author, :created_at);"),
        {
            "id": next_id,
            "todo_id": todo_id,
            "body": body,
            "author": author,
            "created_at": int(time.time()),
        },
    )
    session.commit()
    return DeleteResult(deleted=1)


@app.post("/api/todos/clear-completed", response_model=DeleteResult)
def clear_completed(session: Session = Depends(get_session)) -> DeleteResult:
    result = session.execute(text("DELETE FROM todos WHERE is_done = TRUE;"))
    session.commit()
    rowcount = getattr(result, "rowcount", None)
    deleted = 0 if rowcount is None or rowcount < 0 else int(rowcount)
    return DeleteResult(deleted=deleted)


@app.get("/api/stats", response_model=StatsOut)
def stats(session: Session = Depends(get_session)) -> StatsOut:
    total = int(session.execute(text("SELECT COUNT(*) FROM todos;")).scalar() or 0)
    completed = int(
        session.execute(text("SELECT COUNT(*) FROM todos WHERE is_done = TRUE;")).scalar() or 0
    )
    return StatsOut(total=total, completed=completed, open=total - completed)


@app.get("/api/stats/by-category", response_model=list[CategoryStatsOut])
def category_stats(
    min_count: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> list[CategoryStatsOut]:
    rows = session.execute(
        select(
            Category.id.label("category_id"),
            Category.name.label("category_name"),
            func.count(Todo.id).label("todo_count"),
            func.avg(func.length(Todo.title)).label("avg_title_len"),
        )
        .join(Todo, Todo.category_id == Category.id)
        .group_by(Category.id, Category.name)
        .having(func.count(Todo.id) >= min_count)
        .order_by(func.count(Todo.id).desc())
    ).all()

    out: list[CategoryStatsOut] = []
    for row in rows:
        done_count = int(
            session.execute(
                text(
                    "SELECT COUNT(*) FROM todos WHERE category_id = :cid AND is_done = TRUE;"
                ),
                {"cid": int(row.category_id)},
            ).scalar()
            or 0
        )
        out.append(
            CategoryStatsOut(
                category_id=int(row.category_id),
                category_name=str(row.category_name),
                todo_count=int(row.todo_count),
                done_count=done_count,
                avg_title_len=float(row.avg_title_len or 0.0),
            )
        )
    return out


@app.get("/api/features/showcase", response_model=FeatureShowcaseOut)
def feature_showcase(session: Session = Depends(get_session)) -> FeatureShowcaseOut:
    # CTE + COUNT(*)
    cte_count = int(
        session.execute(
            text(
                "WITH open_todos AS ("
                "SELECT id FROM todos WHERE is_done = FALSE"
                ") SELECT COUNT(*) FROM open_todos;"
            )
        ).scalar()
        or 0
    )

    # DISTINCT (ORM)
    distinct_assignees = [
        str(v)
        for v in session.scalars(
            select(Todo.assignee)
            .where(Todo.assignee.is_not(None))
            .distinct()
            .order_by(Todo.assignee.asc())
        ).all()
    ]

    # Set operations
    union_rows = session.execute(
        text(
            "SELECT name FROM categories UNION SELECT name FROM projects ORDER BY name;"
        )
    ).all()
    intersect_rows = session.execute(
        text(
            "SELECT name FROM categories INTERSECT SELECT name FROM projects ORDER BY name;"
        )
    ).all()
    except_rows = session.execute(
        text(
            "SELECT name FROM categories EXCEPT SELECT name FROM projects ORDER BY name;"
        )
    ).all()

    return FeatureShowcaseOut(
        total_count_star=cte_count,
        distinct_assignees=distinct_assignees,
        union_names=[str(r[0]) for r in union_rows],
        intersect_names=[str(r[0]) for r in intersect_rows],
        except_names=[str(r[0]) for r in except_rows],
    )


@app.get("/api/debug/orm")
def orm_debug(session: Session = Depends(get_session)) -> dict[str, int]:
    # Explicitly touch ORM relationship-mapped classes to prove ORM mapping usage.
    project_count = int(session.scalar(select(func.count()).select_from(Project)) or 0)
    category_count = int(session.scalar(select(func.count()).select_from(Category)) or 0)
    todo_count = int(session.scalar(select(func.count()).select_from(Todo)) or 0)
    comment_count = int(session.scalar(select(func.count()).select_from(TodoComment)) or 0)
    priority_count = int(session.scalar(select(func.count()).select_from(Priority)) or 0)
    return {
        "projects": project_count,
        "categories": category_count,
        "todos": todo_count,
        "comments": comment_count,
        "priorities": priority_count,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
