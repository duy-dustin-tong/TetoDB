from datetime import datetime
import re
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import Boolean, Integer, String, create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Mapped, Session, declarative_base, mapped_column, sessionmaker


DATABASE_URL = "tetodb://127.0.0.1:9090/todoapp_demo"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)


class TodoList(Base):
    __tablename__ = "todo_lists"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[str] = mapped_column(String(19), nullable=False)


class Todo(Base):
    __tablename__ = "todos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    list_id: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    is_done: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    due_at: Mapped[Optional[str]] = mapped_column(String(19), nullable=True)
    created_at: Mapped[str] = mapped_column(String(19), nullable=False)


class ActivityLog(Base):
    __tablename__ = "activity_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    todo_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    action: Mapped[str] = mapped_column(String(30), nullable=False)
    created_at: Mapped[str] = mapped_column(String(19), nullable=False)


class CreateListIn(BaseModel):
    title: str = Field(min_length=1, max_length=100)


class CreateTodoIn(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    priority: int = Field(default=1, ge=1, le=3)
    due_at: Optional[str] = None

    @field_validator("due_at")
    @classmethod
    def validate_due_at(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        if value == "":
            return None
        pattern = r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$"
        if not re.match(pattern, value):
            raise ValueError("due_at must be YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
        return value


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_session():
    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def safe_exec(session: Session, sql: str) -> bool:
    try:
        session.execute(text(sql))
        session.commit()
        return True
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass
        return False


def safe_exec_sql(sql: str) -> bool:
    with SessionLocal() as session:
        return safe_exec(session, sql)


def next_id(session: Session, table_name: str) -> int:
    try:
        count = int(session.execute(text(f"SELECT COUNT(*) FROM {table_name};")).scalar() or 0)
        if count == 0:
            return 1
    except Exception:
        pass

    try:
        max_id = session.execute(text(f"SELECT MAX(id) FROM {table_name};")).scalar()
        return int(max_id or 0) + 1
    except Exception:
        return 1


def init_schema() -> None:
    try:
        with SessionLocal() as session:
            session.execute(text("SELECT COUNT(*) FROM users;")).scalar()
            return
    except Exception:
        pass

    safe_exec_sql(
        """
        CREATE TABLE users (
          id INTEGER PRIMARY KEY,
          name VARCHAR(50) NOT NULL UNIQUE
        );
        """
    )

    safe_exec_sql(
        """
        CREATE TABLE todo_lists (
          id INTEGER PRIMARY KEY,
          user_id INTEGER NOT NULL,
          title VARCHAR(100) NOT NULL,
          created_at TIMESTAMP NOT NULL,
          FOREIGN KEY (user_id) REFERENCES users (id)
            ON DELETE CASCADE ON UPDATE RESTRICT
        );
        """
    )

    safe_exec_sql(
        """
        CREATE TABLE todos (
          id INTEGER PRIMARY KEY,
          list_id INTEGER NOT NULL,
          title VARCHAR(200) NOT NULL,
          is_done BOOLEAN NOT NULL,
          priority INTEGER NOT NULL,
          due_at TIMESTAMP,
          created_at TIMESTAMP NOT NULL,
          FOREIGN KEY (list_id) REFERENCES todo_lists (id)
            ON DELETE CASCADE ON UPDATE RESTRICT
        );
        """
    )

    safe_exec_sql(
        """
        CREATE TABLE activity_logs (
          id INTEGER PRIMARY KEY,
          todo_id INTEGER,
          action VARCHAR(30) NOT NULL,
          created_at TIMESTAMP NOT NULL,
          FOREIGN KEY (todo_id) REFERENCES todos (id)
            ON DELETE SET NULL ON UPDATE RESTRICT
        );
        """
    )

    safe_exec_sql("CREATE UNIQUE INDEX idx_users_name ON users (name);")
    safe_exec_sql("CREATE INDEX idx_todo_lists_user ON todo_lists (user_id);")
    safe_exec_sql("CREATE INDEX idx_todos_list_done ON todos (list_id, is_done);")
    safe_exec_sql("CREATE INDEX idx_todos_priority_due ON todos (priority, due_at);")

    safe_exec_sql(
        """
        CREATE VIEW v_open_todos AS
          SELECT t.id, t.list_id, t.title, t.priority, t.due_at
          FROM todos t
          WHERE t.is_done = FALSE;
        """
    )

    safe_exec_sql("INSERT INTO users VALUES (1, 'demo');")


app = FastAPI(title="TetoDB Todo App")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


@app.exception_handler(SQLAlchemyError)
async def sqlalchemy_exception_handler(request, exc):
    return JSONResponse(status_code=400, content={"detail": "Database query failed"})


@app.exception_handler(Exception)
async def unhandled_exception_handler(request, exc):
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


@app.on_event("startup")
def on_startup() -> None:
    init_schema()


@app.get("/health")
def healthcheck() -> dict:
    return {"ok": True, "database": DATABASE_URL}


@app.get("/lists")
def get_lists(session: Session = Depends(get_session)) -> list[dict]:
    rows = session.execute(
        text(
            """
            SELECT id, title
            FROM todo_lists
            ORDER BY id ASC;
            """
        )
    ).all()

    items: list[dict] = []
    for row in rows:
        total = int(
            session.execute(
                text("SELECT COUNT(*) FROM todos WHERE list_id = :list_id;"),
                {"list_id": row[0]},
            ).scalar()
            or 0
        )
        items.append({"id": row[0], "title": row[1], "total": total})

    return items


@app.post("/lists")
def create_list(payload: CreateListIn, session: Session = Depends(get_session)) -> dict:
    list_id = next_id(session, "todo_lists")
    session.execute(
        text("INSERT INTO todo_lists VALUES (:id, :user_id, :title, :created_at);"),
        {"id": list_id, "user_id": 1, "title": payload.title, "created_at": now_str()},
    )
    session.commit()
    return {"id": list_id, "title": payload.title}


@app.get("/lists/{list_id}/todos")
def get_todos(list_id: int, session: Session = Depends(get_session)) -> list[dict]:
    rows = session.execute(
        text(
            """
            SELECT id, title, is_done, priority
            FROM todos
            WHERE list_id = :list_id
            ORDER BY is_done ASC, priority DESC, id DESC;
            """
        ),
        {"list_id": list_id},
    ).all()
    return [
        {
            "id": row[0],
            "title": row[1],
            "is_done": bool(row[2]),
            "priority": int(row[3]),
            "due_at": None,
            "created_at": None,
        }
        for row in rows
    ]


@app.post("/lists/{list_id}/todos")
def create_todo(list_id: int, payload: CreateTodoIn, session: Session = Depends(get_session)) -> dict:
    list_exists = session.execute(
        text("SELECT COUNT(*) FROM todo_lists WHERE id = :list_id;"), {"list_id": list_id}
    ).scalar()
    if int(list_exists or 0) == 0:
        raise HTTPException(status_code=404, detail="List not found")

    todo_id = next_id(session, "todos")

    try:
        session.execute(
            text(
                """
                INSERT INTO todos VALUES (
                  :id, :list_id, :title, :is_done, :priority, :due_at, :created_at
                );
                """
            ),
            {
                "id": todo_id,
                "list_id": list_id,
                "title": payload.title,
                "is_done": False,
                "priority": payload.priority,
                "due_at": payload.due_at,
                "created_at": now_str(),
            },
        )

        with session.begin_nested():
            log_id = next_id(session, "activity_logs")
            session.execute(
                text("INSERT INTO activity_logs VALUES (:id, :todo_id, :action, :created_at);"),
                {
                    "id": log_id,
                    "todo_id": todo_id,
                    "action": "CREATE",
                    "created_at": now_str(),
                },
            )

        session.commit()
    except SQLAlchemyError:
        session.rollback()
        raise HTTPException(status_code=400, detail="Invalid todo payload for database")
    except Exception:
        session.rollback()
        raise HTTPException(status_code=500, detail="Failed to create todo")

    return {"id": todo_id, "title": payload.title, "priority": payload.priority}


@app.patch("/todos/{todo_id}/toggle")
def toggle_todo(todo_id: int, session: Session = Depends(get_session)) -> dict:
    todo = session.get(Todo, todo_id)
    if not todo:
        raise HTTPException(status_code=404, detail="Todo not found")

    todo.is_done = not bool(todo.is_done)
    session.commit()

    log_id = next_id(session, "activity_logs")
    session.execute(
        text("INSERT INTO activity_logs VALUES (:id, :todo_id, :action, :created_at);"),
        {
            "id": log_id,
            "todo_id": todo_id,
            "action": "TOGGLE",
            "created_at": now_str(),
        },
    )
    session.commit()

    return {"id": todo.id, "is_done": bool(todo.is_done)}


@app.delete("/todos/{todo_id}")
def delete_todo(todo_id: int, session: Session = Depends(get_session)) -> dict:
    todo = session.get(Todo, todo_id)
    if not todo:
        raise HTTPException(status_code=404, detail="Todo not found")

    try:
        with session.begin_nested():
            log_id = next_id(session, "activity_logs")
            session.execute(
                text("INSERT INTO activity_logs VALUES (:id, :todo_id, :action, :created_at);"),
                {
                    "id": log_id,
                    "todo_id": todo_id,
                    "action": "DELETE",
                    "created_at": now_str(),
                },
            )

        session.delete(todo)
        session.commit()
    except Exception:
        session.rollback()
        raise

    return {"ok": True}


@app.get("/lists/{list_id}/stats")
def stats(list_id: int, session: Session = Depends(get_session)) -> dict:
    total = int(
        session.execute(
            text("SELECT COUNT(*) FROM todos WHERE list_id = :list_id;"),
            {"list_id": list_id},
        ).scalar()
        or 0
    )

    done = int(
        session.execute(
            text("SELECT COUNT(*) FROM todos WHERE list_id = :list_id AND is_done = TRUE;"),
            {"list_id": list_id},
        ).scalar()
        or 0
    )

    rows = session.execute(
        text("SELECT priority FROM todos WHERE list_id = :list_id;"),
        {"list_id": list_id},
    ).all()

    avg_priority = 0.0
    if rows:
        avg_priority = sum(float(row[0] or 0) for row in rows) / len(rows)

    return {
        "total": total,
        "done": done,
        "avg_priority": avg_priority,
    }


@app.get("/open-todos")
def open_todos(session: Session = Depends(get_session)) -> list[dict]:
    rows = session.execute(
        text("SELECT id, list_id, title, priority, due_at FROM v_open_todos ORDER BY priority DESC, id DESC;")
    ).all()
    return [
        {
            "id": row[0],
            "list_id": row[1],
            "title": row[2],
            "priority": row[3],
            "due_at": row[4],
        }
        for row in rows
    ]


@app.get("/lists/{list_id}/explain")
def explain_list_query(list_id: int, session: Session = Depends(get_session)) -> list[dict]:
    rows = session.execute(
        text("EXPLAIN SELECT * FROM todos WHERE list_id = :list_id AND is_done = FALSE ORDER BY priority DESC;"),
        {"list_id": list_id},
    ).all()
    return [{"line": str(row[0])} for row in rows]
