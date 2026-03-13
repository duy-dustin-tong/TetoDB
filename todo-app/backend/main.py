"""
Todo List API — FastAPI + SQLAlchemy + TetoDB
Using proper Session management to avoid transaction poisoning.
"""
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

from sqlalchemy import create_engine, Column, Integer, String, Boolean, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session

DATABASE_URL = "tetodb://127.0.0.1:9090/todoapp"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)
Base = declarative_base()


def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


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


def safe_exec(session: Session, sql: str) -> bool:
    try:
        session.execute(text(sql))
        session.commit()
        return True
    except Exception as e:
        try:
            session.rollback()
        except:
            pass
        return False


# ── ORM Models ───────────────────────────────────────────────────────
class Category(Base):
    __tablename__ = "categories"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)


class Todo(Base):
    __tablename__ = "todos"
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    description = Column(String)
    completed = Column(Boolean)
    priority = Column(Integer)
    category_id = Column(Integer)
    created_at = Column(String)


# ── Pydantic schemas ────────────────────────────────────────────────
class CategoryCreate(BaseModel):
    name: str

class TodoCreate(BaseModel):
    title: str
    description: Optional[str] = ""
    priority: Optional[int] = 1
    category_id: Optional[int] = None

class TodoUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    completed: Optional[bool] = None
    priority: Optional[int] = None
    category_id: Optional[int] = None


app = FastAPI(title="TetoDB Todo App")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def init_schema():
    with SessionLocal() as session:
        # Tables
        safe_exec(session, "CREATE TABLE categories (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL UNIQUE);")
        safe_exec(session, """
            CREATE TABLE todos (
                id INTEGER PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                description VARCHAR(500),
                completed BOOLEAN,
                priority INTEGER,
                category_id INTEGER,
                created_at TIMESTAMP,
                FOREIGN KEY (category_id) REFERENCES categories (id) ON DELETE CASCADE
            );
        """)
        # Indexes
        safe_exec(session, "CREATE INDEX idx_todos_category ON todos (category_id);")
        safe_exec(session, "CREATE INDEX idx_todos_completed ON todos (completed);")
        safe_exec(session, "CREATE INDEX idx_todos_priority ON todos (priority);")
    print("Schema initialized.")


@app.get("/api/categories")
def list_categories(session: Session = Depends(get_session)):
    try:
        rows = session.execute(text("SELECT id, name FROM categories ORDER BY name;")).fetchall()
        return [{"id": r[0], "name": r[1]} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/categories")
def create_category(cat: CategoryCreate, session: Session = Depends(get_session)):
    name = cat.name.strip()
    new_id = next_id(session, "categories")
    try:
        session.execute(
            text("INSERT INTO categories VALUES (:id, :name);"),
            {"id": new_id, "name": name}
        )
        session.commit()
        return {"id": new_id, "name": name}
    except Exception as e:
        session.rollback()
        import traceback
        raise HTTPException(status_code=500, detail=traceback.format_exc())


@app.delete("/api/categories/{cat_id}")
def delete_category(cat_id: int, session: Session = Depends(get_session)):
    try:
        session.execute(text("DELETE FROM categories WHERE id = :id;"), {"id": cat_id})
        session.commit()
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"deleted": cat_id}


@app.get("/api/todos")
def list_todos(category_id: Optional[int] = None, session: Session = Depends(get_session)):
    try:
        if category_id is not None:
            sql = """
                SELECT t.id, t.title, t.description, t.completed, t.priority,
                       t.category_id, t.created_at, c.name as category_name
                FROM todos t
                JOIN categories c ON t.category_id = c.id
                WHERE t.category_id = :cat_id
                ORDER BY t.priority;
            """
            rows = session.execute(text(sql), {"cat_id": category_id}).fetchall()
        else:
            sql = """
                SELECT t.id, t.title, t.description, t.completed, t.priority,
                       t.category_id, t.created_at
                FROM todos t
                ORDER BY t.priority;
            """
            rows = session.execute(text(sql)).fetchall()

        todos = []
        for r in rows:
            todo = {
                "id": r[0],
                "title": r[1],
                "description": r[2],
                "completed": r[3],
                "priority": r[4],
                "category_id": r[5],
                "created_at": str(r[6]) if r[6] else None,
            }
            if len(r) > 7:
                todo["category_name"] = r[7]
            todos.append(todo)

        return todos
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/todos")
def create_todo(todo: TodoCreate, session: Session = Depends(get_session)):
    new_id = next_id(session, "todos")
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    title = todo.title.strip()
    try:
        session.execute(
            text("INSERT INTO todos VALUES (:id, :title, :desc, :completed, :priority, :cat_id, :created_at);"),
            {
                "id": new_id,
                "title": title,
                "desc": todo.description if todo.description else None,
                "completed": False,
                "priority": todo.priority if todo.priority else 1,
                "cat_id": todo.category_id if todo.category_id else None,
                "created_at": now
            }
        )
        session.commit()
        return {
            "id": new_id,
            "title": title,
            "description": todo.description,
            "completed": False,
            "priority": todo.priority,
            "category_id": todo.category_id,
            "created_at": now,
        }
    except Exception as e:
        session.rollback()
        import traceback
        raise HTTPException(status_code=500, detail=traceback.format_exc())


@app.put("/api/todos/{todo_id}")
def update_todo(todo_id: int, updates: TodoUpdate, session: Session = Depends(get_session)):
    set_clauses = []
    params = {"id": todo_id}

    if updates.title is not None:
        set_clauses.append("title = :title")
        params["title"] = updates.title
    if updates.description is not None:
        set_clauses.append("description = :description")
        params["description"] = updates.description
    if updates.completed is not None:
        set_clauses.append("completed = :completed")
        params["completed"] = updates.completed
    if updates.priority is not None:
        set_clauses.append("priority = :priority")
        params["priority"] = updates.priority
    if updates.category_id is not None:
        set_clauses.append("category_id = :category_id")
        params["category_id"] = updates.category_id

    if not set_clauses:
        raise HTTPException(status_code=400, detail="No fields to update")

    sql = f"UPDATE todos SET {', '.join(set_clauses)} WHERE id = :id;"

    try:
        session.execute(text(sql), params)
        session.commit()
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    return {"updated": todo_id}


@app.delete("/api/todos/{todo_id}")
def delete_todo(todo_id: int, session: Session = Depends(get_session)):
    try:
        session.execute(text("DELETE FROM todos WHERE id = :id;"), {"id": todo_id})
        session.commit()
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"deleted": todo_id}


@app.get("/api/stats")
def get_stats(session: Session = Depends(get_session)):
    try:
        total = int(session.execute(text("SELECT COUNT(*) FROM todos;")).scalar() or 0)
        completed = int(session.execute(text("SELECT COUNT(*) FROM todos WHERE completed = TRUE;")).scalar() or 0)

        per_cat_rows = session.execute(text("""
            SELECT c.name, COUNT(*)
            FROM todos t
            JOIN categories c ON t.category_id = c.id
            GROUP BY c.name;
        """)).fetchall()

        per_category = {r[0]: r[1] for r in per_cat_rows}

        return {
            "total": total,
            "completed": completed,
            "pending": total - completed,
            "per_category": per_category,
        }
    except Exception as e:
        session.rollback()
        import traceback
        raise HTTPException(status_code=500, detail=traceback.format_exc())


@app.post("/api/seed")
def seed_data():
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute("BEGIN;")

        cursor.execute("SAVEPOINT seed_cats;")
        try:
            cursor.execute("INSERT INTO categories VALUES (100, 'Work');")
            cursor.execute("INSERT INTO categories VALUES (101, 'Personal');")
            cursor.execute("INSERT INTO categories VALUES (102, 'Shopping');")
            cursor.execute("RELEASE SAVEPOINT seed_cats;")
        except Exception:
            cursor.execute("ROLLBACK TO seed_cats;")

        cursor.execute("SAVEPOINT seed_todos;")
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        try:
            cursor.execute(f"INSERT INTO todos VALUES (100, 'Finish report', 'Q1 report', FALSE, 1, 100, '{now}');")
            cursor.execute(f"INSERT INTO todos VALUES (101, 'Buy groceries', 'Milk and eggs', FALSE, 2, 102, '{now}');")
            cursor.execute(f"INSERT INTO todos VALUES (102, 'Go for a run', 'Morning jog', FALSE, 3, 101, '{now}');")
            cursor.execute("RELEASE SAVEPOINT seed_todos;")
        except Exception:
            cursor.execute("ROLLBACK TO seed_todos;")

        cursor.execute("COMMIT;")
        cursor.close()
        raw_conn.close()
    except Exception as e:
        try:
            cursor = raw_conn.cursor()
            cursor.execute("ROLLBACK;")
            cursor.close()
            raw_conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))

    return {"message": "Sample data seeded"}
