# TetoDB Todo App

A simple todo list web app with React + Vite frontend and FastAPI + SQLAlchemy backend using the TetoDB dialect.

## What this demo uses from TetoDB

- Primary keys and foreign keys
- `ON DELETE CASCADE` and `ON DELETE SET NULL`
- `CREATE INDEX` and `CREATE UNIQUE INDEX`
- `CREATE VIEW`
- CTE query (`WITH ...`)
- `EXPLAIN` query endpoint
- Transactions and nested transactions via `session.begin_nested()`
- ORM model usage with SQLAlchemy session patterns
- Manual ID generation pattern (`COUNT(*)` then `MAX(id)`) for no-autoincrement environments

## 1) Start TetoDB server

From your TetoDB build output folder (example):

```powershell
./teto_main.exe todoapp 9090
```

## 2) Backend setup

Install the local TetoDB SQLAlchemy dialect first:

```powershell
cd python
pip install -e .
```

Then run the API:

```powershell
cd todo-tetodb/backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

## 3) Frontend setup

```powershell
cd todo-tetodb/frontend
npm install
npm run dev
```

Open the URL shown by Vite, typically `http://127.0.0.1:5173`.

## API quick list

- `GET /health`
- `GET /lists`
- `POST /lists`
- `GET /lists/{list_id}/todos`
- `POST /lists/{list_id}/todos`
- `PATCH /todos/{todo_id}/toggle`
- `DELETE /todos/{todo_id}`
- `GET /lists/{list_id}/stats`
- `GET /open-todos`
- `GET /lists/{list_id}/explain`
