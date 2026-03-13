# Todo Demo App

This repository includes a full-stack demo in `todo-tetodb/`:

- Backend: FastAPI + SQLAlchemy
- Frontend: React (Vite)
- Database: TetoDB via `tetodb` SQLAlchemy dialect

## 1) Start TetoDB

```powershell
./build/Release/teto_main.exe todoapp 9090
```

## 2) Start Backend

```powershell
cd python
pip install -e .

cd ../todo-tetodb/backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

## 3) Start Frontend

```powershell
cd todo-tetodb/frontend
npm install
npm run dev
```

Default URLs:

- API: `http://127.0.0.1:8000`
- Frontend: typically `http://127.0.0.1:5173`

## API Endpoints

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
