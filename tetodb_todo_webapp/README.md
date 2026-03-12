# TetoDB Todo Web App

Simple full-stack Todo app with:
- React (Vite) frontend
- FastAPI backend
- SQLAlchemy using the TetoDB dialect

This app lives in a separate folder and does not modify TetoDB source code.

## 1) Start TetoDB server

From the TetoDB root:

```powershell
cmake -S . -B build
cmake --build build --config Release
./build/Release/teto_main.exe e2e
```

## 2) Backend setup

Open a new terminal:

```powershell
cd tetodb_todo_webapp/backend
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pip install -e ../../python
uvicorn app.main:app --reload
```

Backend runs on `http://127.0.0.1:8000`.

## 3) Frontend setup

Open another terminal:

```powershell
cd tetodb_todo_webapp/frontend
npm install
npm run dev
```

Frontend runs on `http://127.0.0.1:5173`.

## Notes

- TetoDB does not support `Base.metadata.create_all()`, so schema creation is done with explicit SQL at startup.
- TetoDB does not support `INSERT INTO table (col1, ...) VALUES (...)`, so inserts use positional `VALUES`.
