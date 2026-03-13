import React, { useEffect, useMemo, useState } from "react";

const API = "http://127.0.0.1:8000";

async function api(path, options) {
  const response = await fetch(`${API}${path}`, options);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `HTTP ${response.status}`);
  }
  return response.json();
}

function formatDueAt(value) {
  if (!value) return "No due date";
  return value;
}

export default function App() {
  const [lists, setLists] = useState([]);
  const [activeListId, setActiveListId] = useState(null);
  const [todos, setTodos] = useState([]);
  const [stats, setStats] = useState({ total: 0, done: 0, avg_priority: 0 });
  const [listTitle, setListTitle] = useState("");
  const [todoTitle, setTodoTitle] = useState("");
  const [todoPriority, setTodoPriority] = useState(1);
  const [dueAt, setDueAt] = useState("");
  const [error, setError] = useState("");

  const activeList = useMemo(
    () => lists.find((list) => list.id === activeListId) || null,
    [lists, activeListId]
  );

  async function loadLists() {
    const data = await api("/lists");
    setLists(data);

    if (data.length === 0) {
      setActiveListId(null);
      return;
    }

    if (!activeListId || !data.some((item) => item.id === activeListId)) {
      setActiveListId(data[0].id);
    }
  }

  async function loadTodos(listId) {
    if (!listId) {
      setTodos([]);
      setStats({ total: 0, done: 0, avg_priority: 0 });
      return;
    }

    const [todoData, statsData] = await Promise.all([
      api(`/lists/${listId}/todos`),
      api(`/lists/${listId}/stats`)
    ]);

    setTodos(todoData);
    setStats(statsData);
  }

  useEffect(() => {
    loadLists().catch((err) => setError(err.message));
  }, []);

  useEffect(() => {
    loadTodos(activeListId).catch((err) => setError(err.message));
  }, [activeListId]);

  async function onCreateList(event) {
    event.preventDefault();
    const title = listTitle.trim();
    if (!title) return;

    setError("");
    try {
      const created = await api("/lists", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title })
      });
      setListTitle("");
      await loadLists();
      setActiveListId(created.id);
    } catch (err) {
      setError(err.message);
    }
  }

  async function onCreateTodo(event) {
    event.preventDefault();
    const title = todoTitle.trim();
    if (!activeListId || !title) return;

    setError("");
    try {
      await api(`/lists/${activeListId}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title,
          priority: Number(todoPriority),
          due_at: dueAt.trim() ? dueAt.trim() : null
        })
      });
      setTodoTitle("");
      setTodoPriority(1);
      setDueAt("");
      await loadTodos(activeListId);
      await loadLists();
    } catch (err) {
      setError(err.message);
    }
  }

  async function onToggleTodo(todoId) {
    setError("");
    try {
      await api(`/todos/${todoId}/toggle`, { method: "PATCH" });
      await loadTodos(activeListId);
      await loadLists();
    } catch (err) {
      setError(err.message);
    }
  }

  async function onDeleteTodo(todoId) {
    setError("");
    try {
      await api(`/todos/${todoId}`, { method: "DELETE" });
      await loadTodos(activeListId);
      await loadLists();
    } catch (err) {
      setError(err.message);
    }
  }

  return (
    <div className="app-shell">
      <header className="hero">
        <p className="kicker">React + Vite + SQLAlchemy + TetoDB</p>
        <h1>Teto Tasks</h1>
        <p className="subtitle">A tiny todo app that still uses serious relational features.</p>
      </header>

      {error ? <div className="error-box">{error}</div> : null}

      <section className="panel create-list-panel">
        <form className="inline-form" onSubmit={onCreateList}>
          <input
            type="text"
            value={listTitle}
            onChange={(e) => setListTitle(e.target.value)}
            placeholder="Create a list (e.g. Sprint 1)"
            maxLength={100}
          />
          <button type="submit">Add List</button>
        </form>
      </section>

      <main className="grid-layout">
        <aside className="panel">
          <div className="panel-title-row">
            <h2>Lists</h2>
            <span>{lists.length}</span>
          </div>

          <div className="list-stack">
            {lists.map((list) => (
              <button
                key={list.id}
                className={`list-item ${activeListId === list.id ? "active" : ""}`}
                onClick={() => setActiveListId(list.id)}
                type="button"
              >
                <span>{list.title}</span>
                <strong>{list.total}</strong>
              </button>
            ))}

            {lists.length === 0 ? <p className="muted">No lists yet.</p> : null}
          </div>
        </aside>

        <section className="panel">
          <div className="panel-title-row">
            <h2>{activeList ? activeList.title : "Todos"}</h2>
            <span>
              {stats.done}/{stats.total} done
            </span>
          </div>

          <div className="stats-row">
            <div className="chip">Total: {stats.total}</div>
            <div className="chip">Done: {stats.done}</div>
            <div className="chip">Avg Priority: {stats.avg_priority.toFixed(2)}</div>
          </div>

          <form className="todo-form" onSubmit={onCreateTodo}>
            <input
              type="text"
              value={todoTitle}
              onChange={(e) => setTodoTitle(e.target.value)}
              placeholder="New task"
              maxLength={200}
              disabled={!activeListId}
            />

            <select
              value={todoPriority}
              onChange={(e) => setTodoPriority(e.target.value)}
              disabled={!activeListId}
            >
              <option value={1}>P1</option>
              <option value={2}>P2</option>
              <option value={3}>P3</option>
            </select>

            <input
              type="text"
              value={dueAt}
              onChange={(e) => setDueAt(e.target.value)}
              placeholder="YYYY-MM-DD HH:MM:SS"
              disabled={!activeListId}
            />

            <button type="submit" disabled={!activeListId}>
              Add Todo
            </button>
          </form>

          <div className="todo-stack">
            {todos.map((todo) => (
              <article key={todo.id} className={`todo-card ${todo.is_done ? "done" : ""}`}>
                <button
                  className="check-btn"
                  onClick={() => onToggleTodo(todo.id)}
                  type="button"
                  aria-label="Toggle done"
                >
                  {todo.is_done ? "Done" : "Open"}
                </button>

                <div className="todo-main">
                  <h3>{todo.title}</h3>
                  <p>
                    Priority {todo.priority} • {formatDueAt(todo.due_at)}
                  </p>
                </div>

                <button className="ghost-btn" onClick={() => onDeleteTodo(todo.id)} type="button">
                  Delete
                </button>
              </article>
            ))}
            {todos.length === 0 ? <p className="muted">No tasks in this list.</p> : null}
          </div>
        </section>
      </main>
    </div>
  );
}
