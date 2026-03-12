import { useEffect, useMemo, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";

function App() {
  const [todos, setTodos] = useState([]);
  const [categories, setCategories] = useState([]);
  const [title, setTitle] = useState("");
  const [newCatName, setNewCatName] = useState("");
  const [selectedCatId, setSelectedCatId] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const doneCount = useMemo(() => todos.filter((t) => t.is_done).length, [todos]);

  async function loadTodos() {
    setLoading(true);
    setError("");
    try {
      const res = await fetch(`${API_BASE}/api/todos`);
      if (!res.ok) {
        throw new Error(`Failed to load todos (${res.status})`);
      }
      const data = await res.json();
      setTodos(data.items || []);
    } catch (e) {
      setError(e.message || "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  async function loadCategories() {
    try {
      const res = await fetch(`${API_BASE}/api/categories`);
      if (res.ok) {
        const data = await res.json();
        setCategories(data || []);
      }
    } catch (e) {
      console.error("Failed to load categories", e);
    }
  }

  useEffect(() => {
    loadTodos();
    loadCategories();
  }, []);

  async function addTodo(e) {
    e.preventDefault();
    const clean = title.trim();
    if (!clean) return;
    setError("");
    try {
      const res = await fetch(`${API_BASE}/api/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: clean,
          category_id: selectedCatId ? parseInt(selectedCatId) : null,
        }),
      });
      if (!res.ok) {
        const msg = await res.text();
        throw new Error(msg || "Failed to create todo");
      }
      setTitle("");
      await loadTodos();
    } catch (e) {
      setError(e.message || "Unknown error");
    }
  }

  async function toggleTodo(todo) {
    setError("");
    try {
      const res = await fetch(`${API_BASE}/api/todos/${todo.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ is_done: !todo.is_done }),
      });
      if (!res.ok) {
        throw new Error(`Failed to update todo (${res.status})`);
      }
      await loadTodos();
    } catch (e) {
      setError(e.message || "Unknown error");
    }
  }

  async function removeTodo(todo) {
    setError("");
    try {
      const res = await fetch(`${API_BASE}/api/todos/${todo.id}`, {
        method: "DELETE",
      });
      if (!res.ok) {
        throw new Error(`Failed to delete todo (${res.status})`);
      }
      await loadTodos();
    } catch (e) {
      setError(e.message || "Unknown error");
    }
  }

  async function clearCompleted() {
    setError("");
    try {
      const res = await fetch(`${API_BASE}/api/todos/clear-completed`, {
        method: "POST",
      });
      if (!res.ok) {
        throw new Error(`Failed to clear completed (${res.status})`);
      }
      await loadTodos();
    } catch (e) {
      setError(e.message || "Unknown error");
    }
  }

  async function addCategory(e) {
    e.preventDefault();
    const clean = newCatName.trim();
    if (!clean) return;
    setError("");
    try {
      const res = await fetch(`${API_BASE}/api/categories`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: clean }),
      });
      if (!res.ok) {
        throw new Error("Failed to create category");
      }
      setNewCatName("");
      await loadCategories();
    } catch (e) {
      setError(e.message);
    }
  }

  async function removeCategory(id) {
    setError("");
    try {
      const res = await fetch(`${API_BASE}/api/categories/${id}`, {
        method: "DELETE",
      });
      if (!res.ok) throw new Error("Failed to delete category");
      await loadCategories();
      await loadTodos(); // In case of cascade delete
    } catch (e) {
      setError(e.message);
    }
  }

  return (
    <main className="page">
      <h1>TetoDB Todo Pro</h1>

      <section className="category-section">
        <h3>Categories</h3>
        <ul className="cat-list">
          {categories.map((c) => (
            <li key={c.id}>
              {c.name}
              <button onClick={() => removeCategory(c.id)}>x</button>
            </li>
          ))}
        </ul>
        <form onSubmit={addCategory}>
          <input
            value={newCatName}
            onChange={(e) => setNewCatName(e.target.value)}
            placeholder="New category"
          />
          <button type="submit">Add</button>
        </form>
      </section>

      <form className="todo-form" onSubmit={addTodo}>
        <input
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          placeholder="Add a task"
        />
        <select
          value={selectedCatId}
          onChange={(e) => setSelectedCatId(e.target.value)}
        >
          <option value="">No Category</option>
          {categories.map((c) => (
            <option key={c.id} value={c.id}>
              {c.name}
            </option>
          ))}
        </select>
        <button type="submit">Add Task</button>
      </form>

      <div className="toolbar">
        <span>Total: {todos.length}</span>
        <span>Done: {doneCount}</span>
        <button onClick={clearCompleted} disabled={doneCount === 0}>
          Clear completed
        </button>
      </div>

      {error ? <p className="error">{error}</p> : null}
      {loading ? <p>Loading...</p> : null}

      <ul className="todo-list">
        {todos.map((todo) => (
          <li key={todo.id}>
            <label>
              <input
                type="checkbox"
                checked={todo.is_done}
                onChange={() => toggleTodo(todo)}
              />
              <div className="todo-content">
                <span className={todo.is_done ? "done" : ""}>{todo.title}</span>
                {todo.category_name && (
                  <span className="cat-badge">{todo.category_name}</span>
                )}
              </div>
            </label>
            <button onClick={() => removeTodo(todo)}>Delete</button>
          </li>
        ))}
      </ul>
    </main>
  );
}

export default App;
