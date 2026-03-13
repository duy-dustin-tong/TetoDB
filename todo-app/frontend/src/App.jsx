import { useState, useEffect, useCallback } from 'react'

const API = '/api'

function App() {
  const [categories, setCategories] = useState([])
  const [todos, setTodos] = useState([])
  const [stats, setStats] = useState(null)
  const [selectedCat, setSelectedCat] = useState(null)
  const [newCatName, setNewCatName] = useState('')
  const [newTodo, setNewTodo] = useState({ title: '', description: '', priority: 1, category_id: '' })
  const [error, setError] = useState('')

  // ── Fetch helpers ─────────────────────────────────────────────
  const fetchCategories = useCallback(async () => {
    const res = await fetch(`${API}/categories`)
    setCategories(await res.json())
  }, [])

  const fetchTodos = useCallback(async () => {
    const url = selectedCat ? `${API}/todos?category_id=${selectedCat}` : `${API}/todos`
    const res = await fetch(url)
    setTodos(await res.json())
  }, [selectedCat])

  const fetchStats = useCallback(async () => {
    const res = await fetch(`${API}/stats`)
    setStats(await res.json())
  }, [])

  useEffect(() => { fetchCategories(); fetchStats() }, [fetchCategories, fetchStats])
  useEffect(() => { fetchTodos() }, [fetchTodos])

  // ── Category actions ──────────────────────────────────────────
  const addCategory = async (e) => {
    e.preventDefault()
    if (!newCatName.trim()) return
    setError('')
    const res = await fetch(`${API}/categories`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: newCatName.trim() })
    })
    if (!res.ok) { setError((await res.json()).detail); return }
    setNewCatName('')
    fetchCategories()
    fetchStats()
  }

  const deleteCategory = async (id) => {
    await fetch(`${API}/categories/${id}`, { method: 'DELETE' })
    if (selectedCat === id) setSelectedCat(null)
    fetchCategories()
    fetchTodos()
    fetchStats()
  }

  // ── Todo actions ──────────────────────────────────────────────
  const addTodo = async (e) => {
    e.preventDefault()
    if (!newTodo.title.trim()) return
    setError('')
    const body = {
      ...newTodo,
      category_id: newTodo.category_id ? Number(newTodo.category_id) : null,
      priority: Number(newTodo.priority) || 1
    }
    const res = await fetch(`${API}/todos`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    })
    if (!res.ok) { setError((await res.json()).detail); return }
    setNewTodo({ title: '', description: '', priority: 1, category_id: '' })
    fetchTodos()
    fetchStats()
  }

  const toggleTodo = async (todo) => {
    await fetch(`${API}/todos/${todo.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ completed: !todo.completed })
    })
    fetchTodos()
    fetchStats()
  }

  const deleteTodo = async (id) => {
    await fetch(`${API}/todos/${id}`, { method: 'DELETE' })
    fetchTodos()
    fetchStats()
  }

  const seedData = async () => {
    setError('')
    const res = await fetch(`${API}/seed`, { method: 'POST' })
    if (!res.ok) { setError((await res.json()).detail); return }
    fetchCategories()
    fetchTodos()
    fetchStats()
  }

  // ── Render ────────────────────────────────────────────────────
  return (
    <div className="app">
      <h1>📝 TetoDB Todo App</h1>

      {error && <div className="error">⚠ {error}</div>}

      {/* Stats */}
      {stats && (
        <div className="stats" style={{ marginBottom: 20 }}>
          <div className="stat-box"><div className="num">{stats.total}</div><div className="label">Total</div></div>
          <div className="stat-box"><div className="num">{stats.completed}</div><div className="label">Completed</div></div>
          <div className="stat-box"><div className="num">{stats.pending}</div><div className="label">Pending</div></div>
          {stats.per_category && Object.entries(stats.per_category).map(([cat, count]) => (
            <div className="stat-box" key={cat}><div className="num">{count}</div><div className="label">{cat}</div></div>
          ))}
        </div>
      )}

      <div className="layout">
        {/* Sidebar: Categories */}
        <div className="sidebar">
          <div className="card">
            <h2>Categories</h2>
            <form onSubmit={addCategory} className="add-form" style={{ marginTop: 8 }}>
              <input
                value={newCatName}
                onChange={e => setNewCatName(e.target.value)}
                placeholder="New category"
                style={{ width: '100%' }}
              />
              <button type="submit" className="primary" style={{ width: '100%' }}>Add</button>
            </form>
            <div
              className={`cat-item ${selectedCat === null ? 'active' : ''}`}
              onClick={() => setSelectedCat(null)}
            >
              <span>All</span>
            </div>
            {categories.map(cat => (
              <div
                key={cat.id}
                className={`cat-item ${selectedCat === cat.id ? 'active' : ''}`}
                onClick={() => setSelectedCat(cat.id)}
              >
                <span>{cat.name}</span>
                <button className="danger" onClick={e => { e.stopPropagation(); deleteCategory(cat.id) }}>×</button>
              </div>
            ))}
          </div>
          <button onClick={seedData} style={{ width: '100%' }}>🌱 Seed Sample Data</button>
        </div>

        {/* Main: Todos */}
        <div className="main">
          <div className="card">
            <h2>Add Todo</h2>
            <form onSubmit={addTodo} className="add-form">
              <input
                value={newTodo.title}
                onChange={e => setNewTodo({ ...newTodo, title: e.target.value })}
                placeholder="Title"
                required
              />
              <input
                value={newTodo.description}
                onChange={e => setNewTodo({ ...newTodo, description: e.target.value })}
                placeholder="Description"
              />
              <select
                value={newTodo.priority}
                onChange={e => setNewTodo({ ...newTodo, priority: e.target.value })}
              >
                <option value="1">Priority 1 (High)</option>
                <option value="2">Priority 2</option>
                <option value="3">Priority 3 (Low)</option>
              </select>
              <select
                value={newTodo.category_id}
                onChange={e => setNewTodo({ ...newTodo, category_id: e.target.value })}
              >
                <option value="">No Category</option>
                {categories.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
              <button type="submit" className="primary">Add Todo</button>
            </form>
          </div>

          <div className="card">
            <h2>Todos {selectedCat ? `(${categories.find(c => c.id === selectedCat)?.name || ''})` : '(All)'}</h2>
            {todos.length === 0 && <p style={{ color: '#999', padding: '10px 0' }}>No todos yet.</p>}
            {todos.map(todo => (
              <div key={todo.id} className={`todo-item ${todo.completed ? 'completed' : ''}`}>
                <input type="checkbox" checked={!!todo.completed} onChange={() => toggleTodo(todo)} />
                <span className="title">{todo.title}</span>
                {todo.description && <span style={{ color: '#999', fontSize: 13 }}>{todo.description}</span>}
                <span className="priority">P{todo.priority || 1}</span>
                {todo.category_name && <span style={{ fontSize: 12, color: '#4a90d9' }}>{todo.category_name}</span>}
                <button className="danger" onClick={() => deleteTodo(todo.id)}>×</button>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

export default App
