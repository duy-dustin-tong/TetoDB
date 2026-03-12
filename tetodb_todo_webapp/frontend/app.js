const API_BASE = "http://127.0.0.1:8000";

const todoList = document.getElementById("todo-list");
const catList = document.getElementById("cat-list");
const todoForm = document.getElementById("todo-form");
const catForm = document.getElementById("cat-form");
const titleInput = document.getElementById("todo-title");
const catInput = document.getElementById("cat-name");
const catSelect = document.getElementById("todo-cat");
const totalCount = document.getElementById("total-count");
const doneCount = document.getElementById("done-count");
const clearBtn = document.getElementById("clear-completed");
const errorEl = document.getElementById("error");

let todos = [];
let categories = [];

function showError(msg) {
  if (!msg) {
    errorEl.style.display = "none";
    errorEl.textContent = "";
    return;
  }
  errorEl.style.display = "block";
  errorEl.textContent = msg;
}

async function api(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, options);
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(txt || `Request failed (${res.status})`);
  }
  if (res.status === 204) return null;
  return res.json();
}

function renderCounts() {
  const done = todos.filter((t) => t.is_done).length;
  totalCount.textContent = `Total: ${todos.length}`;
  doneCount.textContent = `Done: ${done}`;
  clearBtn.disabled = done === 0;
}

function renderCategories() {
  catList.innerHTML = "";
  catSelect.innerHTML = '<option value="">No Category</option>';

  for (const c of categories) {
    const li = document.createElement("li");
    li.innerHTML = `${c.name} <button data-id="${c.id}">x</button>`;
    catList.appendChild(li);

    const opt = document.createElement("option");
    opt.value = c.id;
    opt.textContent = c.name;
    catSelect.appendChild(opt);
  }
}

function renderTodos() {
  todoList.innerHTML = "";
  for (const t of todos) {
    const li = document.createElement("li");
    const checked = t.is_done ? "checked" : "";
    const titleClass = t.is_done ? "done" : "";
    const cat = t.category_name ? `<span class="cat-badge">${t.category_name}</span>` : "";

    li.innerHTML = `
      <label>
        <input type="checkbox" data-toggle="${t.id}" ${checked} />
        <div class="todo-content">
          <span class="${titleClass}">${t.title}</span>
          ${cat}
        </div>
      </label>
      <button data-del="${t.id}">Delete</button>
    `;
    todoList.appendChild(li);
  }
  renderCounts();
}

async function loadCategories() {
  categories = await api("/api/categories");
  renderCategories();
}

async function loadTodos() {
  const data = await api("/api/todos");
  todos = data.items || [];
  renderTodos();
}

todoForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  showError("");
  const title = titleInput.value.trim();
  if (!title) return;

  try {
    await api("/api/todos", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        title,
        category_id: catSelect.value ? Number(catSelect.value) : null,
      }),
    });
    titleInput.value = "";
    await loadTodos();
  } catch (err) {
    showError(err.message);
  }
});

catForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  showError("");
  const name = catInput.value.trim();
  if (!name) return;

  try {
    await api("/api/categories", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    });
    catInput.value = "";
    await loadCategories();
  } catch (err) {
    showError(err.message);
  }
});

catList.addEventListener("click", async (e) => {
  const id = e.target?.dataset?.id;
  if (!id) return;
  showError("");
  try {
    await api(`/api/categories/${id}`, { method: "DELETE" });
    await loadCategories();
    await loadTodos();
  } catch (err) {
    showError(err.message);
  }
});

todoList.addEventListener("click", async (e) => {
  const delId = e.target?.dataset?.del;
  const toggleId = e.target?.dataset?.toggle;
  if (!delId && !toggleId) return;

  showError("");
  try {
    if (delId) {
      await api(`/api/todos/${delId}`, { method: "DELETE" });
    }
    if (toggleId) {
      const todo = todos.find((t) => String(t.id) === String(toggleId));
      if (!todo) return;
      await api(`/api/todos/${toggleId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ is_done: !todo.is_done }),
      });
    }
    await loadTodos();
  } catch (err) {
    showError(err.message);
  }
});

clearBtn.addEventListener("click", async () => {
  showError("");
  try {
    await api("/api/todos/clear-completed", { method: "POST" });
    await loadTodos();
  } catch (err) {
    showError(err.message);
  }
});

async function boot() {
  try {
    await loadCategories();
    await loadTodos();
  } catch (err) {
    showError(err.message);
  }
}

boot();
