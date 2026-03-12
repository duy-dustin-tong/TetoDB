from pydantic import BaseModel, Field


class CategoryCreate(BaseModel):
    name: str = Field(min_length=1, max_length=50)
    project_id: int | None = None


class CategoryOut(BaseModel):
    id: int
    name: str
    project_id: int
    project_name: str | None = None


class ProjectOut(BaseModel):
    id: int
    slug: str
    name: str


class TodoCreate(BaseModel):
    title: str = Field(min_length=1, max_length=255)
    project_id: int | None = None
    category_id: int | None = None
    priority_id: int | None = None
    due_at: int | None = None
    assignee: str | None = Field(default=None, min_length=1, max_length=64)


class TodoUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=255)
    is_done: bool | None = None
    category_id: int | None = None
    priority_id: int | None = None
    due_at: int | None = None
    assignee: str | None = Field(default=None, min_length=1, max_length=64)


class TodoOut(BaseModel):
    id: int
    title: str
    is_done: bool
    created_at: int
    due_at: int | None = None
    assignee: str | None = None
    project_id: int
    project_name: str | None = None
    category_id: int | None = None
    category_name: str | None = None
    priority_id: int | None = None
    priority_code: str | None = None


class TodoListResponse(BaseModel):
    items: list[TodoOut]
    total: int


class StatsOut(BaseModel):
    total: int
    completed: int
    open: int


class CategoryStatsOut(BaseModel):
    category_id: int
    category_name: str
    todo_count: int
    done_count: int
    avg_title_len: float


class FeatureShowcaseOut(BaseModel):
    total_count_star: int
    distinct_assignees: list[str]
    union_names: list[str]
    intersect_names: list[str]
    except_names: list[str]


class DeleteResult(BaseModel):
    deleted: int
