from sqlalchemy import BigInteger, Boolean, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(40), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(80), nullable=False)

    categories: Mapped[list["Category"]] = relationship(back_populates="project")
    todos: Mapped[list["Todo"]] = relationship(back_populates="project")


class Priority(Base):
    __tablename__ = "priorities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    weight: Mapped[int] = mapped_column(Integer, nullable=False)

    todos: Mapped[list["Todo"]] = relationship(back_populates="priority")


class Category(Base):
    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    project_id: Mapped[int] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )

    project: Mapped[Project] = relationship(back_populates="categories")
    todos: Mapped[list["Todo"]] = relationship(back_populates="category")


class Todo(Base):
    __tablename__ = "todos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    is_done: Mapped[bool] = mapped_column(Boolean, nullable=False)
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    due_at: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    assignee: Mapped[str | None] = mapped_column(String(64), nullable=True)
    project_id: Mapped[int] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    category_id: Mapped[int | None] = mapped_column(
        ForeignKey("categories.id", ondelete="CASCADE"), nullable=True
    )
    priority_id: Mapped[int | None] = mapped_column(
        ForeignKey("priorities.id", ondelete="CASCADE"), nullable=True
    )

    project: Mapped[Project] = relationship(back_populates="todos")
    category: Mapped[Category | None] = relationship(back_populates="todos")
    priority: Mapped[Priority | None] = relationship(back_populates="todos")
    comments: Mapped[list["TodoComment"]] = relationship(back_populates="todo")


class TodoComment(Base):
    __tablename__ = "todo_comments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    todo_id: Mapped[int] = mapped_column(
        ForeignKey("todos.id", ondelete="CASCADE"), nullable=False
    )
    body: Mapped[str] = mapped_column(String(255), nullable=False)
    author: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)

    todo: Mapped[Todo] = relationship(back_populates="comments")
