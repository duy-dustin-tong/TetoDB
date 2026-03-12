import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


DATABASE_URL = os.getenv("TETODB_URL", "tetodb://127.0.0.1:5432/e2e")

engine = create_engine(
    DATABASE_URL,
    future=True,
    echo=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


def get_session():
    session: Session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
