import psycopg
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)

# Clean connection string with standard driver
engine = create_engine(
    "postgresql+psycopg://postgres@127.0.0.1:5432/tetodb",
    connect_args={"options": "-c client_min_messages=WARNING"}
)

Session = sessionmaker(bind=engine)
session = Session()

try:
    print("Testing TetoDB via Native SQLAlchemy...")

    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR);"))
        conn.commit()
    print("[SUCCESS] Table 'users' created.")

    new_user = User(id=1, name="Dustin")
    session.add(new_user)
    session.commit()
    print("[SUCCESS] Object persisted.")

    user = session.query(User).filter_by(id=1).first()
    if user:
        print(f"[SUCCESS] ORM Hydration Result: ID={user.id}, Name={user.name}")

except Exception as e:
    print(f"[FAILED] Error: {e}")
finally:
    session.close()