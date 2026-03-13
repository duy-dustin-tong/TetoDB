"""
End-to-end test: TetoDB + SQLAlchemy ORM
Run teto_main.exe first, then run this script.
"""
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker, declarative_base

# --- Setup ---
engine = create_engine("tetodb://127.0.0.1:5432/e2e", echo=False)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)

Session = sessionmaker(bind=engine)

def main():
    print("=" * 50)
    print("  TetoDB SQLAlchemy ORM End-to-End Test")
    print("=" * 50)

    # 1. Create table via raw SQL
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50));"))
        conn.commit()
    print("[PASS] Table 'users' created.")

    # 2. Insert via ORM
    session = Session()
    try:
        session.execute(text("INSERT INTO users VALUES (1, 'Alice');"))
        session.execute(text("INSERT INTO users VALUES (2, 'Bob');"))
        session.execute(text("INSERT INTO users VALUES (3, 'Charlie');"))
        session.commit()
        print("[PASS] 3 users inserted via ORM session.")

        # 3. Query via raw SQL through connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM users;"))
            rows = result.fetchall()
            print(f"[PASS] SELECT returned {len(rows)} rows:")
            for row in rows:
                print(f"       id={row[0]}, name={row[1]}")

        # 4. Update
        session.execute(text("UPDATE users SET name = 'Alice Updated' WHERE id = 1;"))
        session.commit()
        print("[PASS] Updated user 1.")

        # 5. Query again to verify update
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM users WHERE id = 1;"))
            row = result.fetchone()
            assert row[1] == "Alice Updated", f"Expected 'Alice Updated', got '{row[1]}'"
            print(f"[PASS] Verified update: id={row[0]}, name={row[1]}")

        # 6. Delete
        session.execute(text("DELETE FROM users WHERE id = 3;"))
        session.commit()
        print("[PASS] Deleted user 3.")

        # 7. Final count
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM users;"))
            rows = result.fetchall()
            assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
            print(f"[PASS] Final count: {len(rows)} users.")

    finally:
        session.close()

    # 8. Cleanup
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE users;"))
        conn.commit()
    print("[PASS] Table 'users' dropped.")

    print()
    print("=" * 50)
    print("  ALL TESTS PASSED!")
    print("=" * 50)

if __name__ == "__main__":
    main()
