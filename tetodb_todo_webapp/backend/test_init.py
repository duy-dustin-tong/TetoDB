import traceback
import os
import sys

# Add the current directory to sys.path so app can be imported
sys.path.append(os.getcwd())

try:
    from app.main import _init_schema
    from app.db import SessionLocal
    print("Imports OK")
    s = SessionLocal()
    print("Database connected")
    try:
        _init_schema(s)
        print("Schema init SUCCESS")
    except Exception as e:
        print(f"Schema init FAILED: {e}")
        traceback.print_exc()
    finally:
        s.close()
        print("Session closed")
except Exception as e:
    print(f"Startup FAILED: {e}")
    traceback.print_exc()
