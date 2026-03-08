import subprocess
import time
import psycopg2
import sys
import os
import shutil

DB_PATH = "e2e.db"
LOG_PATH = "e2e.log"
CATALOG_PATH = "e2e.catalog"

def cleanup():
    # Wait a bit so file handles from the killed process are released
    time.sleep(1)
    for f in [DB_PATH, LOG_PATH, CATALOG_PATH]:
        if os.path.exists(f):
            try:
                os.remove(f)
                print(f"[TEST] Deleted {f}")
            except OSError as e:
                print(f"[!] FAILED TO DELETE {f}: {e}. THIS WILL CORRUPT THE TEST.")
                sys.exit(1)

def start_server():
    print("[TEST] Starting server...")
    return subprocess.Popen(
        ["build\\Release\\teto_main.exe"],
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
    )

def kill_server():
    print("[TEST] Force crashing server...")
    subprocess.run(["powershell", "-c", "Stop-Process -Name teto_main -Force -ErrorAction SilentlyContinue"], capture_output=True)

def phase1_inject_data():
    conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='tetodb')
    conn.autocommit = True
    cur = conn.cursor()
    
    # 1. Create table and commit (so it persists)
    cur.execute("CREATE TABLE t_clr (id INT PRIMARY KEY, val VARCHAR)")

    print("[TEST] Table created. Starting active (loser) transaction...")
    
    # 2. Insert rows but DO NOT COMMIT
    cur.execute("BEGIN")
    for i in range(1, 10):
        cur.execute(f"INSERT INTO t_clr VALUES ({i}, 'row_{i}')")
    
    # We leave the connection open and uncommitted, simulating a crash during active work
    return conn

def phase2_verify():
    conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='tetodb')
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT * FROM t_clr")
    rows = cur.fetchall()
    
    print(f"[TEST] Rows surviving after double-crash recovery: {len(rows)}")
    conn.close()
    return len(rows)

if __name__ == "__main__":
    kill_server()
    cleanup()

    # PHASE 1: Populate and Crash
    # Start server, insert data in an uncommitted transaction, and kill it abruptly.
    start_server()
    time.sleep(2)
    conn = phase1_inject_data()
    time.sleep(1)
    kill_server() # Crash 1: Server dies with active transaction. RIDs exist on disk but not committed.

    # PHASE 2: Crash DURING Recovery
    # Start server. Recovery starts. Redo puts the uncommitted changes back.
    # Undo starts rolling them back. We kill the server again very quickly 
    # to simulate a crash *during* the Undo phase.
    p2 = start_server()
    time.sleep(0.5) # Give it just enough time to start Redo and maybe some Undo
    kill_server() # Crash 2: Server dies DURING recovery.
    
    # PHASE 3: Complete Recovery and Verify
    # Start server normally. Recovery should read the CLRs written during Phase 2.
    # It will skip the already-undone records and finish gracefully.
    p3 = start_server()
    time.sleep(3) # Let it fully boot and recover
    
    try:
        survivors = phase2_verify()
        if survivors == 0:
            print("\n[RESULT] PASS ✓: All uncommitted records were safely undone via CLRs despite repeated crashes.")
            sys.exit(0)
        else:
            print(f"\n[RESULT] FAIL ✗: Expected 0 rows, but {survivors} survived.")
            sys.exit(1)
    finally:
        kill_server()
