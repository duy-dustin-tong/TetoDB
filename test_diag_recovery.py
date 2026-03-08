"""Quick check: does table data survive crash recovery at all? (30s timeout)"""
import psycopg2, subprocess, time, os, sys

SERVER_EXE = r"D:\TetoDB\build\Release\teto_main.exe"
SERVER_CWD = r"D:\TetoDB"

def wait_for_server(timeout=8):
    for _ in range(timeout * 10):
        try:
            c = psycopg2.connect(host="127.0.0.1", port=5432, dbname="tetodb")
            c.close()
            return True
        except:
            time.sleep(0.1)
    return False

os.system("taskkill /F /IM teto_main.exe >nul 2>&1")
time.sleep(2)
for f in ["tetodb.db", "tetodb.log", "tetodb.catalog"]:
    p = os.path.join(SERVER_CWD, f)
    if os.path.exists(p):
        os.remove(p)

# Phase 1: Start, create, insert
subprocess.Popen([SERVER_EXE], cwd=SERVER_CWD, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, creationflags=subprocess.CREATE_NO_WINDOW)
assert wait_for_server(), "Server didn't start"

conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="tetodb")
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE TABLE diag2 (id INT PRIMARY KEY, val VARCHAR)")
for i in range(1, 6):
    cur.execute(f"INSERT INTO diag2 VALUES ({i}, 'row_{i}')")
cur.execute("SELECT * FROM diag2")
print(f"PRE-CRASH: {cur.fetchall()}")
cur.close()
conn.close()

for f in ["tetodb.db", "tetodb.log", "tetodb.catalog"]:
    p = os.path.join(SERVER_CWD, f)
    exists = os.path.exists(p)
    print(f"  {f}: {'EXISTS ' + str(os.path.getsize(p)) + 'B' if exists else 'MISSING'}")

# Kill
print("CRASHING server...")
os.system("taskkill /F /IM teto_main.exe >nul 2>&1")
time.sleep(2)

print("Files after crash:")
for f in ["tetodb.db", "tetodb.log", "tetodb.catalog"]:
    p = os.path.join(SERVER_CWD, f)
    exists = os.path.exists(p)
    print(f"  {f}: {'EXISTS ' + str(os.path.getsize(p)) + 'B' if exists else 'MISSING'}")

# Restart
print("Restarting...")
subprocess.Popen([SERVER_EXE], cwd=SERVER_CWD, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, creationflags=subprocess.CREATE_NO_WINDOW)
if not wait_for_server(timeout=20):
    print("HANG DETECTED: server didn't start within 20s")
    os.system("taskkill /F /IM teto_main.exe >nul 2>&1")
    sys.exit(1)

conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="tetodb")
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT * FROM diag2")
post_rows = cur.fetchall()
print(f"POST-RECOVERY seq scan: {len(post_rows)} rows -> {post_rows}")

if len(post_rows) > 0:
    cur.execute("SELECT val FROM diag2 WHERE id = 1")
    print(f"POST-RECOVERY index id=1: {cur.fetchall()}")

cur.close()
conn.close()
os.system("taskkill /F /IM teto_main.exe >nul 2>&1")
