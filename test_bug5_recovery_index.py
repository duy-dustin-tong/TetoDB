"""
Test: Bug #5 — Recovery Index Rebuild
Verifies indexes are rebuilt after crash recovery.
Expects server to be started/stopped externally (via PowerShell).
"""
import psycopg2
import sys
import time

DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "tetodb"


def wait_for_server(timeout=15):
    for _ in range(timeout * 10):
        try:
            c = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME)
            c.close()
            return True
        except:
            time.sleep(0.1)
    return False


def run_phase1():
    """Create table, insert rows, verify. Returns True on success."""
    print("[PHASE 1] Inserting rows and verifying via index lookup...")

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("DROP TABLE recovery_test")
    except Exception:
        pass

    cur.execute("CREATE TABLE recovery_test (id INT PRIMARY KEY, val VARCHAR)")
    N = 10
    for i in range(1, N + 1):
        cur.execute(f"INSERT INTO recovery_test VALUES ({i}, 'row_{i}')")

    for i in range(1, N + 1):
        cur.execute(f"SELECT val FROM recovery_test WHERE id = {i}")
        result = cur.fetchall()
        if len(result) != 1 or result[0][0] != f"row_{i}":
            print(f"[FAIL] Pre-crash lookup id={i}: {result}")
            cur.close(); conn.close()
            return False

    print(f"[PHASE 1] PASS — {N} rows inserted and verified")
    cur.close(); conn.close()
    return True


def run_phase4():
    """After recovery, verify all rows via seq scan + index lookup."""
    N = 10
    print(f"[PHASE 4] Verifying {N} rows after recovery...")

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT * FROM recovery_test")
    rows = cur.fetchall()
    print(f"  Seq scan: {len(rows)} rows")

    if len(rows) != N:
        print(f"[FAIL] Expected {N} rows, got {len(rows)}")
        cur.close(); conn.close()
        return False

    for i in range(1, N + 1):
        cur.execute(f"SELECT val FROM recovery_test WHERE id = {i}")
        result = cur.fetchall()
        if len(result) != 1 or result[0][0] != f"row_{i}":
            print(f"[FAIL] Post-recovery index lookup id={i}: {result}")
            cur.close(); conn.close()
            return False

    print(f"[PHASE 4] PASS — All {N} rows verified via seq scan + index")

    # Phase 5: Insert new rows to prove tree is functional
    print(f"\n[PHASE 5] Inserting {N} more rows...")
    for i in range(N + 1, 2 * N + 1):
        cur.execute(f"INSERT INTO recovery_test VALUES ({i}, 'row_{i}')")

    cur.execute("SELECT COUNT(*) FROM recovery_test")
    count = cur.fetchone()[0]
    if count != 2 * N:
        print(f"[FAIL] Expected {2*N} rows, got {count}")
        cur.close(); conn.close()
        return False

    print(f"[PHASE 5] PASS — {2*N} total rows verified")
    cur.close(); conn.close()
    return True


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "phase1"

    if mode == "phase1":
        success = run_phase1()
    elif mode == "phase4":
        success = run_phase4()
    else:
        print(f"Usage: {sys.argv[0]} [phase1|phase4]")
        sys.exit(1)

    sys.exit(0 if success else 1)
