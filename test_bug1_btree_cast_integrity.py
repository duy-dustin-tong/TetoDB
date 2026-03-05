"""
Test: Bug #1 — B+Tree reinterpret_cast Integrity
=================================================
Verifies that replacing placement-new with reinterpret_cast in b_plus_tree.cpp
does NOT corrupt page data.  The old SAFE_LEAF_CAST / SAFE_INTERNAL_CAST macros
saved only the FIRST key-value pair before calling placement-new; entries [1..N]
could be silently zeroed out (undefined behaviour).

Strategy
--------
1. Create a table with a PRIMARY KEY (B+Tree index).
2. Insert enough rows to force multiple leaf-page splits.
3. After all inserts, do a full sequential scan AND an index-driven lookup
   for every inserted key.  Verify every value matches exactly.
4. DELETE half the rows, then verify the remaining half is still intact.
5. Re-INSERT the deleted rows and verify again.

Note: Buffer pool is limited to 50 pages, so we use N=50 rows to exercise
splits without exhausting the pool.
"""

import psycopg2
import sys
import time
import traceback


DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "tetodb"


def run_test():
    print("=" * 60)
    print("Bug #1 Test: B+Tree reinterpret_cast Integrity")
    print("=" * 60)

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        options="-c client_encoding=UTF8"
    )
    conn.autocommit = True
    cur = conn.cursor()

    # ------------------------------------------------------------------
    # SETUP — clean slate
    # ------------------------------------------------------------------
    try:
        cur.execute("DROP TABLE btree_test")
    except Exception:
        pass

    cur.execute("""
        CREATE TABLE btree_test (
            id INT PRIMARY KEY,
            val VARCHAR
        )
    """)
    print("[SETUP] Table 'btree_test' created with PK index on 'id'.")

    # ------------------------------------------------------------------
    # PHASE 1 — Insert rows to force B+Tree splits
    # ------------------------------------------------------------------
    N = 50  # enough to trigger multiple splits; fits in 50-page buffer pool
    print(f"\n[PHASE 1] Inserting {N} rows...")
    t0 = time.time()
    for i in range(1, N + 1):
        cur.execute(f"INSERT INTO btree_test VALUES ({i}, 'row_{i}')")
    t1 = time.time()
    print(f"[PHASE 1] Inserted {N} rows in {t1 - t0:.2f}s")

    # ------------------------------------------------------------------
    # PHASE 2 — Verify ALL rows via sequential scan
    # ------------------------------------------------------------------
    print(f"\n[PHASE 2] Verifying all {N} rows via sequential scan...")
    cur.execute("SELECT id, val FROM btree_test")
    rows = cur.fetchall()

    if len(rows) != N:
        print(f"[FAIL] Expected {N} rows, got {len(rows)}")
        return False

    seen_ids = set()
    for row_id, row_val in rows:
        expected_val = f"row_{row_id}"
        if row_val != expected_val:
            print(f"[FAIL] Row id={row_id}: expected val='{expected_val}', got '{row_val}'")
            return False
        seen_ids.add(row_id)

    if seen_ids != set(range(1, N + 1)):
        missing = set(range(1, N + 1)) - seen_ids
        print(f"[FAIL] Missing IDs in scan: {sorted(missing)[:20]}...")
        return False

    print(f"[PHASE 2] PASS — All {N} rows verified (seq scan)")

    # ------------------------------------------------------------------
    # PHASE 3 — Verify ALL rows via index lookup (WHERE id = ?)
    # ------------------------------------------------------------------
    print(f"\n[PHASE 3] Verifying all {N} rows via index lookup (WHERE id = X)...")
    for i in range(1, N + 1):
        cur.execute(f"SELECT id, val FROM btree_test WHERE id = {i}")
        result = cur.fetchall()
        if len(result) != 1:
            print(f"[FAIL] Index lookup for id={i} returned {len(result)} rows (expected 1)")
            return False
        got_id, got_val = result[0]
        if got_id != i or got_val != f"row_{i}":
            print(f"[FAIL] Index lookup for id={i}: got ({got_id}, '{got_val}')")
            return False

    print(f"[PHASE 3] PASS — All {N} index lookups correct")

    # ------------------------------------------------------------------
    # PHASE 4 — Delete half, verify the other half survives
    # ------------------------------------------------------------------
    delete_ids = list(range(1, N + 1, 2))  # odd IDs
    keep_ids = list(range(2, N + 1, 2))    # even IDs
    print(f"\n[PHASE 4] Deleting {len(delete_ids)} rows (odd IDs)...")

    for did in delete_ids:
        cur.execute(f"DELETE FROM btree_test WHERE id = {did}")

    cur.execute("SELECT id, val FROM btree_test")
    remaining = cur.fetchall()

    if len(remaining) != len(keep_ids):
        print(f"[FAIL] Expected {len(keep_ids)} remaining rows, got {len(remaining)}")
        return False

    remaining_ids = sorted([r[0] for r in remaining])
    if remaining_ids != keep_ids:
        print(f"[FAIL] Remaining IDs mismatch")
        return False

    # Verify via index lookup
    for kid in keep_ids:
        cur.execute(f"SELECT val FROM btree_test WHERE id = {kid}")
        result = cur.fetchall()
        if len(result) != 1 or result[0][0] != f"row_{kid}":
            print(f"[FAIL] Index lookup for remaining id={kid} failed")
            return False

    # Verify deleted IDs return nothing
    for did in delete_ids:
        cur.execute(f"SELECT val FROM btree_test WHERE id = {did}")
        result = cur.fetchall()
        if len(result) != 0:
            print(f"[FAIL] Deleted id={did} still found via index!")
            return False

    print(f"[PHASE 4] PASS — {len(keep_ids)} rows intact after deletion, {len(delete_ids)} properly removed")

    # ------------------------------------------------------------------
    # PHASE 5 — Re-insert deleted rows, verify full dataset again
    # ------------------------------------------------------------------
    print(f"\n[PHASE 5] Re-inserting {len(delete_ids)} deleted rows...")
    for did in delete_ids:
        cur.execute(f"INSERT INTO btree_test VALUES ({did}, 'row_{did}')")

    cur.execute("SELECT id, val FROM btree_test")
    final_rows = cur.fetchall()

    if len(final_rows) != N:
        print(f"[FAIL] Expected {N} rows after re-insert, got {len(final_rows)}")
        return False

    for row_id, row_val in final_rows:
        if row_val != f"row_{row_id}":
            print(f"[FAIL] After re-insert, id={row_id}: expected 'row_{row_id}', got '{row_val}'")
            return False

    print(f"[PHASE 5] PASS — All {N} rows verified after delete+re-insert cycle")

    # ------------------------------------------------------------------
    # CLEANUP
    # ------------------------------------------------------------------
    cur.execute("DROP TABLE btree_test")
    cur.close()
    conn.close()

    return True


if __name__ == "__main__":
    try:
        success = run_test()
    except Exception as e:
        print(f"\n[EXCEPTION] {e}")
        traceback.print_exc()
        success = False

    print("\n" + "=" * 60)
    if success:
        print("RESULT: ALL PHASES PASSED ✓")
    else:
        print("RESULT: TEST FAILED ✗")
    print("=" * 60)

    sys.exit(0 if success else 1)
