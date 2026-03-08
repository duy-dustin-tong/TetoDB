import psycopg2
import sys

DSN = "postgresql://test_user@127.0.0.1:5432/tetodb"

def run_test():
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        
        try: cur.execute("DROP TABLE test_bug14_isolated_clean;")
        except: pass
        
        cur.execute("CREATE TABLE test_bug14_isolated_clean (id INT PRIMARY KEY, val INT);")
        
        # Manually manage transaction over the wire
        cur.execute("BEGIN;")
        
        # 1. Insert a baseline row
        cur.execute("INSERT INTO test_bug14_isolated_clean VALUES (1, 100);")
        
        # 2. Trigger an abort locally on the server
        try:
            cur.execute("INSERT INTO test_bug14_isolated_clean VALUES (1, 200);") # Should fail
        except psycopg2.Error as e:
            print(f"[EXPECTED] Caught expected duplicate key exception: {e}")
            
        # 3. The transaction is now ABORTED on the server.
        # Force a manual COMMIT block down the wire to trigger the double-abort logic
        print("[TEST] Issuing manual COMMIT bypass down the wire...")
        try:
            cur.execute("COMMIT;")
        except Exception as e:
            print(f"[EXPECTED ROUTE] {e}")
            
        # Reset the connection to a clean state if the socket was tainted
        conn.close()
        
        # 5. Connect fresh and verify the baseline survived perfectly without double-undoing into garbage
        conn2 = psycopg2.connect(DSN)
        conn2.autocommit = True
        cur2 = conn2.cursor()
        cur2.execute("SELECT * FROM test_bug14_isolated_clean;")
        rows = cur2.fetchall()
        c = len(rows)
        if c != 0: 
            print(f"[FAIL] Expected 0 rows after rollback, found {c}.")
            return False
            
        print("[PASS] Double-undo prevented successfully. The server remained stable.")
        return True
        
    except Exception as e:
        print(f"[ERROR] {e}")
        return False
        
if __name__ == '__main__':
    success = run_test()
    sys.exit(0 if success else 1)
