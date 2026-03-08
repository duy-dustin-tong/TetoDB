import psycopg2
import sys

DSN = "postgresql://test_user@127.0.0.1:5432/tetodb"

def run_test():
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        
        try: cur.execute("DROP TABLE test_set_a;")
        except: pass
        try: cur.execute("DROP TABLE test_set_b;")
        except: pass
        
        cur.execute("CREATE TABLE test_set_a (val INT);")
        cur.execute("CREATE TABLE test_set_b (val INT);")
        
        # Table A: [1, 1, 1, 2, 2, 3] (3 ones, 2 twos, 1 three)
        cur.execute("INSERT INTO test_set_a VALUES (1);")
        cur.execute("INSERT INTO test_set_a VALUES (1);")
        cur.execute("INSERT INTO test_set_a VALUES (1);")
        cur.execute("INSERT INTO test_set_a VALUES (2);")
        cur.execute("INSERT INTO test_set_a VALUES (2);")
        cur.execute("INSERT INTO test_set_a VALUES (3);")
        
        # Table B: [1, 1, 2, 3, 3, 4] (2 ones, 1 two, 2 threes, 1 four)
        cur.execute("INSERT INTO test_set_b VALUES (1);")
        cur.execute("INSERT INTO test_set_b VALUES (1);")
        cur.execute("INSERT INTO test_set_b VALUES (2);")
        cur.execute("INSERT INTO test_set_b VALUES (3);")
        cur.execute("INSERT INTO test_set_b VALUES (3);")
        cur.execute("INSERT INTO test_set_b VALUES (4);")
        
        # === INTERSECT ALL ===
        # A INTERSECT ALL B -> MIN(A, B) -> [1, 1] (2), [2] (1), [3] (1) = 4 rows
        cur.execute("SELECT * FROM test_set_a INTERSECT ALL SELECT * FROM test_set_b;")
        res1 = cur.fetchall()
        if len(res1) != 4:
            print(f"[FAIL] INTERSECT ALL returned {len(res1)} rows, expected 4. Rows: {res1}")
            return False
        
        # === EXCEPT ALL ===
        # A EXCEPT ALL B -> MAX(A - B, 0) -> [1] (3-2=1), [2] (2-1=1), [3] (1-2=0) = 2 rows
        cur.execute("SELECT * FROM test_set_a EXCEPT ALL SELECT * FROM test_set_b;")
        res2 = cur.fetchall()
        if len(res2) != 2:
            print(f"[FAIL] EXCEPT ALL returned {len(res2)} rows, expected 2. Rows: {res2}")
            return False
            
        print("[PASS] Multiset logic for EXCEPT ALL and INTERSECT ALL operates flawlessly.")
        return True
        
    except Exception as e:
        print(f"[ERROR] {e}")
        return False
        
if __name__ == '__main__':
    success = run_test()
    sys.exit(0 if success else 1)
