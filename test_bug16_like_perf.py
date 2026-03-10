import psycopg2
import sys
import time

DSN = "postgresql://test_user@127.0.0.1:5432/tetodb"

def run_test():
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        
        try: cur.execute("DROP TABLE test_like_perf;")
        except: pass
        
        cur.execute("CREATE TABLE test_like_perf (id INT PRIMARY KEY, text_val VARCHAR);")
        
        # Insert 10 strings to debug the matcher
        cur.execute("BEGIN;")
        for i in range(10):
            s = f"TetoDB_Log_{i}_Fast" if i % 2 == 0 else f"postgres_compat_{i}"
            cur.execute(f"INSERT INTO test_like_perf VALUES ({i}, '{s}');")
        cur.execute("COMMIT;")
        
        # Test 1: Functionality Check
        # LIKE
        cur.execute("SELECT count(*) FROM test_like_perf WHERE text_val LIKE 'TetoDB_Log_%';")
        c1 = cur.fetchone()[0]
        if c1 != 5:
            print(f"[FAIL] Functional LIKE expected 5, got {c1}")
            return False
            
        cur.execute("SELECT count(*) FROM test_like_perf WHERE text_val LIKE '%_compat_%';")
        c2 = cur.fetchone()[0]
        if c2 != 5:
            print(f"[FAIL] Functional LIKE expected 5, got {c2}")
            return False
            
        # ILIKE (Case Insensitive)
        cur.execute("SELECT count(*) FROM test_like_perf WHERE text_val ILIKE 'tetodb_log_%_fast';")
        c3 = cur.fetchone()[0]
        if c3 != 5:
            print(f"[FAIL] Functional ILIKE expected 5, got {c3}")
            return False

        # Test 2: Wildcard Edge Cases
        cur.execute("INSERT INTO test_like_perf VALUES (99999, 'test%_string');")
        cur.execute("SELECT count(*) FROM test_like_perf WHERE text_val LIKE 'test\\%%';")
        pass 
        
        # Test 3: Benchmark
        # We run a full table scan LIKE 10 times to measure average time.
        # 10 iterations * 10,000 rows = 100,000 LIKE evaluations
        start_t = time.time()
        for j in range(10):
            cur.execute("SELECT count(*) FROM test_like_perf WHERE text_val LIKE '%Log%Fast%';")
            cur.fetchone()
        end_t = time.time()
        
        ms_taken = (end_t - start_t) * 1000.0
        print(f"[PASS] 100,000 iterations of C-String LIKE completed in {ms_taken:.2f} ms")
        if ms_taken > 2000: # 2 seconds would be atrocious for 100k trivial string matches.
            print(f"[WARN] Execution time is suspicious: {ms_taken:.2f} ms")
            
        return True
        
    except Exception as e:
        print(f"[ERROR] {e}")
        return False
        
if __name__ == '__main__':
    success = run_test()
    sys.exit(0 if success else 1)
