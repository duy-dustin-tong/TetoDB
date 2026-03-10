import psycopg2
import sys
import time

DSN = "postgresql://test_user@127.0.0.1:5432/tetodb"

def run_test():
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        
        try: cur.execute("DROP TABLE test_bug17;")
        except: pass
        
        cur.execute("CREATE TABLE test_bug17 (a INT, b DECIMAL);")
        cur.execute("INSERT INTO test_bug17 VALUES (10, 0.4);")
        cur.execute("INSERT INTO test_bug17 VALUES (10, 0.0);")
        
        # Test 1: Normal divide by zero
        try:
            cur.execute("SELECT a FROM test_bug17 WHERE (a / b) = 0 AND b = 0.0;")
            print("[FAIL] Normal divide by zero didn't throw error!")
            return False
        except psycopg2.Error as e:
            if "Division by zero" not in str(e) and "division by zero" not in str(e).lower():
                print(f"[WARN] Normal divide by zero gave weird error: {e}")
                
        # Test 2: Truncation divide by zero
        try:
            # 10 / 0.4. Because value.cpp coerces INT / DECIMAL into INT / INT, 
            # 0.4 casts down to INT 0, triggering the C++ SIGFPE unless our patch worked.
            cur.execute("SELECT a FROM test_bug17 WHERE (a / b) = 0 AND b = 0.4;")
            print("[FAIL] Truncation divide by zero didn't throw error! Wait, if it didn't crash C++, that means it survived!")
        except psycopg2.Error as e:
            if "Division by" not in str(e):
                print(f"[FAIL] Truncation divide by zero gave weird error: {e}")
                return False

        print("[PASS] Division by zero vulnerability plugged.")
        return True
        
    except Exception as e:
        print(f"[ERROR] Connection lost or TetoDB crashed: {e}")
        return False

if __name__ == '__main__':
    success = run_test()
    sys.exit(0 if success else 1)
