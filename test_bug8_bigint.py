import psycopg2
import sys

DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "tetodb"

def run_test():
    print("[TEST] Connecting to TetoDB...")
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME)
        conn.autocommit = True
        cur = conn.cursor()
        
        try: cur.execute("DROP TABLE bug8_bigint")
        except: pass
        
        print("[TEST] Creating table with BIGINT column...")
        cur.execute("CREATE TABLE bug8_bigint (id BIGINT, name VARCHAR)")
        
        big_val = 9000000000000000000 # Larger than 32-bit max (2B)
        
        print(f"[TEST] Inserting massive 64-bit integer: {big_val}")
        cur.execute("INSERT INTO bug8_bigint VALUES (%s, %s)", (big_val, 'test'))
        
        print("[TEST] Fetching value back from server...")
        cur.execute("SELECT id FROM bug8_bigint")
        row = cur.fetchone()
        
        if row and row[0] == big_val:
            print(f"[TEST] PASS! The server correctly extracted and returned {row[0]}")
            return True
        else:
            print(f"[TEST] FAIL! Server returned {row[0]} instead of {big_val}")
            return False
            
    except Exception as e:
        print(f"[TEST] Exception: {e}")
        return False

if __name__ == "__main__":
    success = run_test()
    sys.exit(0 if success else 1)
