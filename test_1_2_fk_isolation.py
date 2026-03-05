import psycopg2
import threading
import time
import sys

# DSN to connect to the local TetoDB server
DSN = "postgresql://test_user@127.0.0.1:5432/tetodb"

test_failed = False
ready_event = threading.Event()

def thread_a():
    """ The Child thread: tries to insert a row referencing parent ID 1 """
    global test_failed
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()

        # Wait until main gives the go ahead
        ready_event.wait()
        
        print("[CHILD] Inserting dependent row with parent_id = 1...")
        # This will sleep in C++ BEFORE finishing the insert, leaving the window open
        cur.execute("INSERT INTO child_table VALUES (100, 1);")
        
        print("[CHILD] Insert completed successfully!")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[CHILD] Error (expected if Wait-Die kills it or FK fails): {e}")

def thread_b():
    """ The Adversary thread: tries to DELETE parent ID 1 """
    global test_failed
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()

        ready_event.wait()
        
        # Give Thread A a tiny head start so it completes the FK lookup BEFORE we delete it
        time.sleep(0.5) 

        print("[ADVERSARY] Deleting parent_id = 1...")
        cur.execute("DELETE FROM parent_table WHERE id = 1;")
        print("[ADVERSARY] Delete committed successfully!")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ADVERSARY] Error (expected if blocked by SharedLock): {e}")

if __name__ == "__main__":
    print("=== TetoDB Concurrency Isolation Test (FK Race Condition) ===")
    
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        
        try: cur.execute("DROP TABLE child_table;")
        except: pass
        try: cur.execute("DROP TABLE parent_table;")
        except: pass

        cur.execute("CREATE TABLE parent_table (id INT PRIMARY KEY, data VARCHAR(50));")
        cur.execute("CREATE TABLE child_table (c_id INT PRIMARY KEY, p_id INT, FOREIGN KEY (p_id) REFERENCES parent_table(id));")

        cur.execute("INSERT INTO parent_table VALUES (1, 'parent 1');")
        
    except Exception as e:
        print(f"[SETUP Error] {e}")
        sys.exit(1)

    tA = threading.Thread(target=thread_a)
    tB = threading.Thread(target=thread_b)
    
    tA.start()
    tB.start()
    
    ready_event.set()
    
    tA.join()
    tB.join()
    
    try:
        # Assert: if child_table has 1 row and parent_table has 0 rows, we have an orphaned row
        cur.execute("SELECT count(*) FROM child_table;")
        row_c = cur.fetchone()
        c_count = int(row_c[0]) if row_c else 0
        
        cur.execute("SELECT count(*) FROM parent_table;")
        row_p = cur.fetchone()
        p_count = int(row_p[0]) if row_p else 0
        
        if c_count == 1 and p_count == 0:
            print(f"!!! [RESULT] FAILED. Orphaned Row Detected! Child Count: {c_count}, Parent Count: {p_count} !!!")
            test_failed = True
        elif c_count == 0 and p_count == 1:
            print(f"[RESULT] SUCCESS. Child Insert accurately rejected or aborted.")
        elif c_count == 1 and p_count == 1:
            print(f"[RESULT] SUCCESS. Wait-Die forced Adversary to abort. Reference integrity maintained.")
        else:
             print(f"[RESULT] UNEXPECTED STATE. Child Count: {c_count}, Parent Count: {p_count}")
             test_failed = True
             
    except Exception as e:
        print(f"[ASSERT Error] {e}")
        test_failed = True
        
    if test_failed:
        sys.exit(1)
    else:
        sys.exit(0)
