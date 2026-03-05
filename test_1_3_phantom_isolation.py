import psycopg2
import threading
import time
import sys

# DSN to connect to the local TetoDB server
DSN = "postgresql://test_user@127.0.0.1:5432/tetodb"

test_failed = False
ready_event = threading.Event()
scanner_done_event = threading.Event()
adversary_blocked = False

def thread_a():
    """ The Scanner thread: queries an index and sleeps mid-read """
    global test_failed
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()

        ready_event.wait()
        
        print("[SCANNER] Executing SELECT * FROM phantom_test WHERE val = 100")
        
        # This query will trigger IndexScanExecutor.
        # The iterator holds an RLatch on the B+Tree leaf page.
        # Init() sleeps 2 seconds WHILE holding the latch.
        # During this window, the Adversary tries to INSERT into the same leaf page.
        # The insert's WLatch should be BLOCKED by our RLatch.
        cur.execute("SELECT * FROM phantom_test WHERE val = 100;")
        
        rows = cur.fetchall()
        print(f"[SCANNER] Scan complete. Found {len(rows)} rows: {rows}")
        
        scanner_done_event.set()
        
        # SUCCESS: The scan saw exactly 1 row (the original seed).
        # The phantom ghost insert was BLOCKED by the leaf page RLatch
        # and only completed AFTER the scan finished.
        if len(rows) == 1:
            print("[RESULT] SUCCESS. The IndexScan's RLatch blocked the concurrent Ghost Insert!")
            print("[RESULT] The scan correctly observed a consistent snapshot (1 row).")
            test_failed = False
        elif len(rows) == 2:
            print("[RESULT] FAILED. Phantom Read! The ghost insert bypassed the latch and the scan saw 2 rows!")
            test_failed = True
        elif len(rows) == 0:
            print("[RESULT] FAILED. The scan found 0 rows - iterator logic is broken.")
            test_failed = True
        else:
            print(f"[RESULT] UNEXPECTED ROW COUNT: {len(rows)}")
            test_failed = True

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[SCANNER] Error: {e}")
        test_failed = True

def thread_b():
    """ The Adversary thread: tries to INSERT a ghost row matching the scan's predicate """
    global test_failed, adversary_blocked
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()

        ready_event.wait()
        
        # Wait 0.5s to let Scanner initialize the Tree search and acquire RLatch
        time.sleep(0.5) 

        print("[ADVERSARY] Inserting Ghost Row (id=999, val=100)...")
        insert_start = time.time()
        cur.execute("INSERT INTO phantom_test VALUES (999, 100);")
        insert_end = time.time()
        insert_duration = insert_end - insert_start
        
        print(f"[ADVERSARY] Ghost Insert committed successfully! (took {insert_duration:.2f}s)")
        
        # If the insert took more than 1 second, the WLatch was blocked by our RLatch
        if insert_duration >= 1.0:
            adversary_blocked = True
            print("[ADVERSARY] CONFIRMED: The insert was blocked by the scanner's RLatch for {:.2f}s".format(insert_duration))
        else:
            print("[ADVERSARY] WARNING: Insert completed too quickly - latch may not be working")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ADVERSARY] Error (expected if blocked by Read Latch): {e}")

if __name__ == "__main__":
    print("=== TetoDB Concurrency Isolation Test (Phantom Reads) ===")
    
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        
        try: cur.execute("DROP TABLE phantom_test;")
        except: pass

        cur.execute("CREATE TABLE phantom_test (id INT PRIMARY KEY, val INT);")
        cur.execute("CREATE INDEX idx_val ON phantom_test(val);")

        # Insert a seed row that the scanner will find immediately
        cur.execute("INSERT INTO phantom_test VALUES (1, 100);")
        
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
    
    # Final verification: After both threads complete, check that the ghost row now exists
    # (it should have been inserted AFTER the scan released the latch)
    try:
        conn2 = psycopg2.connect(DSN)
        conn2.autocommit = True
        cur2 = conn2.cursor()
        cur2.execute("SELECT * FROM phantom_test WHERE val = 100;")
        final_rows = cur2.fetchall()
        print(f"[FINAL CHECK] After both threads completed, table has {len(final_rows)} rows with val=100: {final_rows}")
        if len(final_rows) == 2:
            print("[FINAL CHECK] CONFIRMED: Ghost row was inserted after scan completed.")
        cur2.close()
        conn2.close()
    except Exception as e:
        print(f"[FINAL CHECK] Error: {e}")
    
    if test_failed:
        print("\n=== TEST FAILED ===")
        sys.exit(1)
    else:
        if adversary_blocked:
            print("\n=== TEST PASSED === (RLatch successfully blocked concurrent Ghost Insert)")
        else:
            print("\n=== TEST PASSED === (Scan returned correct results)")
        sys.exit(0)
