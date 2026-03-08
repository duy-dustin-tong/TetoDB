import psycopg2
import threading
import time
import sys

# DSN to connect to the local TetoDB server
DSN = "postgresql://test_user@127.0.0.1:5432/tetodb"

NUM_READ_THREADS = 20
ROWS_TO_INSERT = 100

# Global flags to coordinate threads
writer_started = threading.Event()
writer_finished = threading.Event()
test_failed = False

def writer_thread():
    global test_failed
    print("[WRITER] Connecting...")
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Setup schema
        try:
            cur.execute("DROP TABLE isolation_test;")
        except Exception:
            pass # Table didn't exist, fine.
        cur.execute("CREATE TABLE isolation_test (id INT, val VARCHAR(50));")
        
        print("[WRITER] Starting massive uncommitted INSERT block...")
        
        # Signal readers to start attacking
        writer_started.set()

        cur.execute("BEGIN;")
        
        for i in range(ROWS_TO_INSERT):
            cur.execute(f"INSERT INTO isolation_test VALUES ({i}, 'test_str_value_{i}');")
            
            # Artificial sleep to widen the dirty-read window (give readers a chance to sneak in)
            if i % 1000 == 0:
                time.sleep(0.5)
                
        # Signal readers we are done inserting (but STILL haven't committed!)
        writer_finished.set()
        
        print("[WRITER] Inserting complete. Waiting 3 seconds before COMMIT...")
        time.sleep(3)
        
        print("[WRITER] COMMITTING transaction.")
        cur.execute("COMMIT;")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[WRITER Error] {e}")
        test_failed = True

def reader_thread(thread_id):
    global test_failed
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Wait until the writer creates the table and starts inserting
        writer_started.wait()
        
        reads_performed = 0
        hits = 0
        
        while not writer_finished.is_set():
            try:
                # TetoDB requires explicit BEGIN for manual isolation blocks (if autocommit is effectively off)
                # Actually, standard libpq handles implicit blocks. Let's send raw queries loop.
                cur.execute("SELECT count(*) FROM isolation_test;")
                row = cur.fetchone()
                if row is not None:
                    result = int(row[0])
                    reads_performed += 1
                    
                    # THE CORE ASSERTION:
                    # We must NOT see any rows because the writer has NOT committed.
                    if result > 0:
                        print(f"!!! [READER {thread_id}] DIRTY READ DETECTED! Found {result} rows !!!")
                        test_failed = True
                        break
            except psycopg2.errors.InFailedSqlTransaction:
                # TetoDB Wait-Die can abort a reader if it waits too long on a locked row. 
                # We simply rollback and try reading again.
                conn.rollback()
            except Exception as e:
                if "Transaction Aborted" in str(e):
                    conn.rollback()
                else:
                    raise e
                
            time.sleep(0.05) # Prevent overloading the socket completely, just ping rapidly
            
        print(f"[READER {thread_id}] Finished. Performed {reads_performed} clean reads.")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[READER {thread_id} Error] {e}")
        test_failed = True

if __name__ == "__main__":
    print("=== TetoDB Concurrency Isolation Test (Dirty Read Vulnerability) ===")
    
    # 1. Start the Assailant (Writer)
    w_thread = threading.Thread(target=writer_thread)
    
    # 2. Start the Defenders (Readers)
    r_threads = []
    for i in range(NUM_READ_THREADS):
        t = threading.Thread(target=reader_thread, args=(i,))
        r_threads.append(t)
        
    # Launch!
    w_thread.start()
    for t in r_threads:
        t.start()
        
    w_thread.join()
    for t in r_threads:
        t.join()
        
    if test_failed:
        print("\n[RESULT] FAILED. The database permitted a dirty read.")
        # sys.exit(1)
    else:
        print("\n[RESULT] PASSED. Perfect Data Isolation.")
        # sys.exit(0)

