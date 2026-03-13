import psycopg

print("Connecting via psycopg 3 (True Extended Protocol)...")
try:
    # psycopg 3 natively uses the Extended Query Protocol
    conn = psycopg.connect("host=127.0.0.1 port=5432 user=postgres", autocommit=True)
    print("[SUCCESS] Handshake Bypassed. TetoDB accepted the connection.")

    # --- ADDED: Recreate the table for the clean database ---
    print("Executing DDL...")
    conn.execute("CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR, balance INT);")
    print("[SUCCESS] Table 'accounts' created.")

    print("Testing Parse -> Bind -> Execute Pipeline...")
    # psycopg 3 uses %s in Python, but translates it to $1, $2, $3 over the wire.
    conn.execute("INSERT INTO accounts VALUES (%s, %s, %s);", ('777', 'Palantir', '999999'))
    print("[SUCCESS] Data inserted securely via ParameterValueExpression.")

    print("Testing Parameterized Fetch...")
    cur = conn.execute("SELECT id, name, balance FROM accounts WHERE id = %s;", ('777',))
    row = cur.fetchone()
    
    print(f"[SUCCESS] Retrieved Data: {row[0].strip()}")

    conn.close()
    print("All tests passed. TetoDB is officially ORM compatible.")

except Exception as e:
    print(f"[FAILED] Error: {e}")