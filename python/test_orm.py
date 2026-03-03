import psycopg2

print("Connecting to TetoDB...")
try:
    # 1. THE HANDSHAKE
    # This triggers the Startup, Auth, and the dummy SET/SHOW bypasses in your parser.
    conn = psycopg2.connect(host="127.0.0.1", port=5432, user="postgres")
    cur = conn.cursor()
    print("[SUCCESS] Handshake Bypassed. TetoDB accepted the connection.")

    # 2. STANDARD DDL
    cur.execute("CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR, balance INT);")
    conn.commit()
    print("[SUCCESS] Table 'accounts' created.")

    # 3. TRUE SERVER-SIDE PREPARED STATEMENTS
    # The driver sends the string containing $1, $2 separately from the tuple of data.
    print("Testing Parameterized Inserts...")
    cur.execute("INSERT INTO accounts VALUES ($1, $2, $3);", (1, 'Dustin', 5000))
    cur.execute("INSERT INTO accounts VALUES ($1, $2, $3);", (2, 'Palantir', 999999))
    conn.commit()
    print("[SUCCESS] Data inserted securely via ParameterValueExpression.")

    # 4. PARAMETERIZED FILTERING
    print("Testing Parameterized Query...")
    cur.execute("SELECT id, name, balance FROM accounts WHERE id = $1;", (2,))
    row = cur.fetchone()
    
    # Note: Because TetoDB currently packs all output into a single formatted string column, 
    # we read index 0.
    print(f"[SUCCESS] Retrieved Data: {row[0].strip()}")

    cur.close()
    conn.close()
    print("All tests passed. TetoDB is ready to map to SQLAlchemy.")

except Exception as e:
    print(f"[FAILED] Error: {e}")