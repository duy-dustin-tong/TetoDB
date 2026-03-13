import tetodb.dbapi
now = '2026-03-13 04:31:06'
conn = tetodb.dbapi.connect(port=9090, database='todoapp')
cur = conn.cursor()
try:
    cur.execute('BEGIN;')
    print("BEGIN OK")
    cur.execute('SAVEPOINT seed_todos;')
    print("SAVEPOINT OK")
    sql = f"INSERT INTO todos VALUES (100, 'Finish report', 'Q1 report', FALSE, 1, 100, '{now}');"
    print('Executing:', sql)
    cur.execute(sql)
    print('SUCCESS')
except Exception as e:
    print('ERROR:', e)
