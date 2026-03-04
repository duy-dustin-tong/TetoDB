-- Minimal reproduction of FK insert bug
CREATE TABLE p (id INTEGER PRIMARY KEY, name VARCHAR);
INSERT INTO p VALUES (1, 'one');
INSERT INTO p VALUES (2, 'two');
SELECT id, name FROM p;

-- FK on single-column INTEGER -> INTEGER
CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p (id) ON DELETE CASCADE);

-- This should succeed but fails with FK constraint error
INSERT INTO c VALUES (100, 1);
INSERT INTO c VALUES (200, 2);
SELECT id, pid FROM c;

DROP TABLE c;
DROP TABLE p;
