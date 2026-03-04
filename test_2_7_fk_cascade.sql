-- ============================================
-- TEST 2.7: FK Cascade/Restrict via FKConstraintHandler
-- ============================================
CREATE TABLE dept (id INTEGER PRIMARY KEY, name VARCHAR);
INSERT INTO dept VALUES (1, 'Engineering');
INSERT INTO dept VALUES (2, 'Sales');

-- ON DELETE CASCADE
CREATE TABLE emp (id INTEGER PRIMARY KEY, dept_id INTEGER, name VARCHAR, FOREIGN KEY (dept_id) REFERENCES dept (id) ON DELETE CASCADE);
INSERT INTO emp VALUES (10, 1, 'Alice');
INSERT INTO emp VALUES (20, 1, 'Bob');
INSERT INTO emp VALUES (30, 2, 'Charlie');

SELECT id, dept_id, name FROM emp;

-- Delete dept 1 -> should CASCADE delete Alice and Bob
DELETE FROM dept WHERE id = 1;

-- Only Charlie should remain
SELECT id, dept_id, name FROM emp;

-- Verify dept 1 is gone
SELECT id, name FROM dept;

DROP TABLE emp;
DROP TABLE dept;

-- Test RESTRICT
CREATE TABLE parent_r (id INTEGER PRIMARY KEY, val VARCHAR);
CREATE TABLE child_r (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent_r (id));
INSERT INTO parent_r VALUES (1, 'P1');
INSERT INTO child_r VALUES (100, 1);

-- This should FAIL with RESTRICT
DELETE FROM parent_r WHERE id = 1;

-- Verify both still exist
SELECT id, val FROM parent_r;
SELECT id, pid FROM child_r;

DROP TABLE child_r;
DROP TABLE parent_r;
