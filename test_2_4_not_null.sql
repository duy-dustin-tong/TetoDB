-- ============================================
-- TEST 2.4: NOT NULL Constraint Enforcement
-- ============================================

-- Create table with a NOT NULL column
CREATE TABLE nn_test (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, age INTEGER);

-- This should succeed (no nulls in NOT NULL column)
INSERT INTO nn_test VALUES (1, 'Alice', 25);

-- This should succeed (age is nullable, NULL is ok there)
INSERT INTO nn_test VALUES (2, 'Bob', NULL);

-- This should FAIL: name is NOT NULL but we're inserting NULL
INSERT INTO nn_test VALUES (3, NULL, 30);

-- Verify only 2 rows exist (the 3rd INSERT failed)
SELECT id, name, age FROM nn_test;

-- This should FAIL: PK is implicitly NOT NULL
INSERT INTO nn_test VALUES (NULL, 'Charlie', 40);

-- Verify still only 2 rows
SELECT id, name, age FROM nn_test;

DROP TABLE nn_test;
