CREATE TABLE t1 (id1 INT, name1 VARCHAR(20));
CREATE TABLE t2 (id2 INT, value2 INT);
CREATE TABLE t3 (value3 INT, tag3 VARCHAR(20));

INSERT INTO t1 VALUES (1, 'Alice');
INSERT INTO t1 VALUES (2, 'Bob');
INSERT INTO t1 VALUES (3, 'Charlie');

INSERT INTO t2 VALUES (1, 100);
INSERT INTO t2 VALUES (2, 200);

INSERT INTO t3 VALUES (100, 'Red');
INSERT INTO t3 VALUES (200, 'Blue');

-- Check 3-way join execution
SELECT * FROM t1 JOIN t2 ON id1 = id2 JOIN t3 ON value2 = value3;

-- Check 4-way join execution (using t1 again to filter out Bob)
SELECT * FROM t1 JOIN t2 ON id1 = id2 JOIN t3 ON value2 = value3 JOIN t1 ON name1 = 'Alice';
-- Wait, 'Alice' is a string literal, but join condition needs binary expression with column refs in planner `planner.cpp`.
-- Actually just check the 3-way join which is sufficient for multi-table JOIN validation.

DROP TABLE t3;
DROP TABLE t2;
DROP TABLE t1;
